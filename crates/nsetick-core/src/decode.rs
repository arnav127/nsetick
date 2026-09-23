//! Fixed-width record decoding into Arrow record batches.
//!
//! Two things make this fast enough to be worth writing in Rust:
//!
//! * **Projection.** Only the selected fields are ever touched. Asking for `symbol` and
//!   `txn_time` out of a CM order means 24 of the 87 bytes are read and the rest are never
//!   looked at.
//! * **Filter pushdown.** The predicate runs against raw record bytes before any column is
//!   built, so rejected records cost a handful of byte comparisons rather than a full decode
//!   plus an Arrow append that is then thrown away.
//!
//! Decoding proceeds in two passes over the chunk: select surviving records, then build each
//! projected column over just those records. Column-at-a-time keeps one builder hot at a
//! time instead of cycling through twenty of them per row.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Int64Builder, StringBuilder, Time32SecondBuilder,
    TimestampMicrosecondBuilder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{DataType, Field as ArrowField, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::filter::Predicate;
use crate::layout::{Field, FieldType, Version};
use crate::value::{
    date_to_days, is_blank, jiffies_to_unix_micros, parse_date_dmmmy, parse_date_ymd,
    parse_time_hms, parse_u64,
};

#[derive(Debug, Clone)]
pub struct DecodeOptions {
    /// Abort on a malformed record rather than counting it and moving on.
    ///
    /// Defaults to true. The existing pipelines wrap every field in `TRY_CAST`, so a
    /// misaligned file yields hundreds of millions of NULLs and no indication anything went
    /// wrong. Failing loudly is the better default; `--lenient` opts out and the rejected
    /// count is always reported either way.
    pub strict: bool,
}

impl Default for DecodeOptions {
    fn default() -> Self {
        Self { strict: true }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    pub rows_read: u64,
    pub rows_emitted: u64,
    pub rows_filtered: u64,
    pub rows_malformed: u64,
}

impl Stats {
    pub fn merge(&mut self, other: Stats) {
        self.rows_read += other.rows_read;
        self.rows_emitted += other.rows_emitted;
        self.rows_filtered += other.rows_filtered;
        self.rows_malformed += other.rows_malformed;
    }
}

fn arrow_type(f: &Field) -> DataType {
    match f.ty {
        FieldType::Str => DataType::Utf8,
        FieldType::U8 => DataType::UInt8,
        FieldType::U64 => DataType::UInt64,
        // Prices stay in their raw integer units (paise for CM/FAO, 1e-4 for CD). The scale
        // travels in the field metadata so a consumer can scale correctly per segment
        // instead of assuming a global divide-by-100.
        FieldType::Price => DataType::Int64,
        // No timezone: NSE jiffies decode to IST wall-clock directly, and stamping a UTC
        // timezone on them would shift every timestamp by 5h30m.
        FieldType::Jiffies => DataType::Timestamp(TimeUnit::Microsecond, None),
        FieldType::BoolYn => DataType::Boolean,
        FieldType::DateDmmmy | FieldType::DateYmd => DataType::Date32,
        FieldType::TimeHms => DataType::Time32(TimeUnit::Second),
    }
}

fn arrow_field(f: &Field) -> ArrowField {
    let mut meta: HashMap<String, String> = HashMap::new();
    if let Some(scale) = f.scale {
        meta.insert("scale".into(), scale.to_string());
        meta.insert("units".into(), format!("10^-{scale}"));
    }
    if f.ty == FieldType::Jiffies {
        meta.insert(
            "timezone".into(),
            "Asia/Kolkata (wall clock, not UTC)".into(),
        );
        meta.insert(
            "source".into(),
            "NSE jiffies, 65536/s from 1980-01-01".into(),
        );
    }
    if !f.doc.is_empty() {
        meta.insert("doc".into(), f.doc.clone());
    }
    // Every field is nullable: a blank fixed-width field is a legitimate null.
    ArrowField::new(&f.name, arrow_type(f), true).with_metadata(meta)
}

#[derive(Debug)]
pub struct Decoder {
    version: Version,
    projected: Vec<Field>,
    schema: SchemaRef,
    options: DecodeOptions,
}

impl Decoder {
    /// Build a decoder for `version`, optionally restricted to `select`ed fields.
    ///
    /// Selected fields are emitted in layout order regardless of the order they were
    /// requested in, so the schema is a function of the layout alone and two callers who ask
    /// for the same set always get identical parquet.
    pub fn new(
        version: &Version,
        select: Option<&[String]>,
        options: DecodeOptions,
    ) -> Result<Self> {
        let projected: Vec<Field> = match select {
            None => version.fields.clone(),
            Some(names) => {
                for n in names {
                    if version.field(n).is_none() {
                        let mut known: Vec<&str> =
                            version.fields.iter().map(|f| f.name.as_str()).collect();
                        known.sort_unstable();
                        bail!(
                            "cannot select unknown field {n:?}; this layout has: {}",
                            known.join(", ")
                        );
                    }
                }
                version
                    .fields
                    .iter()
                    .filter(|f| names.iter().any(|n| n == &f.name))
                    .cloned()
                    .collect()
            }
        };

        if projected.is_empty() {
            bail!("projection selected no fields");
        }

        let schema = Arc::new(Schema::new(
            projected.iter().map(arrow_field).collect::<Vec<_>>(),
        ));

        Ok(Self {
            version: version.clone(),
            projected,
            schema,
            options,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub fn version(&self) -> &Version {
        &self.version
    }

    /// Bytes per record including the LF delimiter.
    pub fn line_length(&self) -> usize {
        self.version.line_length()
    }

    /// Names of the fields this decoder emits, in schema order.
    pub fn projected_names(&self) -> Vec<&str> {
        self.projected.iter().map(|f| f.name.as_str()).collect()
    }

    /// Decode a buffer holding a whole number of complete records.
    ///
    /// The caller (the reader) is responsible for splitting on record boundaries, which is
    /// exact arithmetic for a fixed-width format rather than a scan for newlines.
    pub fn decode(
        &self,
        buf: &[u8],
        predicate: &Predicate,
        stats: &mut Stats,
    ) -> Result<RecordBatch> {
        let line = self.line_length();
        let reclen = self.version.record_length;

        if buf.len() % line != 0 {
            bail!(
                "buffer of {} bytes is not a whole number of {line}-byte records \
                 (layout {} expects {reclen}-byte records plus LF); {} bytes left over",
                buf.len(),
                self.version.spec_version,
                buf.len() % line
            );
        }

        let n_records = buf.len() / line;
        let mut keep: Vec<usize> = Vec::with_capacity(n_records);

        for i in 0..n_records {
            let start = i * line;
            let record = &buf[start..start + reclen];
            let delim = buf[start + reclen];

            if delim != b'\n' {
                stats.rows_malformed += 1;
                let hint = if delim == b'\r' {
                    " - the file appears to use CRLF line endings, but the NSE layout \
                     specifies a bare LF delimiter"
                } else {
                    ""
                };
                if self.options.strict {
                    bail!(
                        "record {i} is not {reclen} bytes: expected LF at offset {reclen}, \
                         found {:?}{hint}. Either the layout version is wrong or the file is \
                         truncated; refusing to guess.",
                        delim as char
                    );
                }
                continue;
            }

            stats.rows_read += 1;
            if predicate.eval(record) {
                keep.push(start);
            } else {
                stats.rows_filtered += 1;
            }
        }

        let columns = self
            .projected
            .iter()
            .map(|f| self.build_column(f, buf, &keep))
            .collect::<Result<Vec<ArrayRef>>>()?;

        stats.rows_emitted += keep.len() as u64;

        RecordBatch::try_new(Arc::clone(&self.schema), columns).context("assembling record batch")
    }

    fn build_column(&self, f: &Field, buf: &[u8], keep: &[usize]) -> Result<ArrayRef> {
        let n = keep.len();
        let strict = self.options.strict;

        // Raised when a field holds something that is neither a valid value nor blank. In
        // strict mode this aborts; otherwise the cell becomes null.
        macro_rules! bad {
            ($raw:expr) => {{
                if strict {
                    bail!(
                        "field {} contains {:?}, which is neither a valid {:?} value nor blank. \
                         This usually means the layout version does not match the file.",
                        f.name,
                        String::from_utf8_lossy($raw),
                        f.ty
                    );
                }
            }};
        }

        Ok(match f.ty {
            FieldType::Str => {
                // 12 bytes/value is a reasonable starting guess for symbols and codes.
                let mut b = StringBuilder::with_capacity(n, n * f.len.min(12));
                for &start in keep {
                    let raw = f.unpad(&buf[start + f.offset..start + f.offset + f.len]);
                    match std::str::from_utf8(raw) {
                        Ok(s) => b.append_value(s),
                        Err(_) => {
                            bad!(raw);
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }

            FieldType::U8 => {
                let mut b = UInt8Builder::with_capacity(n);
                for &start in keep {
                    let raw = &buf[start + f.offset..start + f.offset + f.len];
                    match parse_u64(raw) {
                        Some(v) if v <= u8::MAX as u64 => b.append_value(v as u8),
                        _ if is_blank(raw) => b.append_null(),
                        _ => {
                            bad!(raw);
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }

            FieldType::U64 => {
                let mut b = UInt64Builder::with_capacity(n);
                for &start in keep {
                    let raw = &buf[start + f.offset..start + f.offset + f.len];
                    match parse_u64(raw) {
                        Some(v) => b.append_value(v),
                        None if is_blank(raw) => b.append_null(),
                        None => {
                            bad!(raw);
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }

            FieldType::Price => {
                let mut b = Int64Builder::with_capacity(n);
                for &start in keep {
                    let raw = &buf[start + f.offset..start + f.offset + f.len];
                    match parse_u64(raw) {
                        Some(v) => b.append_value(v as i64),
                        None if is_blank(raw) => b.append_null(),
                        None => {
                            bad!(raw);
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }

            FieldType::Jiffies => {
                let mut b = TimestampMicrosecondBuilder::with_capacity(n);
                for &start in keep {
                    let raw = &buf[start + f.offset..start + f.offset + f.len];
                    match parse_u64(raw) {
                        Some(v) => b.append_value(jiffies_to_unix_micros(v)),
                        None if is_blank(raw) => b.append_null(),
                        None => {
                            bad!(raw);
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }

            FieldType::BoolYn => {
                let mut b = BooleanBuilder::with_capacity(n);
                for &start in keep {
                    match buf[start + f.offset] {
                        b'Y' => b.append_value(true),
                        b'N' => b.append_value(false),
                        other if crate::layout::is_pad(other) => b.append_null(),
                        _ => {
                            bad!(&buf[start + f.offset..start + f.offset + 1]);
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }

            FieldType::DateDmmmy | FieldType::DateYmd => {
                let mut b = Date32Builder::with_capacity(n);
                for &start in keep {
                    let raw = &buf[start + f.offset..start + f.offset + f.len];
                    let parsed = if f.ty == FieldType::DateDmmmy {
                        parse_date_dmmmy(raw)
                    } else {
                        parse_date_ymd(raw)
                    };
                    match parsed {
                        Some(d) => b.append_value(date_to_days(d)),
                        None if is_blank(raw) => b.append_null(),
                        None => {
                            bad!(raw);
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }

            FieldType::TimeHms => {
                let mut b = Time32SecondBuilder::with_capacity(n);
                for &start in keep {
                    let raw = &buf[start + f.offset..start + f.offset + f.len];
                    match parse_time_hms(raw) {
                        Some(s) => b.append_value(s),
                        None if is_blank(raw) => b.append_null(),
                        None => {
                            bad!(raw);
                            b.append_null();
                        }
                    }
                }
                Arc::new(b.finish())
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::compile;
    use crate::layout::load;
    use arrow::array::{Array, StringArray, TimestampMicrosecondArray, UInt64Array};
    use chrono::NaiveDate;

    /// Four real records from CASH_Orders_27012022.DAT.gz, LF-delimited.
    const SAMPLE: &[u8] = b"POCASH100000000000208487014847291575S1BALKRISINDEQ00000000000000760023000000000000NNN13\n\
POCASH100000000000209487014847291579S1bbbbbbbBCGBE00000000000000750001650000000000NNN13\n\
POCASH100000000000209787014847291580S1bbCARTRADEEQ00000000000003000007490000000000NNN13\n\
POCASH100000000000210187014847293583B1ADANIPOWEREQ00000000000000500000980000000000NNN13\n";

    fn version() -> Version {
        load("cm_orders")
            .unwrap()
            .for_date(NaiveDate::from_ymd_opt(2022, 1, 27).unwrap())
            .unwrap()
            .clone()
    }

    fn decode_with(select: Option<Vec<String>>, filter: &str) -> (RecordBatch, Stats) {
        let v = version();
        let d = NaiveDate::from_ymd_opt(2022, 1, 27).unwrap();
        let dec = Decoder::new(&v, select.as_deref(), DecodeOptions::default()).unwrap();
        let pred = compile(filter, &v, Some(d)).unwrap();
        let mut stats = Stats::default();
        let batch = dec.decode(SAMPLE, &pred, &mut stats).unwrap();
        (batch, stats)
    }

    #[test]
    fn decodes_every_field_of_every_record() {
        let (batch, stats) = decode_with(None, "");
        assert_eq!(stats.rows_read, 4);
        assert_eq!(stats.rows_emitted, 4);
        assert_eq!(stats.rows_malformed, 0);
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.num_columns(), 17);
    }

    #[test]
    fn symbols_keep_their_padding_stripped_and_nothing_else() {
        let (batch, _) = decode_with(Some(vec!["symbol".into()]), "");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let got: Vec<&str> = (0..col.len()).map(|i| col.value(i)).collect();
        assert_eq!(got, vec!["BALKRISIND", "BCG", "CARTRADE", "ADANIPOWER"]);
    }

    #[test]
    fn projection_emits_only_requested_fields_in_layout_order() {
        // Requested out of order on purpose.
        let sel = vec!["limit_price".to_string(), "symbol".to_string()];
        let (batch, _) = decode_with(Some(sel), "");
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            vec!["symbol", "limit_price"]
        );
    }

    #[test]
    fn projection_rejects_an_unknown_field_up_front() {
        let v = version();
        let err = Decoder::new(&v, Some(&["nope".to_string()]), DecodeOptions::default())
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown field"), "{err}");
    }

    #[test]
    fn filters_reject_rows_before_any_column_is_built() {
        let (batch, stats) = decode_with(Some(vec!["symbol".into()]), "series == 'EQ'");
        assert_eq!(stats.rows_read, 4);
        assert_eq!(stats.rows_filtered, 1); // BCG is series BE
        assert_eq!(stats.rows_emitted, 3);
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn compound_filters_narrow_correctly() {
        let (batch, _) = decode_with(
            Some(vec!["symbol".into()]),
            "series == 'EQ' and buy_sell == 'B' and limit_price < 50000",
        );
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.len(), 1);
        assert_eq!(col.value(0), "ADANIPOWER");
    }

    #[test]
    fn prices_stay_in_raw_units_and_carry_their_scale() {
        let (batch, _) = decode_with(Some(vec!["limit_price".into()]), "symbol == 'BALKRISIND'");
        let f = &batch.schema().fields()[0].clone();
        assert_eq!(f.metadata().get("scale").map(String::as_str), Some("2"));
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 230_000); // 2300.00 rupees
    }

    #[test]
    fn timestamps_are_naive_ist_wall_clock() {
        let (batch, _) = decode_with(Some(vec!["txn_time".into()]), "symbol == 'BALKRISIND'");
        let f = batch.schema().fields()[0].clone();
        assert_eq!(
            f.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let dt = chrono::DateTime::from_timestamp_micros(col.value(0)).unwrap();
        assert_eq!(
            dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
            "2022-01-27 09:00:00.127792"
        );
    }

    #[test]
    fn numeric_fields_decode_to_their_values() {
        let (batch, _) = decode_with(Some(vec!["volume_original".into()]), "");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(
            (0..4).map(|i| col.value(i)).collect::<Vec<_>>(),
            vec![76, 75, 300, 50]
        );
    }

    #[test]
    fn a_truncated_buffer_is_refused_rather_than_silently_shifted() {
        let v = version();
        let dec = Decoder::new(&v, None, DecodeOptions::default()).unwrap();
        let mut stats = Stats::default();
        let err = dec
            .decode(&SAMPLE[..SAMPLE.len() - 3], &Predicate::True, &mut stats)
            .unwrap_err()
            .to_string();
        assert!(err.contains("whole number"), "{err}");
    }

    #[test]
    fn a_wrong_layout_length_is_caught_at_the_delimiter() {
        // Decode 87-byte CM order records as if they were 100-byte CM trades.
        let trades = load("cm_trades").unwrap();
        let v = trades
            .for_date(NaiveDate::from_ymd_opt(2022, 1, 27).unwrap())
            .unwrap();
        let dec = Decoder::new(v, None, DecodeOptions::default()).unwrap();
        let mut stats = Stats::default();
        // 4 * 88 = 352 bytes; not a multiple of 101, so framing fails immediately.
        let err = dec
            .decode(SAMPLE, &Predicate::True, &mut stats)
            .unwrap_err();
        assert!(err.to_string().contains("whole number"), "{err}");
    }

    #[test]
    fn lenient_mode_counts_malformed_records_instead_of_aborting() {
        let v = version();
        let dec = Decoder::new(&v, None, DecodeOptions { strict: false }).unwrap();
        // Corrupt the delimiter of the second record.
        let mut bad = SAMPLE.to_vec();
        bad[87 * 2 + 1] = b'X';
        let mut stats = Stats::default();
        let batch = dec.decode(&bad, &Predicate::True, &mut stats).unwrap();
        assert_eq!(stats.rows_malformed, 1);
        assert_eq!(batch.num_rows(), 3);
    }
}
