//! Layout registry.
//!
//! Deserialises the TOML specs in `spec/layouts`, which are embedded into the binary at
//! compile time so a released `nsetick` needs no data files alongside it. The very same
//! files are read by the Python package, so the two can never disagree about an offset.

use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use serde::Deserialize;

/// The spec files, embedded. Adding a layout means adding it here and in `spec/layouts`.
const EMBEDDED: &[(&str, &str)] = &[
    ("cm_orders", include_str!("../../../spec/layouts/cm_orders.toml")),
    ("cm_trades", include_str!("../../../spec/layouts/cm_trades.toml")),
    ("cm_index", include_str!("../../../spec/layouts/cm_index.toml")),
    ("fao_orders", include_str!("../../../spec/layouts/fao_orders.toml")),
    ("fao_trades", include_str!("../../../spec/layouts/fao_trades.toml")),
    ("cd_orders", include_str!("../../../spec/layouts/cd_orders.toml")),
    ("cd_trades", include_str!("../../../spec/layouts/cd_trades.toml")),
];

/// NSE writes a literal `b` (0x62) as its blank fill byte, because the layout document
/// denotes a blank as "b" and the generator emitted the notation instead of the character
/// it stands for. Genuine 0x20 spaces occur too. Nothing else is ever stripped: `&`, `-`,
/// `*` and `.` are all legal inside NSE symbols.
pub const PAD_BYTES: &[u8] = b"b ";

#[inline]
pub fn is_pad(b: u8) -> bool {
    b == b'b' || b == b' '
}

/// Strip leading pad bytes. Used by every numeric parse, since NSE zero-fills numbers but
/// pad-fills the fields that may be blank.
#[inline]
pub fn unpad_left(raw: &[u8]) -> &[u8] {
    let i = raw.iter().position(|b| !is_pad(*b)).unwrap_or(raw.len());
    &raw[i..]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    Str,
    U8,
    U64,
    Price,
    Jiffies,
    BoolYn,
    DateDmmmy,
    DateYmd,
    TimeHms,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Pad {
    Left,
    Right,
    Both,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Field {
    pub name: String,
    pub offset: usize,
    pub len: usize,
    #[serde(rename = "type")]
    pub ty: FieldType,
    #[serde(default)]
    pub pad: Option<Pad>,
    #[serde(default)]
    pub scale: Option<u32>,
    #[serde(default)]
    pub doc: String,
}

impl Field {
    #[inline]
    pub fn end(&self) -> usize {
        self.offset + self.len
    }

    /// Slice this field out of a record. The caller is responsible for having checked the
    /// record length once, rather than paying for a bounds check per field.
    #[inline]
    pub fn slice<'a>(&self, record: &'a [u8]) -> &'a [u8] {
        &record[self.offset..self.offset + self.len]
    }

    /// Strip only the pad bytes, only from the side the layout declares.
    #[inline]
    pub fn unpad<'a>(&self, raw: &'a [u8]) -> &'a [u8] {
        match self.pad {
            None => raw,
            Some(Pad::Left) => {
                let i = raw.iter().position(|b| !is_pad(*b)).unwrap_or(raw.len());
                &raw[i..]
            }
            Some(Pad::Right) => {
                let i = raw.iter().rposition(|b| !is_pad(*b)).map_or(0, |i| i + 1);
                &raw[..i]
            }
            Some(Pad::Both) => {
                let s = raw.iter().position(|b| !is_pad(*b)).unwrap_or(raw.len());
                let e = raw.iter().rposition(|b| !is_pad(*b)).map_or(s, |i| i + 1);
                &raw[s..e]
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Version {
    pub spec_version: String,
    #[serde(deserialize_with = "de_date")]
    pub valid_from: NaiveDate,
    #[serde(deserialize_with = "de_date")]
    pub valid_to: NaiveDate,
    pub record_length: usize,
    #[serde(default = "yes")]
    pub verified: bool,
    pub fields: Vec<Field>,
}

fn yes() -> bool {
    true
}

/// TOML bare dates arrive as `toml::value::Datetime`; convert to `NaiveDate`.
fn de_date<'de, D: serde::Deserializer<'de>>(d: D) -> Result<NaiveDate, D::Error> {
    use serde::de::Error;
    let dt = toml::value::Datetime::deserialize(d)?;
    let date = dt.date.ok_or_else(|| D::Error::custom("expected a date"))?;
    NaiveDate::from_ymd_opt(date.year as i32, date.month as u32, date.day as u32)
        .ok_or_else(|| D::Error::custom(format!("invalid date {date:?}")))
}

impl Version {
    /// Record length including the LF delimiter.
    #[inline]
    pub fn line_length(&self) -> usize {
        self.record_length + 1
    }

    pub fn field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Structural check, mirroring `spec/validate_layouts.py`. Fields must tile the record
    /// exactly: no gaps, no overlaps, summing to `record_length`.
    fn validate(&self, layout_id: &str) -> Result<()> {
        let mut cursor = 0usize;
        let mut seen = std::collections::HashSet::new();
        for f in &self.fields {
            if !seen.insert(f.name.as_str()) {
                bail!("{layout_id}@{}: duplicate field {}", self.spec_version, f.name);
            }
            if f.len == 0 {
                bail!("{layout_id}@{}: {} has zero length", self.spec_version, f.name);
            }
            if f.offset != cursor {
                bail!(
                    "{layout_id}@{}: {} starts at {} but the previous field ended at {cursor}",
                    self.spec_version,
                    f.name,
                    f.offset
                );
            }
            if f.ty == FieldType::Price && f.scale.is_none() {
                bail!(
                    "{layout_id}@{}: price field {} declares no scale",
                    self.spec_version,
                    f.name
                );
            }
            cursor = f.end();
        }
        if cursor != self.record_length {
            bail!(
                "{layout_id}@{}: fields span {cursor} bytes, record_length is {}",
                self.spec_version,
                self.record_length
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Meta {
    pub id: String,
    pub segment: String,
    pub kind: String,
    pub description: String,
    pub file_glob: String,
    #[serde(default)]
    pub split_files: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Layout {
    #[serde(rename = "layout")]
    pub meta: Meta,
    #[serde(rename = "version")]
    pub versions: Vec<Version>,
}

impl Layout {
    pub fn parse(id: &str, toml_src: &str) -> Result<Self> {
        let layout: Layout =
            toml::from_str(toml_src).with_context(|| format!("parsing layout {id}"))?;
        if layout.meta.id != id {
            bail!("layout id {:?} does not match file name {id:?}", layout.meta.id);
        }
        for v in &layout.versions {
            v.validate(id)?;
        }
        // Length-based detection requires lengths to be unique across versions, and date
        // based selection requires the ranges not to overlap.
        for (i, a) in layout.versions.iter().enumerate() {
            for b in &layout.versions[i + 1..] {
                if a.record_length == b.record_length {
                    bail!(
                        "{id}: versions {} and {} share record_length {} - length-based \
                         detection would be ambiguous",
                        a.spec_version,
                        b.spec_version,
                        a.record_length
                    );
                }
                if a.valid_from <= b.valid_to && b.valid_from <= a.valid_to {
                    bail!(
                        "{id}: date ranges of versions {} and {} overlap",
                        a.spec_version,
                        b.spec_version
                    );
                }
            }
        }
        Ok(layout)
    }

    pub fn for_date(&self, on: NaiveDate) -> Result<&Version> {
        self.versions
            .iter()
            .find(|v| v.valid_from <= on && on <= v.valid_to)
            .with_context(|| format!("{}: no layout version covers {on}", self.meta.id))
    }

    pub fn for_record_length(&self, len: usize) -> Result<&Version> {
        self.versions
            .iter()
            .find(|v| v.record_length == len)
            .with_context(|| {
                let known: Vec<usize> = self.versions.iter().map(|v| v.record_length).collect();
                format!(
                    "{}: observed record length {len} matches no known version (known: \
                     {known:?}). Refusing to guess - a wrong layout shifts every field after \
                     the mismatch and yields silently corrupt output.",
                    self.meta.id
                )
            })
    }

    /// Pick a version by date, cross-checked against the observed record length.
    ///
    /// The dates in the layout document's revision history are revision dates, not
    /// necessarily feed changeover dates, so they are approximate. The observed record
    /// length is ground truth and wins, with a warning, when the two disagree.
    pub fn resolve(&self, on: NaiveDate, observed_length: Option<usize>) -> Result<&Version> {
        let by_date = self.for_date(on)?;
        match observed_length {
            None => Ok(by_date),
            Some(len) if len == by_date.record_length => Ok(by_date),
            Some(len) => {
                let by_len = self.for_record_length(len)?;
                eprintln!(
                    "warning: {}: date {on} selects spec {} ({}B) but records are {len}B; \
                     using spec {}. The valid_from boundary in spec/layouts/{}.toml is \
                     probably wrong.",
                    self.meta.id,
                    by_date.spec_version,
                    by_date.record_length,
                    by_len.spec_version,
                    self.meta.id
                );
                Ok(by_len)
            }
        }
    }

    pub fn filename_pattern(&self, date_str: &str) -> String {
        self.meta.file_glob.replace("{date}", date_str)
    }
}

/// All embedded layouts, parsed and validated.
pub fn registry() -> Result<HashMap<&'static str, Layout>> {
    EMBEDDED
        .iter()
        .map(|(id, src)| Ok((*id, Layout::parse(id, src)?)))
        .collect()
}

pub fn load(id: &str) -> Result<Layout> {
    let (_, src) = EMBEDDED
        .iter()
        .find(|(name, _)| *name == id)
        .with_context(|| {
            let ids: Vec<&str> = EMBEDDED.iter().map(|(n, _)| *n).collect();
            format!("unknown layout {id:?}; available: {ids:?}")
        })?;
    Layout::parse(id, src)
}

pub fn available() -> Vec<&'static str> {
    EMBEDDED.iter().map(|(id, _)| *id).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    #[test]
    fn every_embedded_layout_parses_and_validates() {
        let reg = registry().expect("registry should load");
        assert_eq!(reg.len(), 7);
    }

    #[test]
    fn fao_trades_version_switches_on_the_2020_boundary() {
        let l = load("fao_trades").unwrap();
        let before = l.for_date(d(2019, 5, 1)).unwrap();
        let after = l.for_date(d(2022, 5, 1)).unwrap();
        assert_eq!(before.record_length, 123);
        assert_eq!(after.record_length, 124);
        // The one-byte trade number change shifts every subsequent field.
        assert_eq!(before.field("symbol").unwrap().offset, 36);
        assert_eq!(after.field("symbol").unwrap().offset, 37);
    }

    #[test]
    fn observed_length_overrides_a_wrong_date_boundary() {
        let l = load("fao_trades").unwrap();
        let v = l.resolve(d(2020, 9, 1), Some(124)).unwrap();
        assert_eq!(v.record_length, 124);
    }

    #[test]
    fn unknown_record_length_is_refused_not_guessed() {
        let l = load("cm_orders").unwrap();
        assert!(l.for_record_length(86).is_err());
    }

    #[test]
    fn cd_prices_are_scale_four_and_cm_prices_scale_two() {
        let cd = load("cd_orders").unwrap();
        let v = cd.for_date(d(2022, 5, 1)).unwrap();
        assert_eq!(v.field("limit_price").unwrap().scale, Some(4));

        let cm = load("cm_orders").unwrap();
        let v = cm.for_date(d(2022, 5, 1)).unwrap();
        assert_eq!(v.field("limit_price").unwrap().scale, Some(2));
    }

    #[test]
    fn unpad_strips_b_fill_but_preserves_symbol_characters() {
        let l = load("cm_orders").unwrap();
        let v = l.for_date(d(2022, 1, 27)).unwrap();
        let sym = v.field("symbol").unwrap();

        // Literal 'b' fill, as actually emitted by NSE.
        assert_eq!(sym.unpad(b"bbbbbbbBCG"), b"BCG");
        // Real spaces occur in some files too.
        assert_eq!(sym.unpad(b"  RELIANCE"), b"RELIANCE");
        // An unpadded 10-character symbol is untouched.
        assert_eq!(sym.unpad(b"BALKRISIND"), b"BALKRISIND");
        // The whole point: '&' and '-' must survive. A [A-Z0-9-]+ regex turns these into
        // "M" and "COX", which is what the existing pipelines do today.
        assert_eq!(sym.unpad(b"bbbbbbbM&M"), b"M&M");
        assert_eq!(sym.unpad(b"bCOX&KINGS"), b"COX&KINGS");
        assert_eq!(sym.unpad(b"bbNIFTY-50"), b"NIFTY-50");
    }

    #[test]
    fn segment_is_right_padded_in_derivatives() {
        let l = load("fao_orders").unwrap();
        let v = l.for_date(d(2022, 5, 1)).unwrap();
        let seg = v.field("segment").unwrap();
        assert_eq!(seg.unpad(b"FAOb"), b"FAO");
    }
}
