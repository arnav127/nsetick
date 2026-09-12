//! Hive-partitioned Parquet writing.
//!
//! Layout produced:
//!
//! ```text
//! <root>/segment=cm/kind=orders/date=2022-01-27/symbol=RELIANCE/part-000.parquet
//! ```
//!
//! which DuckDB, Polars and PyArrow all discover without configuration.
//!
//! Partitioning by symbol is close to free here. NSE emits records in time order, so
//! demultiplexing by symbol yields partitions that are already sorted by `txn_time` with no
//! sort step at all. Any layout that does not partition by symbol needs an external sort of
//! ~665 million rows per session to get the same row-group pruning.
//!
//! The cost is that a session touches ~2000 symbols at once, and 2000 writers cannot each
//! hold a full row group in memory. A global row budget bounds that: when the total buffered
//! across all partitions exceeds the budget, the largest partition is flushed. Busy symbols
//! therefore get large, well-compressed row groups and illiquid ones get small files, which
//! is the right outcome in both cases.

use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use arrow::array::{Array, StringArray, UInt32Array};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};

#[derive(Debug, Clone)]
pub struct WriterOptions {
    pub compression: Compression,
    /// Target rows per row group. 256k rows of a CM order record is roughly 20 MB
    /// uncompressed, which keeps row-group pruning granular without fragmenting pages.
    pub row_group_rows: usize,
    /// Global ceiling on rows buffered across all partitions before the largest is spilled.
    pub max_buffered_rows: usize,
    /// Column to partition on, in addition to the fixed segment/kind/date prefix.
    pub partition_by: Option<String>,
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            compression: Compression::ZSTD(ZstdLevel::try_new(3).expect("level 3 is valid")),
            row_group_rows: 256_000,
            max_buffered_rows: 4_000_000,
            partition_by: Some("symbol".to_string()),
        }
    }
}

/// Percent-encode the characters Windows forbids in a path component.
///
/// NSE symbols contain `&`, `-` and `.`, all of which are legal in a filename and are left
/// alone so `symbol=M&M` stays readable. The reserved set is encoded rather than replaced so
/// the mapping stays reversible. The partition value is also written as a column inside the
/// file, so the directory name is only ever a locator.
fn sanitize(value: &str) -> String {
    const RESERVED: &[char] = &['<', '>', ':', '"', '/', '\\', '|', '?', '*', '%', '='];
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        if RESERVED.contains(&c) || (c as u32) < 0x20 {
            out.push_str(&format!("%{:02X}", c as u32));
        } else {
            out.push(c);
        }
    }
    if out.is_empty() {
        out.push_str("__empty__");
    }
    out
}

struct Partition {
    writer: ArrowWriter<File>,
    path: PathBuf,
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    rows_total: u64,
}

pub struct PartitionedWriter {
    root: PathBuf,
    prefix: Vec<(String, String)>,
    schema: SchemaRef,
    props: WriterProperties,
    opts: WriterOptions,
    /// Index of the partition column within the schema, resolved once.
    part_col: Option<usize>,
    parts: HashMap<String, Partition>,
    buffered_rows: usize,
    rows_written: u64,
}

impl PartitionedWriter {
    /// `prefix` is the fixed part of the path, e.g. `[("segment","cm"),("kind","orders"),
    /// ("date","2022-01-27")]`.
    pub fn new(
        root: impl AsRef<Path>,
        prefix: Vec<(String, String)>,
        schema: SchemaRef,
        opts: WriterOptions,
    ) -> Result<Self> {
        let part_col = match &opts.partition_by {
            None => None,
            Some(name) => {
                let idx = schema.index_of(name).map_err(|_| {
                    anyhow::anyhow!(
                        "cannot partition by {name:?} because it is not in the output schema. \
                         Either include it in --select or pass --partition-by none."
                    )
                })?;
                // Only a string column can name a directory.
                if !matches!(schema.field(idx).data_type(), arrow::datatypes::DataType::Utf8) {
                    bail!(
                        "cannot partition by {name:?}: it is {:?}, not a string",
                        schema.field(idx).data_type()
                    );
                }
                Some(idx)
            }
        };

        let props = WriterProperties::builder()
            .set_compression(opts.compression)
            .set_max_row_group_row_count(Some(opts.row_group_rows))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_data_page_size_limit(1024 * 1024)
            .set_created_by(format!("nsetick {}", env!("CARGO_PKG_VERSION")))
            .build();

        Ok(Self {
            root: root.as_ref().to_path_buf(),
            prefix,
            schema,
            props,
            opts,
            part_col,
            parts: HashMap::new(),
            buffered_rows: 0,
            rows_written: 0,
        })
    }

    fn dir_for(&self, key: &str) -> PathBuf {
        let mut p = self.root.clone();
        for (k, v) in &self.prefix {
            p.push(format!("{k}={}", sanitize(v)));
        }
        if let Some(name) = &self.opts.partition_by {
            p.push(format!("{name}={}", sanitize(key)));
        }
        p
    }

    fn partition_mut(&mut self, key: &str) -> Result<&mut Partition> {
        if !self.parts.contains_key(key) {
            let dir = self.dir_for(key);
            fs::create_dir_all(&dir)
                .with_context(|| format!("creating {}", dir.display()))?;
            let path = dir.join("part-000.parquet");
            let file = File::create(&path)
                .with_context(|| format!("creating {}", path.display()))?;
            let writer = ArrowWriter::try_new(
                file,
                Arc::clone(&self.schema),
                Some(self.props.clone()),
            )
            .with_context(|| format!("opening parquet writer for {}", path.display()))?;
            self.parts.insert(
                key.to_string(),
                Partition {
                    writer,
                    path,
                    pending: Vec::new(),
                    pending_rows: 0,
                    rows_total: 0,
                },
            );
        }
        Ok(self.parts.get_mut(key).expect("just inserted"))
    }

    /// Append a batch, splitting it across partitions as needed.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.rows_written += batch.num_rows() as u64;

        match self.part_col {
            None => self.push("", batch.clone())?,
            Some(idx) => {
                let col = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context("partition column is not a string array")?;

                // Group row indices by partition value, then gather each group with `take`.
                let mut groups: HashMap<&str, Vec<u32>> = HashMap::new();
                for i in 0..col.len() {
                    let key = if col.is_null(i) { "" } else { col.value(i) };
                    groups.entry(key).or_default().push(i as u32);
                }

                // A single-symbol batch is the common case once chunks get small; avoid the
                // copy entirely when the whole batch belongs to one partition.
                if groups.len() == 1 {
                    let key = groups.keys().next().expect("one group").to_string();
                    self.push(&key, batch.clone())?;
                    return Ok(());
                }

                let mut slices: Vec<(String, RecordBatch)> = Vec::with_capacity(groups.len());
                for (key, idxs) in groups {
                    let indices = UInt32Array::from(idxs);
                    let cols = batch
                        .columns()
                        .iter()
                        .map(|c| take(c.as_ref(), &indices, None))
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .context("gathering partition rows")?;
                    slices.push((
                        key.to_string(),
                        RecordBatch::try_new(Arc::clone(&self.schema), cols)
                            .context("building partition batch")?,
                    ));
                }
                for (key, slice) in slices {
                    self.push(&key, slice)?;
                }
            }
        }

        self.enforce_budget()
    }

    fn push(&mut self, key: &str, batch: RecordBatch) -> Result<()> {
        let rows = batch.num_rows();
        let target = self.opts.row_group_rows;
        let part = self.partition_mut(key)?;
        part.pending.push(batch);
        part.pending_rows += rows;
        part.rows_total += rows as u64;
        self.buffered_rows += rows;

        if self.parts[key].pending_rows >= target {
            self.flush_partition(key)?;
        }
        Ok(())
    }

    /// Spill the largest partitions until the global buffer is back under budget.
    fn enforce_budget(&mut self) -> Result<()> {
        while self.buffered_rows > self.opts.max_buffered_rows {
            let Some(key) = self
                .parts
                .iter()
                .filter(|(_, p)| p.pending_rows > 0)
                .max_by_key(|(_, p)| p.pending_rows)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            self.flush_partition(&key)?;
        }
        Ok(())
    }

    fn flush_partition(&mut self, key: &str) -> Result<()> {
        let schema = Arc::clone(&self.schema);
        let part = self.parts.get_mut(key).context("unknown partition")?;
        if part.pending.is_empty() {
            return Ok(());
        }
        let merged = concat_batches(&schema, part.pending.iter())
            .with_context(|| format!("concatenating pending batches for {key:?}"))?;
        part.writer
            .write(&merged)
            .with_context(|| format!("writing {}", part.path.display()))?;
        // Close the row group so statistics are emitted at this granularity.
        part.writer
            .flush()
            .with_context(|| format!("flushing row group in {}", part.path.display()))?;

        self.buffered_rows -= part.pending_rows;
        part.pending.clear();
        part.pending_rows = 0;
        Ok(())
    }

    pub fn rows_written(&self) -> u64 {
        self.rows_written
    }

    pub fn partition_count(&self) -> usize {
        self.parts.len()
    }

    /// Flush every partition and close every file, returning per-partition row counts.
    pub fn finish(mut self) -> Result<Vec<PartitionSummary>> {
        let keys: Vec<String> = self.parts.keys().cloned().collect();
        for key in &keys {
            self.flush_partition(key)?;
        }

        let mut out = Vec::with_capacity(self.parts.len());
        for (key, part) in self.parts.drain() {
            let path = part.path.clone();
            let rows = part.rows_total;
            part.writer
                .close()
                .with_context(|| format!("closing {}", path.display()))?;
            let bytes = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            out.push(PartitionSummary {
                key,
                path,
                rows,
                bytes,
            });
        }
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }
}

#[derive(Debug, Clone)]
pub struct PartitionSummary {
    pub key: String,
    pub path: PathBuf,
    pub rows: u64,
    pub bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, true),
            Field::new("price", DataType::Int64, true),
        ]))
    }

    fn batch(symbols: &[&str], prices: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(StringArray::from(symbols.to_vec())),
                Arc::new(Int64Array::from(prices.to_vec())),
            ],
        )
        .unwrap()
    }

    fn tmp(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(name);
        let _ = fs::remove_dir_all(&p);
        p
    }

    fn read_all(path: &Path) -> Vec<(String, i64)> {
        let f = File::open(path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(f).unwrap().build().unwrap();
        let mut out = Vec::new();
        for b in reader {
            let b = b.unwrap();
            let s = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let p = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                out.push((s.value(i).to_string(), p.value(i)));
            }
        }
        out
    }

    #[test]
    fn splits_rows_into_one_directory_per_symbol() {
        let root = tmp("nsetick_w_split");
        let prefix = vec![
            ("segment".into(), "cm".into()),
            ("kind".into(), "orders".into()),
            ("date".into(), "2022-01-27".into()),
        ];
        let mut w =
            PartitionedWriter::new(&root, prefix, schema(), WriterOptions::default()).unwrap();
        w.write(&batch(&["RELIANCE", "TCS", "RELIANCE"], &[1, 2, 3])).unwrap();
        let summary = w.finish().unwrap();

        assert_eq!(summary.len(), 2);
        let rel = root
            .join("segment=cm/kind=orders/date=2022-01-27/symbol=RELIANCE/part-000.parquet");
        assert!(rel.exists(), "expected {}", rel.display());
        assert_eq!(read_all(&rel), vec![("RELIANCE".into(), 1), ("RELIANCE".into(), 3)]);
    }

    #[test]
    fn time_order_is_preserved_within_a_partition() {
        let root = tmp("nsetick_w_order");
        let mut w = PartitionedWriter::new(
            &root,
            vec![("date".into(), "2022-01-27".into())],
            schema(),
            WriterOptions {
                // Force many small row groups so ordering across flushes is exercised.
                row_group_rows: 2,
                ..WriterOptions::default()
            },
        )
        .unwrap();
        for i in 0..50i64 {
            w.write(&batch(&["RELIANCE", "TCS"], &[i, -i])).unwrap();
        }
        w.finish().unwrap();

        let rel = root.join("date=2022-01-27/symbol=RELIANCE/part-000.parquet");
        let got: Vec<i64> = read_all(&rel).into_iter().map(|(_, p)| p).collect();
        assert_eq!(got, (0..50).collect::<Vec<_>>(), "rows must stay in arrival order");
    }

    #[test]
    fn symbols_with_ampersands_get_a_usable_directory_name() {
        let root = tmp("nsetick_w_amp");
        let mut w = PartitionedWriter::new(
            &root,
            vec![("date".into(), "2022-01-27".into())],
            schema(),
            WriterOptions::default(),
        )
        .unwrap();
        w.write(&batch(&["M&M", "COX&KINGS", "NIFTY-50"], &[1, 2, 3])).unwrap();
        w.finish().unwrap();

        assert!(root.join("date=2022-01-27/symbol=M&M/part-000.parquet").exists());
        assert!(root.join("date=2022-01-27/symbol=COX&KINGS/part-000.parquet").exists());
        assert!(root.join("date=2022-01-27/symbol=NIFTY-50/part-000.parquet").exists());
    }

    #[test]
    fn characters_windows_forbids_are_encoded_not_dropped() {
        assert_eq!(sanitize("M&M"), "M&M");
        assert_eq!(sanitize("A*B"), "A%2AB");
        assert_eq!(sanitize("A/B"), "A%2FB");
        assert_eq!(sanitize("A:B"), "A%3AB");
        assert_eq!(sanitize(""), "__empty__");
    }

    #[test]
    fn the_global_budget_bounds_buffered_rows() {
        let root = tmp("nsetick_w_budget");
        let mut w = PartitionedWriter::new(
            &root,
            vec![("date".into(), "2022-01-27".into())],
            schema(),
            WriterOptions {
                row_group_rows: 1_000_000, // never reached
                max_buffered_rows: 10,     // so the budget is what forces flushes
                ..WriterOptions::default()
            },
        )
        .unwrap();
        for i in 0..100i64 {
            w.write(&batch(&["A", "B", "C"], &[i, i, i])).unwrap();
            assert!(
                w.buffered_rows <= 10 + 3,
                "buffer grew to {} rows",
                w.buffered_rows
            );
        }
        let s = w.finish().unwrap();
        assert_eq!(s.iter().map(|p| p.rows).sum::<u64>(), 300);
    }

    #[test]
    fn writing_without_partitioning_produces_a_single_file() {
        let root = tmp("nsetick_w_flat");
        let mut w = PartitionedWriter::new(
            &root,
            vec![("date".into(), "2022-01-27".into())],
            schema(),
            WriterOptions {
                partition_by: None,
                ..WriterOptions::default()
            },
        )
        .unwrap();
        w.write(&batch(&["RELIANCE", "TCS"], &[1, 2])).unwrap();
        let s = w.finish().unwrap();
        assert_eq!(s.len(), 1);
        assert_eq!(read_all(&root.join("date=2022-01-27/part-000.parquet")).len(), 2);
    }

    #[test]
    fn partitioning_by_a_field_that_was_projected_away_fails_clearly() {
        let narrow = Arc::new(Schema::new(vec![Field::new("price", DataType::Int64, true)]));
        // PartitionedWriter holds an ArrowWriter and is not Debug, so unwrap_err is out.
        let err = match PartitionedWriter::new(
            tmp("nsetick_w_missing"),
            vec![],
            narrow,
            WriterOptions::default(),
        ) {
            Ok(_) => panic!("partitioning by a projected-away field should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("--select"), "{err}");
    }
}
