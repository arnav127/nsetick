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
//! ~684 million rows per session to get the same row-group pruning.
//!
//! The cost is that a session touches ~2000 symbols at once. Batches go straight into each
//! partition's `ArrowWriter`, which rolls its own row groups; a byte budget across all open
//! partitions closes a row group on the largest whenever the total gets too big. Buffering
//! batches ourselves and concatenating them later was measurably worse: on a session where
//! most partitions never reach the row-group threshold, it degenerates into holding the whole
//! output in memory as thousands of tiny batches.

use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use arrow::array::{Array, StringArray, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use crate::memory::MemoryGuard;

#[derive(Debug, Clone)]
pub struct WriterOptions {
    pub compression: Compression,
    /// Target rows per row group. 256k rows of a CM order record is roughly 20 MB
    /// uncompressed, which keeps row-group pruning granular without fragmenting pages.
    pub row_group_rows: usize,
    /// Ceiling on bytes buffered across all open partitions, in every shard, before the
    /// largest is spilled. Shared globally rather than applied per shard.
    pub max_buffered_bytes: usize,
    /// Bytes each column writer may buffer before it cuts a data page.
    ///
    /// This is the dominant memory cost when writing many partitions at once: every open
    /// partition holds one such buffer per column, so the footprint is roughly
    /// `partitions x columns x page_size`. Large pages compress marginally better; small
    /// pages are what make a 2000-symbol session fit in memory.
    pub data_page_size: usize,
    /// Column to partition on, in addition to the fixed segment/kind/date prefix.
    pub partition_by: Option<String>,
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            // Snappy by default: it decompresses several times faster than zstd, and
            // these files are read far more often than they are written.
            compression: Compression::SNAPPY,
            row_group_rows: 256_000,
            max_buffered_bytes: 256 * 1024 * 1024,
            data_page_size: 64 * 1024,
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

/// Split a batch into one sub-batch per distinct value of the partition column.
///
/// Row order is preserved within each group, which is what keeps each symbol's output sorted
/// by time: NSE writes a symbol's records contiguously and in time order, and `take` with
/// ascending indices does not disturb that.
pub fn split_by_partition(
    batch: &RecordBatch,
    part_col: usize,
) -> Result<Vec<(String, RecordBatch)>> {
    let col = batch
        .column(part_col)
        .as_any()
        .downcast_ref::<StringArray>()
        .context("partition column is not a string array")?;

    let mut groups: HashMap<&str, Vec<u32>> = HashMap::new();
    for i in 0..col.len() {
        let key = if col.is_null(i) { "" } else { col.value(i) };
        groups.entry(key).or_default().push(i as u32);
    }

    // Whole-batch-is-one-partition is common once chunks get small; skip the gather.
    if groups.len() == 1 {
        let key = groups.keys().next().expect("one group").to_string();
        return Ok(vec![(key, batch.clone())]);
    }

    let schema = batch.schema();
    let mut out = Vec::with_capacity(groups.len());
    for (key, idxs) in groups {
        let indices = UInt32Array::from(idxs);
        let cols = batch
            .columns()
            .iter()
            .map(|c| take(c.as_ref(), &indices, None))
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("gathering partition rows")?;
        out.push((
            key.to_string(),
            RecordBatch::try_new(Arc::clone(&schema), cols).context("building partition batch")?,
        ));
    }
    Ok(out)
}

struct Partition {
    writer: ArrowWriter<File>,
    path: PathBuf,
    /// Bytes the writer is holding for the row group it is currently building.
    in_progress: usize,
    rows_total: u64,
}

pub struct PartitionedWriter {
    root: PathBuf,
    /// Shared with every other shard, so limits are totals rather than per-shard allowances
    /// that silently multiply by the thread count.
    budget: Arc<MemoryGuard>,
    prefix: Vec<(String, String)>,
    schema: SchemaRef,
    props: WriterProperties,
    opts: WriterOptions,
    /// Index of the partition column within the schema, resolved once.
    part_col: Option<usize>,
    parts: HashMap<String, Partition>,
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
        let budget = MemoryGuard::new(usize::MAX / 4, opts.max_buffered_bytes);
        Self::with_budget(root, prefix, schema, opts, budget)
    }

    /// Build a shard that shares an existing budget with its siblings.
    pub fn with_budget(
        root: impl AsRef<Path>,
        prefix: Vec<(String, String)>,
        schema: SchemaRef,
        opts: WriterOptions,
        budget: Arc<MemoryGuard>,
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
                if !matches!(
                    schema.field(idx).data_type(),
                    arrow::datatypes::DataType::Utf8
                ) {
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
            .set_data_page_size_limit(opts.data_page_size)
            .set_created_by(format!("nsetick {}", env!("CARGO_PKG_VERSION")))
            .build();

        Ok(Self {
            root: root.as_ref().to_path_buf(),
            budget,
            prefix,
            schema,
            props,
            opts,
            part_col,
            parts: HashMap::new(),
            rows_written: 0,
        })
    }

    /// Index of the partition column in the schema, if partitioning is enabled.
    pub fn part_col(&self) -> Option<usize> {
        self.part_col
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
            self.budget
                .open_partition(self.opts.partition_by.as_deref().unwrap_or("nothing"))?;
            let dir = self.dir_for(key);
            fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
            let path = dir.join("part-000.parquet");
            let file =
                File::create(&path).with_context(|| format!("creating {}", path.display()))?;
            let writer =
                ArrowWriter::try_new(file, Arc::clone(&self.schema), Some(self.props.clone()))
                    .with_context(|| format!("opening parquet writer for {}", path.display()))?;
            self.parts.insert(
                key.to_string(),
                Partition {
                    writer,
                    path,
                    in_progress: 0,
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
        match self.part_col {
            None => self.write_partition("", batch.clone()),
            Some(idx) => {
                for (key, slice) in split_by_partition(batch, idx)? {
                    self.write_partition(&key, slice)?;
                }
                Ok(())
            }
        }
    }

    /// Append a batch that has already been split, all of whose rows belong to `key`.
    ///
    /// Splitting is the second most expensive stage after decoding, so the parallel pipeline
    /// does it on its worker threads and hands the results straight to the owning shard.
    pub fn write_partition(&mut self, key: &str, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let rows = batch.num_rows() as u64;
        self.rows_written += rows;

        let part = self.partition_mut(key)?;
        let before = part.in_progress;
        part.writer
            .write(&batch)
            .with_context(|| format!("writing to {}", part.path.display()))?;
        part.rows_total += rows;
        part.in_progress = part.writer.in_progress_size();
        let after = part.in_progress;

        self.budget.adjust(before, after);

        // Keep the global budget satisfied by construction, with a per-partition cap, so the
        // hot path never walks every open partition.
        //
        // The previous approach searched all open partitions for the largest whenever the
        // shared budget was exceeded, and flushed exactly one per search. Once enough
        // partitions were open to fill the budget - which a full session does and a short
        // sample does not - that search ran on essentially every write, giving O(partitions)
        // work per write and O(partitions^2) per chunk. On a 500-symbol session that is
        // roughly 250,000 scans per chunk, and it is why a full file took three times longer
        // than its own throughput on a 60M-record slice predicted.
        if after > self.partition_cap() {
            self.flush_partition(key)?;
        }
        if self.budget.over_buffer_limit() {
            self.spill_largest()?;
        }
        Ok(())
    }

    /// Bytes any single partition may hold before its row group is closed.
    ///
    /// Dividing the shared allowance by the number of open partitions means the sum cannot
    /// exceed the allowance, so no global check is needed on the common path. The floor keeps
    /// row groups worth writing when a session opens thousands of partitions.
    fn partition_cap(&self) -> usize {
        const FLOOR: usize = 1024 * 1024;
        let open = self.parts.len().max(1);
        (self.budget.buffer_limit() / open).max(FLOOR)
    }

    /// Safety net for when the shared budget is exceeded anyway, because sibling shards are
    /// holding memory or the per-partition floor sums above the allowance.
    ///
    /// One pass over the open partitions, flushing everything substantial, rather than one
    /// search per partition flushed.
    fn spill_largest(&mut self) -> Result<()> {
        let threshold = (self.partition_cap() / 2).max(64 * 1024);
        let keys: Vec<String> = self
            .parts
            .iter()
            .filter(|(_, p)| p.in_progress >= threshold)
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys {
            self.flush_partition(&key)?;
        }
        Ok(())
    }

    fn flush_partition(&mut self, key: &str) -> Result<()> {
        let part = self.parts.get_mut(key).context("unknown partition")?;
        if part.in_progress == 0 {
            return Ok(());
        }
        // Closes the current row group so its statistics are emitted at this granularity.
        part.writer
            .flush()
            .with_context(|| format!("flushing row group in {}", part.path.display()))?;
        self.budget.release(part.in_progress);
        part.in_progress = 0;
        Ok(())
    }

    pub fn rows_written(&self) -> u64 {
        self.rows_written
    }

    pub fn partition_count(&self) -> usize {
        self.parts.len()
    }

    /// Close every file, returning per-partition row counts.
    pub fn finish(mut self) -> Result<Vec<PartitionSummary>> {
        let mut out = Vec::with_capacity(self.parts.len());
        for (key, part) in self.parts.drain() {
            self.budget.release(part.in_progress);
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
    use arrow::array::Int64Array;
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
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap();
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
        w.write(&batch(&["RELIANCE", "TCS", "RELIANCE"], &[1, 2, 3]))
            .unwrap();
        let summary = w.finish().unwrap();

        assert_eq!(summary.len(), 2);
        let rel =
            root.join("segment=cm/kind=orders/date=2022-01-27/symbol=RELIANCE/part-000.parquet");
        assert!(rel.exists(), "expected {}", rel.display());
        assert_eq!(
            read_all(&rel),
            vec![("RELIANCE".into(), 1), ("RELIANCE".into(), 3)]
        );
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
        assert_eq!(
            got,
            (0..50).collect::<Vec<_>>(),
            "rows must stay in arrival order"
        );
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
        w.write(&batch(&["M&M", "COX&KINGS", "NIFTY-50"], &[1, 2, 3]))
            .unwrap();
        w.finish().unwrap();

        assert!(root
            .join("date=2022-01-27/symbol=M&M/part-000.parquet")
            .exists());
        assert!(root
            .join("date=2022-01-27/symbol=COX&KINGS/part-000.parquet")
            .exists());
        assert!(root
            .join("date=2022-01-27/symbol=NIFTY-50/part-000.parquet")
            .exists());
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
    fn the_byte_budget_bounds_what_is_held_in_memory() {
        let root = tmp("nsetick_w_budget");
        let mut w = PartitionedWriter::new(
            &root,
            vec![("date".into(), "2022-01-27".into())],
            schema(),
            WriterOptions {
                row_group_rows: 1_000_000, // never reached, so the budget is what forces flushes
                max_buffered_bytes: 4096,
                ..WriterOptions::default()
            },
        )
        .unwrap();
        for i in 0..500i64 {
            w.write(&batch(&["A", "B", "C"], &[i, i, i])).unwrap();
        }
        // The budget is enforced after each partition write, so the steady state stays near
        // the limit rather than growing with the input.
        assert!(
            w.budget.used() <= 4096 + 64 * 1024,
            "buffer grew to {} bytes",
            w.budget.used()
        );
        let s = w.finish().unwrap();
        assert_eq!(s.iter().map(|p| p.rows).sum::<u64>(), 1500);
        // And every row still made it to disk.
        let a = root.join("date=2022-01-27/symbol=A/part-000.parquet");
        assert_eq!(read_all(&a).len(), 500);
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
        assert_eq!(
            read_all(&root.join("date=2022-01-27/part-000.parquet")).len(),
            2
        );
    }

    #[test]
    fn partitioning_by_a_field_that_was_projected_away_fails_clearly() {
        let narrow = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Int64,
            true,
        )]));
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

    #[test]
    fn split_preserves_row_order_within_each_group() {
        let b = batch(&["A", "B", "A", "C", "B", "A"], &[1, 10, 2, 100, 20, 3]);
        let mut got: Vec<(String, Vec<i64>)> = split_by_partition(&b, 0)
            .unwrap()
            .into_iter()
            .map(|(k, rb)| {
                let c = rb.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                (k, (0..rb.num_rows()).map(|i| c.value(i)).collect())
            })
            .collect();
        got.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            got,
            vec![
                ("A".to_string(), vec![1, 2, 3]),
                ("B".to_string(), vec![10, 20]),
                ("C".to_string(), vec![100]),
            ]
        );
    }
}
