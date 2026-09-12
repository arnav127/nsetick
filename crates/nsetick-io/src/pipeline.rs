//! End-to-end parse: gzip in, partitioned Parquet out.
//!
//! A deflate stream cannot be decompressed by more than one thread, so inflate is a fixed
//! serial cost and everything else has to get out of its way. Measured on a 30M-record slice
//! of a CM orders session, all 17 columns:
//!
//! ```text
//!   inflate                     4.3s
//!   decode + parquet encode    ~24.5s
//!   partition split            ~13.3s
//!   zstd                       ~10.6s
//! ```
//!
//! Inflate is under a tenth of the work, and the rest parallelises, so the pipeline is:
//!
//! ```text
//!   inflate thread  ->  decode workers  ->  reorder  ->  writer shards
//!      (serial)          (N threads)        (1)          (M threads)
//! ```
//!
//! Decode workers also do the partition split, since that is the second most expensive stage
//! and is embarrassingly parallel. Writer shards own disjoint sets of symbols, so each owns
//! its own files and does its own Parquet encoding and compression with no shared state.
//!
//! Ordering is preserved deliberately: chunks carry a sequence number and are re-ordered
//! before routing, so each symbol's rows reach its writer in the order NSE emitted them.
//! That is what makes the output time-sorted without a sort.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use nsetick_core::decode::{DecodeOptions, Decoder, Stats};
use nsetick_core::filter::{self, Predicate};
use nsetick_core::layout::{self, Layout, Version};

use crate::manifest::{Manifest, PartitionRecord};
use crate::memory::{self, MemoryGuard};
use crate::reader::{read_trigger, RecordReader, DEFAULT_CHUNK_BYTES};
use crate::writer::{split_by_partition, PartitionSummary, PartitionedWriter, WriterOptions};

#[derive(Debug, Clone)]
pub struct ParseRequest {
    pub input: PathBuf,
    pub layout_id: String,
    pub session_date: NaiveDate,
    pub out_root: PathBuf,
    /// Fields to emit. `None` means all of them.
    pub select: Option<Vec<String>>,
    /// Filter expression; empty means keep everything.
    pub filter: String,
    pub strict: bool,
    pub writer: WriterOptions,
    /// Check the file size against its `.trg` sidecar before doing any work.
    pub verify_trigger: bool,
    pub chunk_bytes: usize,
    /// Stop after roughly this many records have been read. Rounded up to a chunk boundary.
    /// Intended for smoke tests against multi-gigabyte sessions.
    pub max_records: Option<u64>,
    /// Ceiling on bytes buffered across every writer shard. `None` derives one from the
    /// memory currently available on the machine.
    pub memory_limit: Option<usize>,
    /// Free-text note recorded in the manifest.
    pub note: Option<String>,
    /// Worker threads for decoding and writing. `None` picks a default from the machine.
    /// `Some(1)` runs the whole pipeline on one thread besides the inflate.
    pub threads: Option<usize>,
}

impl ParseRequest {
    pub fn new(
        input: impl Into<PathBuf>,
        layout_id: impl Into<String>,
        session_date: NaiveDate,
        out_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            input: input.into(),
            layout_id: layout_id.into(),
            session_date,
            out_root: out_root.into(),
            select: None,
            filter: String::new(),
            strict: true,
            writer: WriterOptions::default(),
            verify_trigger: true,
            chunk_bytes: DEFAULT_CHUNK_BYTES,
            max_records: None,
            memory_limit: None,
            note: None,
            threads: None,
        }
    }
}

/// Leave a core for the inflate thread and one for the OS; never fewer than one worker.
pub fn default_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().saturating_sub(2).max(1))
        .unwrap_or(1)
}

#[derive(Debug, Clone)]
pub struct RunReport {
    pub stats: Stats,
    pub bytes_decompressed: u64,
    pub partitions: usize,
    pub elapsed_secs: f64,
    pub manifest_path: PathBuf,
    pub spec_version: String,
    pub record_length: usize,
    pub threads: usize,
    /// Footprint ceiling for the run, and the estimated peak it actually reached.
    pub memory_limit: usize,
    pub memory_peak: usize,
    pub partitions_peak: usize,
}

impl RunReport {
    pub fn throughput_mb_s(&self) -> f64 {
        if self.elapsed_secs <= 0.0 {
            return 0.0;
        }
        self.bytes_decompressed as f64 / self.elapsed_secs / 1e6
    }

    pub fn rows_per_sec(&self) -> f64 {
        if self.elapsed_secs <= 0.0 {
            return 0.0;
        }
        self.stats.rows_read as f64 / self.elapsed_secs
    }
}

/// Determine the record length by finding the first LF in the decompressed stream.
///
/// This is what makes a wrong layout a startup error instead of hundreds of millions of
/// silently misaligned rows. It reads only the first few kilobytes.
pub fn probe_record_length(path: &Path) -> Result<usize> {
    // 1024 is comfortably larger than the longest NSE record (124 bytes).
    let mut reader = RecordReader::open(path, 1, 4096)?;
    let chunk = reader.next_chunk()?.context("file is empty")?;
    let head = &chunk[..chunk.len().min(1024)];
    match head.iter().position(|b| *b == b'\n') {
        // The record length excludes the delimiter.
        Some(pos) => Ok(pos),
        None => bail!(
            "{}: no LF found in the first {} bytes, so this does not look like an \
             LF-delimited fixed-width NSE file",
            path.display(),
            head.len()
        ),
    }
}

/// Resolve which layout version applies to a file, by date and by observed record length.
pub fn resolve_version(layout: &Layout, path: &Path, date: NaiveDate) -> Result<(String, usize)> {
    let observed = probe_record_length(path)?;
    let version = layout.resolve(date, Some(observed))?;
    Ok((version.spec_version.clone(), version.record_length))
}

fn shard_of(key: &str, shards: usize) -> usize {
    if shards <= 1 {
        return 0;
    }
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    (h.finish() % shards as u64) as usize
}

/// One decoded, already-split chunk on its way to the writers.
///
/// Groups are bucketed by destination shard here rather than routed one at a time. A chunk
/// of a CM session splits into ~625 per-symbol batches, and sending those individually cost
/// more in channel traffic than the parallel writers saved.
struct Decoded {
    seq: u64,
    by_shard: Vec<Vec<(String, RecordBatch)>>,
    stats: Stats,
}

pub fn run(req: &ParseRequest) -> Result<RunReport> {
    let started = Instant::now();

    let layout = layout::load(&req.layout_id)?;

    if req.verify_trigger {
        if let Some(trigger) = read_trigger(&req.input)? {
            trigger.check_size(&req.input)?;
        }
    }

    // Pick the layout version from the file itself, not from an assumption.
    let observed = probe_record_length(&req.input)
        .with_context(|| format!("probing record length of {}", req.input.display()))?;
    let version: Version = layout.resolve(req.session_date, Some(observed))?.clone();

    if !version.verified {
        eprintln!(
            "warning: layout {}@{} has not been checked against a real file \
             (verified = false in spec/layouts/{}.toml). Treat the output as provisional.",
            req.layout_id, version.spec_version, req.layout_id
        );
    }

    let decoder = Decoder::new(
        &version,
        req.select.as_deref(),
        DecodeOptions { strict: req.strict },
    )?;
    let predicate = filter::compile(&req.filter, &version, Some(req.session_date))?;

    let prefix = vec![
        ("segment".to_string(), layout.meta.segment.to_lowercase()),
        ("kind".to_string(), layout.meta.kind.clone()),
        ("date".to_string(), req.session_date.to_string()),
    ];

    let threads = req.threads.unwrap_or_else(default_threads).max(1);

    // Everything outside the writer budget's control: chunks in flight on the channels, the
    // decoded batches queued behind them, and the fixed cost of thousands of open writers.
    let headroom = req.chunk_bytes * (threads * 4 + 8) + 256 * 1024 * 1024;
    let (auto_footprint, auto_buffer) = memory::default_limits(headroom);
    let footprint = req.memory_limit.unwrap_or(auto_footprint);
    // Buffering is elastic and measurement showed it does not affect throughput, so it never
    // takes more than a quarter of the footprint the partitions need.
    let buffer = auto_buffer.min(footprint / 4).max(64 * 1024 * 1024);
    let budget = MemoryGuard::new(footprint, buffer);

    let (stats, bytes_decompressed, partitions) = execute(
        req,
        &version,
        decoder,
        predicate,
        prefix,
        threads,
        Arc::clone(&budget),
    )?;

    let elapsed = started.elapsed().as_secs_f64();

    let manifest = Manifest {
        nsetick_version: env!("CARGO_PKG_VERSION").to_string(),
        created_utc: chrono::Utc::now().to_rfc3339(),
        source_path: req.input.display().to_string(),
        source_bytes: std::fs::metadata(&req.input).map(|m| m.len()).unwrap_or(0),
        bytes_decompressed,
        layout_id: req.layout_id.clone(),
        spec_version: version.spec_version.clone(),
        record_length: version.record_length,
        layout_verified: version.verified,
        session_date: req.session_date.to_string(),
        note: req.note.clone(),
        filter: req.filter.clone(),
        selected_fields: req
            .select
            .clone()
            .unwrap_or_else(|| version.fields.iter().map(|f| f.name.clone()).collect()),
        partition_by: req.writer.partition_by.clone(),
        strict: req.strict,
        rows_read: stats.rows_read,
        rows_emitted: stats.rows_emitted,
        rows_filtered: stats.rows_filtered,
        rows_malformed: stats.rows_malformed,
        elapsed_secs: elapsed,
        partitions: partitions
            .iter()
            .map(|p| PartitionRecord {
                key: p.key.clone(),
                rows: p.rows,
                bytes: p.bytes,
            })
            .collect(),
    };

    let manifest_path = manifest.write(&req.out_root, &req.layout_id, req.session_date)?;

    Ok(RunReport {
        stats,
        bytes_decompressed,
        partitions: partitions.len(),
        elapsed_secs: elapsed,
        manifest_path,
        spec_version: version.spec_version,
        record_length: version.record_length,
        threads,
        memory_limit: budget.footprint_limit(),
        memory_peak: budget.projected_bytes(),
        partitions_peak: budget.partitions_peak(),
    })
}

#[allow(clippy::type_complexity)]
fn execute(
    req: &ParseRequest,
    version: &Version,
    decoder: Decoder,
    predicate: Predicate,
    prefix: Vec<(String, String)>,
    threads: usize,
    budget: Arc<MemoryGuard>,
) -> Result<(Stats, u64, Vec<PartitionSummary>)> {
    let schema = decoder.schema();
    let part_col = match &req.writer.partition_by {
        None => None,
        Some(name) => Some(schema.index_of(name).map_err(|_| {
            anyhow::anyhow!(
                "cannot partition by {name:?} because it is not in the output schema. \
                 Either include it in --select or pass --partition-by none."
            )
        })?),
    };

    // Without a partition column every shard would write the same file path, so there can
    // only be one writer.
    let shards = if part_col.is_some() { threads } else { 1 };

    let decoder = Arc::new(decoder);
    let predicate = Arc::new(predicate);
    let line_len = version.line_length();

    let (chunk_tx, chunk_rx) = crossbeam_channel::bounded::<(u64, Vec<u8>)>(threads * 2);
    let (dec_tx, dec_rx) = crossbeam_channel::bounded::<Result<Decoded>>(threads * 2);

    type ShardMsg = Vec<(String, RecordBatch)>;
    let mut shard_txs = Vec::with_capacity(shards);
    let mut shard_rxs = Vec::with_capacity(shards);
    for _ in 0..shards {
        let (t, r) = crossbeam_channel::bounded::<ShardMsg>(8);
        shard_txs.push(t);
        shard_rxs.push(r);
    }

    let mut stats = Stats::default();
    let mut bytes_decompressed = 0u64;
    let mut summaries: Vec<PartitionSummary> = Vec::new();
    let mut route_error: Option<anyhow::Error> = None;

    std::thread::scope(|scope| -> Result<()> {
        // --- inflate ------------------------------------------------------------------
        let input = req.input.clone();
        let chunk_bytes = req.chunk_bytes;
        let reader_handle = scope.spawn(move || -> Result<u64> {
            let mut reader = RecordReader::open(&input, line_len, chunk_bytes)?;
            let mut seq = 0u64;
            loop {
                match reader.next_chunk() {
                    Ok(Some(chunk)) => {
                        if chunk_tx.send((seq, chunk)).is_err() {
                            // Consumers stopped; no point inflating the rest of 56 GB.
                            break;
                        }
                        seq += 1;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        drop(chunk_tx);
                        return Err(e);
                    }
                }
            }
            Ok(reader.bytes_read())
        });

        // --- decode + split -----------------------------------------------------------
        let mut decode_handles = Vec::with_capacity(threads);
        for _ in 0..threads {
            let rx = chunk_rx.clone();
            let tx = dec_tx.clone();
            let decoder = Arc::clone(&decoder);
            let predicate = Arc::clone(&predicate);
            decode_handles.push(scope.spawn(move || {
                for (seq, chunk) in rx.iter() {
                    let mut local = Stats::default();
                    let decoded = decoder
                        .decode(&chunk, &predicate, &mut local)
                        .and_then(|batch| {
                            let mut by_shard: Vec<Vec<(String, RecordBatch)>> =
                                vec![Vec::new(); shards];
                            if batch.num_rows() > 0 {
                                match part_col {
                                    None => by_shard[0].push((String::new(), batch)),
                                    Some(idx) => {
                                        for (key, slice) in split_by_partition(&batch, idx)? {
                                            by_shard[shard_of(&key, shards)].push((key, slice));
                                        }
                                    }
                                }
                            }
                            Ok(Decoded {
                                seq,
                                by_shard,
                                stats: local,
                            })
                        });
                    let failed = decoded.is_err();
                    if tx.send(decoded).is_err() || failed {
                        break;
                    }
                }
            }));
        }
        drop(dec_tx);
        drop(chunk_rx);

        // --- writer shards ------------------------------------------------------------
        let mut writer_handles = Vec::with_capacity(shards);
        for rx in shard_rxs.into_iter() {
            let root = req.out_root.clone();
            let prefix = prefix.clone();
            let schema = Arc::clone(&schema);
            let opts = req.writer.clone();
            let budget = Arc::clone(&budget);
            writer_handles.push(scope.spawn(move || -> Result<Vec<PartitionSummary>> {
                let mut w = PartitionedWriter::with_budget(root, prefix, schema, opts, budget)?;
                for group in rx.iter() {
                    for (key, batch) in group {
                        w.write_partition(&key, batch)?;
                    }
                }
                w.finish()
            }));
        }

        // --- reorder and route --------------------------------------------------------
        // Chunks finish out of order; restoring sequence order here is what keeps each
        // symbol's rows in the order NSE wrote them.
        let mut pending: HashMap<u64, Decoded> = HashMap::new();
        let mut next_seq = 0u64;
        let mut stop = false;

        'outer: for message in dec_rx.iter() {
            let decoded = match message {
                Ok(d) => d,
                Err(e) => {
                    route_error = Some(e);
                    break 'outer;
                }
            };
            pending.insert(decoded.seq, decoded);

            while let Some(d) = pending.remove(&next_seq) {
                next_seq += 1;
                stats.merge(d.stats);
                for (idx, group) in d.by_shard.into_iter().enumerate() {
                    if group.is_empty() {
                        continue;
                    }
                    if shard_txs[idx].send(group).is_err() {
                        route_error = Some(anyhow::anyhow!("a writer shard stopped early"));
                        break 'outer;
                    }
                }
                if let Some(limit) = req.max_records {
                    if stats.rows_read >= limit {
                        stop = true;
                        break;
                    }
                }
            }
            if stop {
                break;
            }
        }

        // Closing the shard senders lets the writers finish and close their files.
        drop(shard_txs);
        // Draining lets the decode workers exit instead of blocking on a full channel.
        drop(dec_rx);

        for h in decode_handles {
            h.join().map_err(|_| anyhow::anyhow!("a decode worker panicked"))?;
        }
        for h in writer_handles {
            let part = h
                .join()
                .map_err(|_| anyhow::anyhow!("a writer shard panicked"))??;
            summaries.extend(part);
        }
        bytes_decompressed = reader_handle
            .join()
            .map_err(|_| anyhow::anyhow!("the inflate thread panicked"))??;
        Ok(())
    })?;

    if let Some(e) = route_error {
        return Err(e);
    }

    summaries.sort_by(|a, b| a.key.cmp(&b.key));
    Ok((stats, bytes_decompressed, summaries))
}

/// Read the first `n` decompressed bytes of a file, for inspection commands.
pub fn head_bytes(path: &Path, n: usize) -> Result<Vec<u8>> {
    let mut reader = RecordReader::open(path, 1, n.max(4096))?;
    let chunk = reader.next_chunk()?.unwrap_or_default();
    let mut out = chunk;
    out.truncate(n);
    Ok(out)
}
