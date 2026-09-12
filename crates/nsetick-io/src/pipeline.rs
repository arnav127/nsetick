//! End-to-end parse: gzip in, partitioned Parquet out.
//!
//! Inflate runs on its own thread and hands record-aligned chunks to the decoder over a
//! bounded channel. A deflate stream cannot be decompressed by more than one thread, so the
//! aim is not to parallelise it but to make sure nothing waits on it: decoding, encoding and
//! writing all overlap with the inflate of the next chunk. At 200-450 MB/s inflate against a
//! 58 GB session, everything else has to stay off the critical path.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use nsetick_core::decode::{DecodeOptions, Decoder, Stats};
use nsetick_core::filter;
use nsetick_core::layout::{self, Layout};

use crate::manifest::{Manifest, PartitionRecord};
use crate::reader::{read_trigger, RecordReader, DEFAULT_CHUNK_BYTES};
use crate::writer::{PartitionedWriter, WriterOptions};

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
        }
    }
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
/// This is what makes a wrong layout a startup error instead of 665 million silently
/// misaligned rows. It reads only the first few kilobytes.
pub fn probe_record_length(path: &Path) -> Result<usize> {
    // 1024 is comfortably larger than the longest NSE record (124 bytes).
    let mut reader = RecordReader::open(path, 1, 4096)?;
    let chunk = reader
        .next_chunk()?
        .context("file is empty")?;
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
    let version = layout.resolve(req.session_date, Some(observed))?.clone();

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
    let mut writer = PartitionedWriter::new(
        &req.out_root,
        prefix,
        decoder.schema(),
        req.writer.clone(),
    )?;

    // Inflate on its own thread. A small bound is deliberate: chunks are 8 MB, and letting
    // the reader run far ahead only trades memory for nothing once the consumer keeps up.
    let (tx, rx) = crossbeam_channel::bounded::<Result<Vec<u8>>>(4);
    let input = req.input.clone();
    let line_len = version.line_length();
    let chunk_bytes = req.chunk_bytes;

    let reader_thread = std::thread::Builder::new()
        .name("nsetick-inflate".into())
        .spawn(move || -> Result<u64> {
            let mut reader = RecordReader::open(&input, line_len, chunk_bytes)?;
            loop {
                match reader.next_chunk() {
                    Ok(Some(chunk)) => {
                        if tx.send(Ok(chunk)).is_err() {
                            // Consumer stopped early; stop inflating rather than finishing
                            // 58 GB nobody will read.
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        break;
                    }
                }
            }
            Ok(reader.bytes_read())
        })
        .context("spawning the inflate thread")?;

    let mut stats = Stats::default();
    let mut decode_error = None;

    for message in rx.iter() {
        if let Some(limit) = req.max_records {
            if stats.rows_read >= limit {
                // Dropping the receiver signals the inflate thread to stop.
                break;
            }
        }
        let chunk = message?;
        match decoder.decode(&chunk, &predicate, &mut stats) {
            Ok(batch) => {
                if batch.num_rows() > 0 {
                    writer.write(&batch)?;
                }
            }
            Err(e) => {
                decode_error = Some(e);
                break;
            }
        }
    }

    drop(rx);
    let bytes_decompressed = reader_thread
        .join()
        .map_err(|_| anyhow::anyhow!("the inflate thread panicked"))??;

    if let Some(e) = decode_error {
        return Err(e);
    }

    let partitions = writer.finish()?;
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
        filter: req.filter.clone(),
        selected_fields: decoder
            .projected_names()
            .into_iter()
            .map(str::to_string)
            .collect(),
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
    })
}

/// Read the first `n` decompressed bytes of a file, for inspection commands.
pub fn head_bytes(path: &Path, n: usize) -> Result<Vec<u8>> {
    let mut reader = RecordReader::open(path, 1, n.max(4096))?;
    let chunk = reader.next_chunk()?.unwrap_or_default();
    let mut out = chunk;
    out.truncate(n);
    Ok(out)
}

/// Read the whole of a small file, used by tests and `nsetick inspect`.
pub fn read_to_end(path: &Path, limit: usize) -> Result<Vec<u8>> {
    let mut reader = RecordReader::open(path, 1, 1 << 20)?;
    let mut out = Vec::new();
    while let Some(c) = reader.next_chunk()? {
        out.extend_from_slice(&c);
        if out.len() >= limit {
            out.truncate(limit);
            break;
        }
    }
    Ok(out)
}

/// Present so `Read` stays imported for the trait-object bound in `reader`.
#[allow(dead_code)]
fn _assert_read_in_scope<T: Read>() {}
