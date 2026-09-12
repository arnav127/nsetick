//! Replay books from already-parsed parquet rather than the raw feed.
//!
//! This is the right path whenever stage-1 parquet already exists, and it is not merely a
//! convenience: it is a better shape of problem than replaying from the `.gz`.
//!
//! Replaying the raw file means one serial inflate of the whole session, then demultiplexing
//! events by symbol and restoring chunk order before any book can see them. Symbol-partitioned
//! parquet has already done that work: each symbol's file is independent and its rows are
//! already in time order. So replay becomes one embarrassingly parallel job per file - no
//! inflate bottleneck, no routing, no reorder buffer - and each worker reads only the ten
//! columns a book needs out of the seventeen on disk.
//!
//! Replaying from the raw file still earns its place when no parquet exists, or when only a
//! few symbols are wanted out of an unparsed session, since the filter runs on raw bytes.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use nsetick_io::memory::MemoryGuard;
use nsetick_io::writer::{PartitionedWriter, WriterOptions};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;

use crate::book::OrderBook;
use crate::replay::{events_by_symbol, ReplayReport, REQUIRED_FIELDS};
use crate::snapshot::{IntervalCounts, SnapshotBuilder};

#[derive(Debug, Clone)]
pub struct ParquetReplayRequest {
    /// Directory holding the parsed orders for one session, i.e. the `date=...` directory
    /// containing `symbol=*` partitions, or a single parquet file.
    pub input: PathBuf,
    pub out_root: PathBuf,
    pub session_date: NaiveDate,
    pub interval_secs: f64,
    pub levels: usize,
    pub threads: Option<usize>,
    /// Restrict to these symbols. Empty means every symbol present.
    pub symbols: Vec<String>,
    pub writer: WriterOptions,
}

impl ParquetReplayRequest {
    pub fn new(
        input: impl Into<PathBuf>,
        out_root: impl Into<PathBuf>,
        session_date: NaiveDate,
    ) -> Self {
        Self {
            input: input.into(),
            out_root: out_root.into(),
            session_date,
            interval_secs: 1.0,
            levels: 5,
            threads: None,
            symbols: Vec::new(),
            writer: WriterOptions::default(),
        }
    }
}

/// Find the parquet files to replay, one unit of work each.
fn discover(input: &Path, symbols: &[String]) -> Result<Vec<PathBuf>> {
    if input.is_file() {
        return Ok(vec![input.to_path_buf()]);
    }
    if !input.is_dir() {
        bail!("{} is neither a parquet file nor a directory", input.display());
    }

    let mut files = Vec::new();
    for entry in std::fs::read_dir(input)
        .with_context(|| format!("listing {}", input.display()))?
    {
        let path = entry?.path();
        if path.is_dir() {
            let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
            if let Some(sym) = name.strip_prefix("symbol=") {
                if !symbols.is_empty() && !symbols.iter().any(|s| s == sym) {
                    continue;
                }
                for f in std::fs::read_dir(&path)? {
                    let f = f?.path();
                    if f.extension().map(|e| e == "parquet").unwrap_or(false) {
                        files.push(f);
                    }
                }
            }
        } else if path.extension().map(|e| e == "parquet").unwrap_or(false) {
            files.push(path);
        }
    }
    files.sort();
    if files.is_empty() {
        bail!(
            "no parquet files found under {}. Expected symbol=* partitions or a parquet file.",
            input.display()
        );
    }
    Ok(files)
}

/// Read one parquet file, projecting only the columns a book needs.
fn read_projected(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("reading {}", path.display()))?;

    // Project by name so a file with extra columns, or columns in another order, still works.
    let schema = builder.parquet_schema();
    let mut indices = Vec::new();
    for (i, col) in schema.columns().iter().enumerate() {
        if REQUIRED_FIELDS.contains(&col.name()) {
            indices.push(i);
        }
    }
    if indices.len() != REQUIRED_FIELDS.len() {
        let present: Vec<&str> = schema.columns().iter().map(|c| c.name()).collect();
        let missing: Vec<&&str> = REQUIRED_FIELDS
            .iter()
            .filter(|f| !present.contains(&f.to_string().as_str()))
            .collect();
        bail!(
            "{} is missing columns a book replay needs: {:?}. It was probably written with a \
             narrower --select than the replay requires.",
            path.display(),
            missing
        );
    }
    let mask = ProjectionMask::roots(schema, indices);

    let reader = builder
        .with_projection(mask)
        .with_batch_size(64 * 1024)
        .build()
        .with_context(|| format!("building reader for {}", path.display()))?;

    let mut out = Vec::new();
    for b in reader {
        out.push(b.with_context(|| format!("decoding a batch of {}", path.display()))?);
    }
    Ok(out)
}

/// Replay one file's worth of events into books and snapshots.
fn replay_file(
    path: &Path,
    interval_micros: i64,
    levels: usize,
    writer: &Mutex<PartitionedWriter>,
) -> Result<(u64, u64, u64, u64, usize, usize)> {
    let batches = read_projected(path)?;
    let mut builder = SnapshotBuilder::new(levels);

    // A single partition file holds one symbol, but a flat file may hold many, so key by
    // symbol rather than assuming.
    let mut books: std::collections::HashMap<String, (OrderBook, i64, crate::book::BookStats)> =
        std::collections::HashMap::new();

    let mut events = 0u64;
    let mut fills = 0u64;
    let mut snaps = 0u64;
    let mut replen = 0u64;
    let mut crossed = 0usize;

    for batch in &batches {
        for (sym, evs) in events_by_symbol(batch)? {
            let entry = books.entry(sym.clone()).or_insert_with(|| {
                (OrderBook::new(sym.clone()), i64::MIN, Default::default())
            });
            for ev in &evs {
                if entry.1 == i64::MIN {
                    entry.1 = ev.timestamp + interval_micros;
                }
                while ev.timestamp >= entry.1 {
                    let now = entry.0.stats();
                    builder.push_with(
                        &entry.0,
                        entry.1,
                        IntervalCounts::between(&entry.2, &now),
                    );
                    entry.2 = now;
                    entry.1 += interval_micros;
                    snaps += 1;
                }
                fills += entry.0.apply(ev) as u64;
                events += 1;
            }
        }
        if builder.rows() >= 8192 {
            let b = builder.finish()?;
            writer.lock().expect("writer lock").write(&b)?;
        }
    }

    if builder.rows() > 0 {
        let b = builder.finish()?;
        writer.lock().expect("writer lock").write(&b)?;
    }

    for (_, (book, _, _)) in books.iter() {
        replen += book.stats().replenishments;
        if book.is_crossed() {
            crossed += 1;
        }
    }
    Ok((events, fills, snaps, replen, books.len(), crossed))
}

pub fn run(req: &ParquetReplayRequest) -> Result<ReplayReport> {
    let started = Instant::now();
    if req.levels == 0 {
        bail!("levels must be at least 1");
    }
    if !(req.interval_secs > 0.0) {
        bail!("interval_secs must be positive, got {}", req.interval_secs);
    }

    let files = discover(&req.input, &req.symbols)?;
    let threads = req
        .threads
        .unwrap_or_else(nsetick_io::pipeline::default_threads)
        .max(1)
        .min(files.len());
    let interval_micros = (req.interval_secs * 1_000_000.0).round() as i64;

    let guard = MemoryGuard::new(
        nsetick_io::memory::default_limits(256 * 1024 * 1024).0,
        256 * 1024 * 1024,
    );
    let prefix = vec![
        ("segment".to_string(), "cm".to_string()),
        ("kind".to_string(), "book_snapshots".to_string()),
        ("date".to_string(), req.session_date.to_string()),
    ];
    let writer = Mutex::new(PartitionedWriter::with_budget(
        &req.out_root,
        prefix,
        SnapshotBuilder::new(req.levels).schema(),
        req.writer.clone(),
        guard,
    )?);

    let next = AtomicUsize::new(0);
    let totals = Mutex::new((0u64, 0u64, 0u64, 0u64, 0usize, 0usize));
    let first_error: Mutex<Option<anyhow::Error>> = Mutex::new(None);

    // One file per unit of work. Each symbol's rows are already in time order inside its own
    // file, so workers need no coordination beyond the shared writer.
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= files.len() || first_error.lock().expect("err lock").is_some() {
                    break;
                }
                match replay_file(&files[i], interval_micros, req.levels, &writer) {
                    Ok((e, f, s, r, syms, cr)) => {
                        let mut t = totals.lock().expect("totals lock");
                        t.0 += e;
                        t.1 += f;
                        t.2 += s;
                        t.3 += r;
                        t.4 += syms;
                        t.5 += cr;
                    }
                    Err(err) => {
                        *first_error.lock().expect("err lock") = Some(err);
                        break;
                    }
                }
            });
        }
    });

    if let Some(e) = first_error.into_inner().expect("err lock") {
        return Err(e);
    }

    writer.into_inner().expect("writer lock").finish()?;
    let t = totals.into_inner().expect("totals lock");

    let mut report = ReplayReport::default();
    report.events_applied = t.0;
    report.fills_generated = t.1;
    report.snapshots = t.2;
    report.replenishments = t.3;
    report.symbols = t.4;
    report.crossed_symbols = t.5;
    report.stats.rows_read = t.0;
    report.stats.rows_emitted = t.0;
    report.elapsed_secs = started.elapsed().as_secs_f64();
    report.threads = threads;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_rejects_a_path_with_no_parquet() {
        let dir = std::env::temp_dir().join("nsetick_book_empty");
        std::fs::create_dir_all(&dir).unwrap();
        let err = discover(&dir, &[]).unwrap_err().to_string();
        assert!(err.contains("no parquet files"), "{err}");
    }

    #[test]
    fn discovery_filters_by_symbol() {
        let dir = std::env::temp_dir().join("nsetick_book_disc");
        let _ = std::fs::remove_dir_all(&dir);
        for sym in ["AAA", "BBB"] {
            let d = dir.join(format!("symbol={sym}"));
            std::fs::create_dir_all(&d).unwrap();
            std::fs::write(d.join("part-000.parquet"), b"x").unwrap();
        }
        assert_eq!(discover(&dir, &[]).unwrap().len(), 2);
        let only = discover(&dir, &["AAA".to_string()]).unwrap();
        assert_eq!(only.len(), 1);
        assert!(only[0].to_string_lossy().contains("AAA"));
    }

    #[test]
    fn a_missing_directory_is_reported_clearly() {
        let err = discover(Path::new("does/not/exist"), &[]).unwrap_err().to_string();
        assert!(err.contains("neither a parquet file nor a directory"), "{err}");
    }
}
