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
use std::sync::Mutex;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use nsetick_io::memory::MemoryGuard;
use nsetick_io::writer::{PartitionedWriter, WriterOptions};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;

use crate::replay::{events_by_symbol, ReplayReport, OPTIONAL_FIELDS, REQUIRED_FIELDS};
use crate::snapshot::SnapshotBuilder;
use crate::stream::{Progress, SymbolReplay};

#[derive(Clone)]
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
    /// Previous session's closing price per symbol, in paise. Only used to break a tie between
    /// equally good pre-open auction prices; see [`crate::book::OrderBook::set_previous_close`].
    pub previous_close: std::collections::HashMap<String, i64>,
    /// Polled between files so a long replay can be interrupted. See `ParseRequest`.
    #[allow(clippy::type_complexity)]
    pub interrupt: Option<std::sync::Arc<dyn Fn() -> Result<()> + Send + Sync>>,
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
            previous_close: Default::default(),
            interrupt: None,
        }
    }
}

/// Find the parquet files to replay, one unit of work each.
pub(crate) fn discover(input: &Path, symbols: &[String]) -> Result<Vec<PathBuf>> {
    if input.is_file() {
        return Ok(vec![input.to_path_buf()]);
    }
    if !input.is_dir() {
        bail!(
            "{} is neither a parquet file nor a directory",
            input.display()
        );
    }

    let mut files = Vec::new();
    for entry in std::fs::read_dir(input).with_context(|| format!("listing {}", input.display()))? {
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
pub(crate) fn read_projected(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("reading {}", path.display()))?;

    // Project by name so a file with extra columns, or columns in another order, still works.
    let schema = builder.parquet_schema();
    let mut indices = Vec::new();
    let mut required_found = 0;
    for (i, col) in schema.columns().iter().enumerate() {
        if REQUIRED_FIELDS.contains(&col.name()) {
            indices.push(i);
            required_found += 1;
        } else if OPTIONAL_FIELDS.contains(&col.name()) {
            indices.push(i);
        }
    }
    if required_found != REQUIRED_FIELDS.len() {
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
    previous_close: &std::collections::HashMap<String, i64>,
) -> Result<(u64, u64, u64, u64, usize, usize)> {
    let batches = read_projected(path)?;
    let mut builder = SnapshotBuilder::new(levels);

    // A single partition file holds one symbol, but a flat file may hold many, so key by
    // symbol rather than assuming.
    let mut books: std::collections::HashMap<String, SymbolReplay> =
        std::collections::HashMap::new();
    let mut progress = Progress::default();

    for batch in &batches {
        for (sym, evs) in events_by_symbol(batch)? {
            let replay = books.entry(sym.clone()).or_insert_with(|| {
                SymbolReplay::new(sym.clone())
                    .with_previous_close(previous_close.get(&sym).copied())
            });
            for ev in &evs {
                progress += replay.feed(ev, &mut builder, interval_micros);
            }
        }
        if builder.rows() >= 8192 {
            let b = builder.finish()?;
            writer.lock().expect("writer lock").write(&b)?;
        }
    }
    for replay in books.values_mut() {
        progress += replay.finish(&mut builder, interval_micros);
    }

    if builder.rows() > 0 {
        let b = builder.finish()?;
        writer.lock().expect("writer lock").write(&b)?;
    }

    let mut replen = 0u64;
    let mut crossed = 0usize;
    for replay in books.values() {
        replen += replay.book.stats().replenishments;
        if replay.book.is_crossed() {
            crossed += 1;
        }
    }
    Ok((
        progress.events,
        progress.fills,
        progress.snapshots,
        replen,
        books.len(),
        crossed,
    ))
}

pub fn run(req: &ParquetReplayRequest) -> Result<ReplayReport> {
    let started = Instant::now();
    if req.levels == 0 {
        bail!("levels must be at least 1");
    }
    // Written to reject NaN as well as zero and negative values.
    if req.interval_secs.is_nan() || req.interval_secs <= 0.0 {
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
                // One file per symbol, so this is a natural checkpoint: nothing is
                // half-written and the shared writer is consistent.
                if let Some(check) = &req.interrupt {
                    if let Err(e) = check() {
                        *first_error.lock().expect("err lock") = Some(e);
                        break;
                    }
                }
                match replay_file(
                    &files[i],
                    interval_micros,
                    req.levels,
                    &writer,
                    &req.previous_close,
                ) {
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

    let mut report = ReplayReport {
        events_applied: t.0,
        fills_generated: t.1,
        snapshots: t.2,
        replenishments: t.3,
        symbols: t.4,
        crossed_symbols: t.5,
        ..Default::default()
    };
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
        let err = discover(Path::new("does/not/exist"), &[])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("neither a parquet file nor a directory"),
            "{err}"
        );
    }
}
