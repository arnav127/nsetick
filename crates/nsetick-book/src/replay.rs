//! Replay a whole session into per-symbol books, in parallel, straight from the raw file.
//!
//! Reconstructing books is where the parallelism in this data really pays: every symbol's
//! book is completely independent of every other, so once events are demultiplexed by symbol
//! the work fans out with no coordination at all. The previous approach - parse the session
//! to parquet, then replay one symbol at a time in Python - paid for a full round trip
//! through storage and then left most of the machine idle.
//!
//! The shape is the same as the parse pipeline: one inflate thread, decode workers, a
//! reorder step, then shard workers. Ordering matters more here than anywhere else, because
//! a book is a fold over its event stream: applying events out of order does not merely
//! reorder the output, it produces a different book. Chunks therefore carry sequence numbers
//! and are restored to order before any book sees them.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use arrow::array::{Array, Int64Array, StringArray, TimestampMicrosecondArray, UInt64Array, UInt8Array};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use nsetick_core::decode::{DecodeOptions, Decoder, Stats};
use nsetick_core::filter;
use nsetick_core::layout;
use nsetick_io::memory::MemoryGuard;
use nsetick_io::pipeline;
use nsetick_io::reader::RecordReader;
use nsetick_io::writer::{PartitionedWriter, WriterOptions};

use crate::book::{OrderBook, OrderEvent, Side};
use crate::snapshot::SnapshotBuilder;

/// Columns the replay needs from the orders feed. Projecting to just these is a large part of
/// why replaying from the raw file beats reading a full parquet.
pub const REQUIRED_FIELDS: &[&str] = &[
    "symbol",
    "txn_time",
    "order_number",
    "activity_type",
    "buy_sell",
    "limit_price",
    "volume_disclosed",
    "volume_original",
    "algo_indicator",
    "client_identity",
];

#[derive(Debug, Clone)]
pub struct ReplayRequest {
    pub input: PathBuf,
    pub out_root: PathBuf,
    pub session_date: NaiveDate,
    /// Filter expression applied before decoding, e.g. a series or symbol restriction.
    pub filter: String,
    /// Seconds between snapshots. Fractional values are allowed.
    pub interval_secs: f64,
    /// Depth captured per side.
    pub levels: usize,
    pub threads: Option<usize>,
    pub chunk_bytes: usize,
    pub max_records: Option<u64>,
    pub writer: WriterOptions,
}

impl ReplayRequest {
    pub fn new(
        input: impl Into<PathBuf>,
        out_root: impl Into<PathBuf>,
        session_date: NaiveDate,
    ) -> Self {
        Self {
            input: input.into(),
            out_root: out_root.into(),
            session_date,
            filter: "series == 'EQ'".to_string(),
            interval_secs: 1.0,
            levels: 5,
            threads: None,
            chunk_bytes: 8 * 1024 * 1024,
            max_records: None,
            writer: WriterOptions::default(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReplayReport {
    pub stats: Stats,
    pub bytes_decompressed: u64,
    pub symbols: usize,
    pub snapshots: u64,
    pub events_applied: u64,
    pub fills_generated: u64,
    pub replenishments: u64,
    pub crossed_symbols: usize,
    pub elapsed_secs: f64,
    pub threads: usize,
}

impl ReplayReport {
    pub fn events_per_sec(&self) -> f64 {
        if self.elapsed_secs <= 0.0 {
            0.0
        } else {
            self.events_applied as f64 / self.elapsed_secs
        }
    }
}

fn shard_of(key: &str, shards: usize) -> usize {
    if shards <= 1 {
        return 0;
    }
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    (h.finish() % shards as u64) as usize
}

/// One symbol's book plus the snapshot clock that drives its output.
struct SymbolState {
    book: OrderBook,
    /// Next snapshot boundary, in microseconds since the Unix epoch.
    next_snapshot: i64,
    /// Cumulative stats as of the previous snapshot, for per-interval deltas.
    last_stats: crate::book::BookStats,
}

/// Decoded events for one chunk, grouped by destination shard.
struct Decoded {
    seq: u64,
    by_shard: Vec<Vec<(String, Vec<OrderEvent>)>>,
    stats: Stats,
}

/// Pull the typed columns out of a decoded batch and group events by symbol.
pub(crate) fn events_by_symbol(batch: &RecordBatch) -> Result<Vec<(String, Vec<OrderEvent>)>> {
    macro_rules! col {
        ($name:literal, $ty:ty) => {
            batch
                .column_by_name($name)
                .with_context(|| format!("replay needs column {}", $name))?
                .as_any()
                .downcast_ref::<$ty>()
                .with_context(|| format!("column {} has an unexpected type", $name))?
        };
    }

    let symbol = col!("symbol", StringArray);
    let time = col!("txn_time", TimestampMicrosecondArray);
    let order_no = col!("order_number", UInt64Array);
    let activity = col!("activity_type", UInt8Array);
    let side = col!("buy_sell", StringArray);
    let price = col!("limit_price", Int64Array);
    let disclosed = col!("volume_disclosed", UInt64Array);
    let original = col!("volume_original", UInt64Array);
    let algo = col!("algo_indicator", UInt8Array);
    let client = col!("client_identity", UInt8Array);

    let mut out: Vec<(String, Vec<OrderEvent>)> = Vec::new();
    let mut index: HashMap<&str, usize> = HashMap::new();

    for i in 0..batch.num_rows() {
        // A row missing any of these cannot be applied to a book at all.
        if symbol.is_null(i) || time.is_null(i) || order_no.is_null(i) || activity.is_null(i) {
            continue;
        }
        let Some(s) = Side::from_byte(side.value(i).as_bytes().first().copied().unwrap_or(0)) else {
            continue;
        };

        let ev = OrderEvent {
            activity_type: activity.value(i),
            order_number: order_no.value(i),
            side: s,
            price: if price.is_null(i) { 0 } else { price.value(i) },
            volume_disclosed: if disclosed.is_null(i) { 0 } else { disclosed.value(i) as i64 },
            volume_original: if original.is_null(i) { 0 } else { original.value(i) as i64 },
            timestamp: time.value(i),
            algo_indicator: if algo.is_null(i) { 255 } else { algo.value(i) },
            client_identity: if client.is_null(i) { 255 } else { client.value(i) },
        };

        let sym = symbol.value(i);
        match index.get(sym) {
            Some(&slot) => out[slot].1.push(ev),
            None => {
                index.insert(sym, out.len());
                out.push((sym.to_string(), vec![ev]));
            }
        }
    }
    Ok(out)
}

pub fn run(req: &ReplayRequest) -> Result<ReplayReport> {
    let started = Instant::now();

    if req.levels == 0 {
        bail!("levels must be at least 1");
    }
    if !(req.interval_secs > 0.0) {
        bail!("interval_secs must be positive, got {}", req.interval_secs);
    }

    let lay = layout::load("cm_orders")?;
    let observed = pipeline::probe_record_length(&req.input)
        .with_context(|| format!("probing {}", req.input.display()))?;
    let version = lay.resolve(req.session_date, Some(observed))?.clone();

    let select: Vec<String> = REQUIRED_FIELDS.iter().map(|s| s.to_string()).collect();
    let decoder = Arc::new(Decoder::new(
        &version,
        Some(&select),
        DecodeOptions { strict: true },
    )?);
    let predicate = Arc::new(filter::compile(
        &req.filter,
        &version,
        Some(req.session_date),
    )?);

    let threads = req
        .threads
        .unwrap_or_else(pipeline::default_threads)
        .max(1);
    let shards = threads;
    let interval_micros = (req.interval_secs * 1_000_000.0).round() as i64;
    let line_len = version.line_length();

    let guard = MemoryGuard::new(
        nsetick_io::memory::default_limits(req.chunk_bytes * (threads * 4 + 8)).0,
        256 * 1024 * 1024,
    );

    let (chunk_tx, chunk_rx) = crossbeam_channel::bounded::<(u64, Vec<u8>)>(threads * 2);
    let (dec_tx, dec_rx) = crossbeam_channel::bounded::<Result<Decoded>>(threads * 2);

    type ShardMsg = Vec<(String, Vec<OrderEvent>)>;
    let mut shard_txs = Vec::with_capacity(shards);
    let mut shard_rxs = Vec::with_capacity(shards);
    for _ in 0..shards {
        let (t, r) = crossbeam_channel::bounded::<ShardMsg>(8);
        shard_txs.push(t);
        shard_rxs.push(r);
    }

    let mut stats = Stats::default();
    let mut bytes = 0u64;
    let mut report = ReplayReport::default();
    let mut route_error: Option<anyhow::Error> = None;

    std::thread::scope(|scope| -> Result<()> {
        // --- inflate --------------------------------------------------------------------
        let input = req.input.clone();
        let chunk_bytes = req.chunk_bytes;
        let reader = scope.spawn(move || -> Result<u64> {
            let mut r = RecordReader::open(&input, line_len, chunk_bytes)?;
            let mut seq = 0u64;
            loop {
                match r.next_chunk() {
                    Ok(Some(c)) => {
                        if chunk_tx.send((seq, c)).is_err() {
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
            Ok(r.bytes_read())
        });

        // --- decode and group by symbol -------------------------------------------------
        let mut decoders = Vec::with_capacity(threads);
        for _ in 0..threads {
            let rx = chunk_rx.clone();
            let tx = dec_tx.clone();
            let dec = Arc::clone(&decoder);
            let pred = Arc::clone(&predicate);
            decoders.push(scope.spawn(move || {
                for (seq, chunk) in rx.iter() {
                    let mut local = Stats::default();
                    let res = dec.decode(&chunk, &pred, &mut local).and_then(|batch| {
                        let mut by_shard: Vec<Vec<(String, Vec<OrderEvent>)>> =
                            vec![Vec::new(); shards];
                        if batch.num_rows() > 0 {
                            for (sym, evs) in events_by_symbol(&batch)? {
                                by_shard[shard_of(&sym, shards)].push((sym, evs));
                            }
                        }
                        Ok(Decoded {
                            seq,
                            by_shard,
                            stats: local,
                        })
                    });
                    let failed = res.is_err();
                    if tx.send(res).is_err() || failed {
                        break;
                    }
                }
            }));
            drop(());
        }
        drop(dec_tx);
        drop(chunk_rx);

        // --- shard workers: own the books, emit snapshots --------------------------------
        let mut workers = Vec::with_capacity(shards);
        for rx in shard_rxs.into_iter() {
            let root = req.out_root.clone();
            let date = req.session_date;
            let opts = req.writer.clone();
            let levels = req.levels;
            let guard = Arc::clone(&guard);
            workers.push(scope.spawn(move || -> Result<ShardOutcome> {
                let prefix = vec![
                    ("segment".to_string(), "cm".to_string()),
                    ("kind".to_string(), "book_snapshots".to_string()),
                    ("date".to_string(), date.to_string()),
                ];
                let sb = SnapshotBuilder::new(levels);
                let mut writer =
                    PartitionedWriter::with_budget(root, prefix, sb.schema(), opts, guard)?;
                let mut builder = sb;
                let mut books: HashMap<String, SymbolState> = HashMap::new();
                let mut outcome = ShardOutcome::default();

                for group in rx.iter() {
                    for (sym, events) in group {
                        let st = books.entry(sym.clone()).or_insert_with(|| SymbolState {
                            book: OrderBook::new(sym.clone()),
                            // Align the first boundary to the first event seen.
                            next_snapshot: i64::MIN,
                            last_stats: Default::default(),
                        });
                        for ev in &events {
                            if st.next_snapshot == i64::MIN {
                                st.next_snapshot = ev.timestamp + interval_micros;
                            }
                            // Snapshot the state *before* applying an event that crosses the
                            // boundary, so a snapshot reflects the book as of that instant.
                            while ev.timestamp >= st.next_snapshot {
                                let now = st.book.stats();
                                builder.push_with(
                                    &st.book,
                                    st.next_snapshot,
                                    crate::snapshot::IntervalCounts::between(&st.last_stats, &now),
                                );
                                st.last_stats = now;
                                st.next_snapshot += interval_micros;
                                outcome.snapshots += 1;
                            }
                            outcome.fills += st.book.apply(ev) as u64;
                        }
                        if builder.rows() >= 8192 {
                            let batch = builder.finish()?;
                            writer.write_partition(&sym, batch)?;
                        }
                    }
                }

                // Flush the tail. Rows for several symbols can be mixed in the builder here,
                // so let the writer split them rather than assuming one partition.
                if builder.rows() > 0 {
                    let batch = builder.finish()?;
                    writer.write(&batch)?;
                }

                for (sym, st) in books.iter() {
                    let s = st.book.stats();
                    outcome.events += s.events_applied;
                    outcome.replenishments += s.replenishments;
                    if st.book.is_crossed() {
                        outcome.crossed.push(sym.clone());
                    }
                }
                outcome.symbols = books.len();
                writer.finish()?;
                Ok(outcome)
            }));
        }

        // --- reorder and route ----------------------------------------------------------
        // A book is a fold over its events, so order is correctness here, not cosmetics.
        let mut pending: HashMap<u64, Decoded> = HashMap::new();
        let mut next_seq = 0u64;
        let mut stop = false;

        'outer: for msg in dec_rx.iter() {
            let d = match msg {
                Ok(d) => d,
                Err(e) => {
                    route_error = Some(e);
                    break 'outer;
                }
            };
            pending.insert(d.seq, d);
            while let Some(d) = pending.remove(&next_seq) {
                next_seq += 1;
                stats.merge(d.stats);
                for (idx, group) in d.by_shard.into_iter().enumerate() {
                    if group.is_empty() {
                        continue;
                    }
                    if shard_txs[idx].send(group).is_err() {
                        route_error = Some(anyhow::anyhow!("a replay shard stopped early"));
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

        drop(shard_txs);
        drop(dec_rx);

        for h in decoders {
            h.join().map_err(|_| anyhow::anyhow!("a decode worker panicked"))?;
        }
        for h in workers {
            let o = h
                .join()
                .map_err(|_| anyhow::anyhow!("a replay shard panicked"))??;
            report.symbols += o.symbols;
            report.snapshots += o.snapshots;
            report.events_applied += o.events;
            report.fills_generated += o.fills;
            report.replenishments += o.replenishments;
            report.crossed_symbols += o.crossed.len();
        }
        bytes = reader
            .join()
            .map_err(|_| anyhow::anyhow!("the inflate thread panicked"))??;
        Ok(())
    })?;

    if let Some(e) = route_error {
        return Err(e);
    }

    report.stats = stats;
    report.bytes_decompressed = bytes;
    report.elapsed_secs = started.elapsed().as_secs_f64();
    report.threads = threads;
    Ok(report)
}

#[derive(Default)]
struct ShardOutcome {
    symbols: usize,
    snapshots: u64,
    events: u64,
    fills: u64,
    replenishments: u64,
    crossed: Vec<String>,
}
