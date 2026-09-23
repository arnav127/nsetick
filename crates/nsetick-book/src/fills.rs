//! The trades the replay generates, as an Arrow table.
//!
//! The exchange's trade file records every execution with the order number on each side. The
//! replay produces the same kind of record, so the two can be joined on
//! `(buy_order, sell_order)` to see exactly which trades the replay reproduces. That is the
//! strongest check available on a reconstructed book, and this module exists to make it a
//! one-line call rather than a research project.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::book::Side;
use crate::from_parquet::{discover, read_projected};
use crate::replay::events_by_symbol;
use crate::snapshot::SnapshotBuilder;
use crate::stream::SymbolReplay;

/// Schema of [`replay_fills`]' output. Column names follow the trade file where they can.
pub fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new(
            "txn_time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("trade_price", DataType::Int64, false),
        Field::new("trade_quantity", DataType::Int64, false),
        Field::new("buy_order_number", DataType::UInt64, false),
        Field::new("sell_order_number", DataType::UInt64, false),
        Field::new("aggressor", DataType::Utf8, false),
    ]))
}

/// Replay parsed orders and return every trade the book generates.
///
/// `input` is a parsed-orders directory (the `date=...` directory with `symbol=*` partitions)
/// or a single parquet file; `symbols` restricts it, empty meaning all. `txn_time` is the
/// time of the incoming order's event; the exchange stamps its own trade records one feed
/// tick (about 15 microseconds) per trade later, so join on order numbers, not on time.
pub fn replay_fills(input: &Path, symbols: &[String]) -> Result<RecordBatch> {
    // Snapshots are not wanted here: an interval longer than any session means none is taken.
    let never = i64::MAX / 4;
    let mut builder = SnapshotBuilder::new(1);

    let mut sym = Vec::new();
    let mut ts = Vec::new();
    let mut px = Vec::new();
    let mut qty = Vec::new();
    let mut buy = Vec::new();
    let mut sell = Vec::new();
    let mut aggr = Vec::new();

    for file in discover(input, symbols)? {
        let mut replays: std::collections::HashMap<String, SymbolReplay> = Default::default();
        for batch in read_projected(&file)? {
            for (s, events) in events_by_symbol(&batch)? {
                let r = replays
                    .entry(s.clone())
                    .or_insert_with(|| SymbolReplay::new(s.clone()).record_fills());
                for ev in &events {
                    r.feed(ev, &mut builder, never);
                }
            }
        }
        let mut names: Vec<String> = replays.keys().cloned().collect();
        names.sort();
        for s in names {
            let r = replays.get_mut(&s).expect("key from map");
            r.finish(&mut builder, never);
            for f in r.take_fills() {
                sym.push(s.clone());
                ts.push(f.timestamp);
                px.push(f.price);
                qty.push(f.quantity);
                buy.push(f.buy_order());
                sell.push(f.sell_order());
                aggr.push(if f.aggressor == Side::Buy { "B" } else { "S" });
            }
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(sym)),
        Arc::new(TimestampMicrosecondArray::from(ts)),
        Arc::new(Int64Array::from(px)),
        Arc::new(Int64Array::from(qty)),
        Arc::new(UInt64Array::from(buy)),
        Arc::new(UInt64Array::from(sell)),
        Arc::new(StringArray::from(aggr)),
    ];
    Ok(RecordBatch::try_new(schema(), columns)?)
}
