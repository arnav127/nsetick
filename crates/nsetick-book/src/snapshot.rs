//! Periodic L2 snapshots of a reconstructed book, emitted as Arrow batches.
//!
//! The schema is flat and fixed-width per configured depth, so the output is a plain table
//! that any consumer can read without knowing anything about books: `bid_px_1 .. bid_px_N`
//! and so on. Hidden quantity is reported alongside displayed quantity at each level,
//! because that is the part of NSE's book most analyses are actually after and it cannot be
//! recovered from a public feed.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, Float64Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
    UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::book::{OrderBook, Side};

/// Build the snapshot schema for a given depth.
pub fn schema(levels: usize) -> SchemaRef {
    let mut f: Vec<Field> = vec![
        Field::new("symbol", DataType::Utf8, false),
        // Naive, like every other nsetick timestamp: NSE jiffies are IST wall clock.
        Field::new(
            "snapshot_time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("best_bid", DataType::Int64, true),
        Field::new("best_ask", DataType::Int64, true),
        Field::new("mid_price", DataType::Float64, true),
        Field::new("spread", DataType::Int64, true),
    ];
    for side in ["bid", "ask"] {
        for i in 1..=levels {
            f.push(Field::new(format!("{side}_px_{i}"), DataType::Int64, true));
            f.push(Field::new(format!("{side}_qty_{i}"), DataType::Int64, true));
            f.push(Field::new(
                format!("{side}_hidden_{i}"),
                DataType::Int64,
                true,
            ));
        }
    }
    // Touch composition and per-interval event counts. Neither can be recovered from an L2
    // snapshot downstream: the first needs to know which resting orders are at the touch,
    // the second needs the events between two snapshots.
    for side in ["bid", "ask"] {
        for name in [
            "algo_qty",
            "custodian_qty",
            "iceberg_qty",
            "iceberg_hidden",
            "orders",
        ] {
            let ty = if name == "orders" {
                DataType::UInt32
            } else {
                DataType::Int64
            };
            f.push(Field::new(format!("touch_{side}_{name}"), ty, false));
        }
    }
    f.extend([
        Field::new("interval_entries", DataType::UInt64, false),
        Field::new("interval_cancels", DataType::UInt64, false),
        Field::new("interval_modifies", DataType::UInt64, false),
        Field::new("interval_fills", DataType::UInt64, false),
        Field::new("interval_volume_matched", DataType::Int64, false),
        Field::new("interval_replenishments", DataType::UInt64, false),
    ]);
    f.extend([
        Field::new("total_bid_visible", DataType::Int64, false),
        Field::new("total_ask_visible", DataType::Int64, false),
        Field::new("resting_hidden", DataType::Int64, false),
        Field::new("active_icebergs", DataType::Int64, false),
        Field::new("live_orders", DataType::UInt32, false),
        Field::new("events_applied", DataType::UInt64, false),
        Field::new("is_crossed", DataType::Utf8, false),
    ]);

    // Prices are integer paise, as everywhere else in nsetick; record the scale so a
    // consumer does not have to assume it.
    let scaled: Vec<Field> = f
        .into_iter()
        .map(|field| {
            let is_price = field.name().contains("_px_")
                || matches!(
                    field.name().as_str(),
                    "best_bid" | "best_ask" | "mid_price" | "spread"
                );
            if is_price {
                let mut m = std::collections::HashMap::new();
                m.insert("scale".to_string(), "2".to_string());
                m.insert("units".to_string(), "10^-2".to_string());
                field.with_metadata(m)
            } else {
                field
            }
        })
        .collect();
    Arc::new(Schema::new(scaled))
}

/// Accumulates snapshot rows and turns them into Arrow batches.
pub struct SnapshotBuilder {
    levels: usize,
    schema: SchemaRef,
    symbol: StringBuilder,
    time: TimestampMicrosecondBuilder,
    best_bid: Int64Builder,
    best_ask: Int64Builder,
    mid: Float64Builder,
    spread: Int64Builder,
    /// Flattened per-level builders: bids then asks, each (px, qty, hidden).
    levels_b: Vec<[Int64Builder; 3]>,
    total_bid: Int64Builder,
    total_ask: Int64Builder,
    hidden: Int64Builder,
    icebergs: Int64Builder,
    live: UInt32Builder,
    events: UInt64Builder,
    crossed: StringBuilder,
    /// Touch composition: [bid, ask] x (algo, custodian, iceberg_qty, iceberg_hidden).
    touch: Vec<[Int64Builder; 4]>,
    touch_orders: Vec<UInt32Builder>,
    interval: [UInt64Builder; 4],
    interval_volume: Int64Builder,
    interval_replen: UInt64Builder,
    rows: usize,
}

/// Event counts since the previous snapshot.
#[derive(Debug, Default, Clone, Copy)]
pub struct IntervalCounts {
    pub entries: u64,
    pub cancels: u64,
    pub modifies: u64,
    pub fills: u64,
    pub volume_matched: i64,
    pub replenishments: u64,
}

impl IntervalCounts {
    /// Difference between two cumulative stat readings.
    pub fn between(prev: &crate::book::BookStats, now: &crate::book::BookStats) -> Self {
        Self {
            entries: now.entries.saturating_sub(prev.entries),
            cancels: now.cancels.saturating_sub(prev.cancels),
            modifies: now.modifies.saturating_sub(prev.modifies),
            fills: now.trades_generated.saturating_sub(prev.trades_generated),
            volume_matched: now.volume_matched - prev.volume_matched,
            replenishments: now.replenishments.saturating_sub(prev.replenishments),
        }
    }
}

impl SnapshotBuilder {
    pub fn new(levels: usize) -> Self {
        Self {
            levels,
            schema: schema(levels),
            symbol: StringBuilder::new(),
            time: TimestampMicrosecondBuilder::new(),
            best_bid: Int64Builder::new(),
            best_ask: Int64Builder::new(),
            mid: Float64Builder::new(),
            spread: Int64Builder::new(),
            levels_b: (0..levels * 2)
                .map(|_| {
                    [
                        Int64Builder::new(),
                        Int64Builder::new(),
                        Int64Builder::new(),
                    ]
                })
                .collect(),
            total_bid: Int64Builder::new(),
            total_ask: Int64Builder::new(),
            hidden: Int64Builder::new(),
            icebergs: Int64Builder::new(),
            live: UInt32Builder::new(),
            events: UInt64Builder::new(),
            crossed: StringBuilder::new(),
            touch: (0..2)
                .map(|_| {
                    [
                        Int64Builder::new(),
                        Int64Builder::new(),
                        Int64Builder::new(),
                        Int64Builder::new(),
                    ]
                })
                .collect(),
            touch_orders: (0..2).map(|_| UInt32Builder::new()).collect(),
            interval: [
                UInt64Builder::new(),
                UInt64Builder::new(),
                UInt64Builder::new(),
                UInt64Builder::new(),
            ],
            interval_volume: Int64Builder::new(),
            interval_replen: UInt64Builder::new(),
            rows: 0,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Capture the current state of a book at `micros` (microseconds since the Unix epoch).
    pub fn push(&mut self, book: &OrderBook, micros: i64) {
        self.push_with(book, micros, IntervalCounts::default())
    }

    /// Capture a book along with the event counts accumulated since the previous snapshot.
    pub fn push_with(&mut self, book: &OrderBook, micros: i64, iv: IntervalCounts) {
        self.symbol.append_value(book.symbol());
        self.time.append_value(micros);
        self.best_bid.append_option(book.best_bid());
        self.best_ask.append_option(book.best_ask());
        self.mid.append_option(book.mid_price());
        self.spread.append_option(book.spread());

        for (side_idx, side) in [Side::Buy, Side::Sell].into_iter().enumerate() {
            let top = book.top_levels(side, self.levels);
            for i in 0..self.levels {
                let b = &mut self.levels_b[side_idx * self.levels + i];
                match top.get(i) {
                    Some((px, qty, hid)) => {
                        b[0].append_value(*px);
                        b[1].append_value(*qty);
                        b[2].append_value(*hid);
                    }
                    None => {
                        b[0].append_null();
                        b[1].append_null();
                        b[2].append_null();
                    }
                }
            }
        }

        self.total_bid.append_value(book.total_visible(Side::Buy));
        self.total_ask.append_value(book.total_visible(Side::Sell));
        self.hidden.append_value(book.resting_hidden_volume());
        self.icebergs.append_value(book.active_icebergs());
        self.live.append_value(book.live_orders() as u32);
        self.events.append_value(book.stats().events_applied);
        self.crossed
            .append_value(if book.is_crossed() { "Y" } else { "N" });

        for (i, side) in [Side::Buy, Side::Sell].into_iter().enumerate() {
            let t = book.touch_composition(side);
            self.touch[i][0].append_value(t.algo_visible);
            self.touch[i][1].append_value(t.custodian_visible);
            self.touch[i][2].append_value(t.iceberg_visible);
            self.touch[i][3].append_value(t.iceberg_hidden);
            self.touch_orders[i].append_value(t.orders);
        }
        self.interval[0].append_value(iv.entries);
        self.interval[1].append_value(iv.cancels);
        self.interval[2].append_value(iv.modifies);
        self.interval[3].append_value(iv.fills);
        self.interval_volume.append_value(iv.volume_matched);
        self.interval_replen.append_value(iv.replenishments);
        self.rows += 1;
    }

    /// Drain everything accumulated so far into one batch.
    pub fn finish(&mut self) -> Result<RecordBatch> {
        let mut cols: Vec<ArrayRef> = vec![
            Arc::new(self.symbol.finish()),
            Arc::new(self.time.finish()),
            Arc::new(self.best_bid.finish()),
            Arc::new(self.best_ask.finish()),
            Arc::new(self.mid.finish()),
            Arc::new(self.spread.finish()),
        ];
        for b in self.levels_b.iter_mut() {
            cols.push(Arc::new(b[0].finish()));
            cols.push(Arc::new(b[1].finish()));
            cols.push(Arc::new(b[2].finish()));
        }
        for i in 0..2 {
            cols.push(Arc::new(self.touch[i][0].finish()));
            cols.push(Arc::new(self.touch[i][1].finish()));
            cols.push(Arc::new(self.touch[i][2].finish()));
            cols.push(Arc::new(self.touch[i][3].finish()));
            cols.push(Arc::new(self.touch_orders[i].finish()));
        }
        cols.push(Arc::new(self.interval[0].finish()));
        cols.push(Arc::new(self.interval[1].finish()));
        cols.push(Arc::new(self.interval[2].finish()));
        cols.push(Arc::new(self.interval[3].finish()));
        cols.push(Arc::new(self.interval_volume.finish()));
        cols.push(Arc::new(self.interval_replen.finish()));
        cols.extend([
            Arc::new(self.total_bid.finish()) as ArrayRef,
            Arc::new(self.total_ask.finish()),
            Arc::new(self.hidden.finish()),
            Arc::new(self.icebergs.finish()),
            Arc::new(self.live.finish()),
            Arc::new(self.events.finish()),
            Arc::new(self.crossed.finish()),
        ]);
        self.rows = 0;
        RecordBatch::try_new(Arc::clone(&self.schema), cols).context("assembling snapshot batch")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::{OrderEvent, ENTRY};
    use arrow::array::{Array, Int64Array, StringArray};

    fn limit(id: u64, side: Side, price: i64, qty: i64, disclosed: i64) -> OrderEvent {
        OrderEvent {
            activity_type: ENTRY,
            order_number: id,
            side,
            price,
            volume_disclosed: disclosed,
            volume_original: qty,
            timestamp: id as i64,
            algo_indicator: 1,
            client_identity: 3,
            ioc: false,
            market: false,
            stop_loss: false,
            trigger_price: 0,
        }
    }

    #[test]
    fn schema_width_scales_with_depth() {
        // 6 header + 2 sides * levels * 3 + 2 sides * 5 touch + 6 interval + 7 trailer
        let fixed = 6 + 2 * 5 + 6 + 7;
        assert_eq!(schema(5).fields().len(), fixed + 2 * 5 * 3);
        assert_eq!(schema(1).fields().len(), fixed + 2 * 1 * 3);
    }

    #[test]
    fn price_columns_carry_the_paise_scale() {
        let s = schema(3);
        for name in ["best_bid", "mid_price", "bid_px_1", "ask_px_3"] {
            let f = s.field_with_name(name).unwrap();
            assert_eq!(
                f.metadata().get("scale").map(String::as_str),
                Some("2"),
                "{name} should declare its scale"
            );
        }
        // Quantities are not prices.
        assert!(s
            .field_with_name("bid_qty_1")
            .unwrap()
            .metadata()
            .is_empty());
    }

    #[test]
    fn a_snapshot_records_the_visible_and_hidden_book() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Buy, 100_00, 50, 0));
        b.apply(&limit(2, Side::Buy, 99_00, 30, 0));
        b.apply(&limit(3, Side::Sell, 101_00, 1000, 100)); // iceberg

        let mut sb = SnapshotBuilder::new(2);
        sb.push(&b, 1_700_000_000_000_000);
        assert_eq!(sb.rows(), 1);
        let batch = sb.finish().unwrap();

        let col = |n: &str| -> i64 {
            batch
                .column_by_name(n)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!(col("best_bid"), 100_00);
        assert_eq!(col("best_ask"), 101_00);
        assert_eq!(col("bid_px_1"), 100_00);
        assert_eq!(col("bid_qty_1"), 50);
        assert_eq!(col("bid_px_2"), 99_00);
        assert_eq!(
            col("ask_qty_1"),
            100,
            "only the disclosed tranche is visible"
        );
        assert_eq!(col("ask_hidden_1"), 900, "the rest is reported as hidden");
        assert_eq!(col("resting_hidden"), 900);

        let sym = batch
            .column_by_name("symbol")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(sym.value(0), "TEST");
    }

    #[test]
    fn missing_levels_are_null_not_zero() {
        // Zero would be indistinguishable from a real empty level; null says "no such level".
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Buy, 100_00, 10, 0));
        let mut sb = SnapshotBuilder::new(3);
        sb.push(&b, 0);
        let batch = sb.finish().unwrap();
        let px2 = batch
            .column_by_name("bid_px_2")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(px2.is_null(0));
        let ask1 = batch
            .column_by_name("ask_px_1")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(ask1.is_null(0), "no asks at all");
    }

    #[test]
    fn finish_resets_so_the_builder_can_be_reused() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Buy, 100_00, 10, 0));
        let mut sb = SnapshotBuilder::new(1);
        sb.push(&b, 0);
        sb.push(&b, 1);
        assert_eq!(sb.finish().unwrap().num_rows(), 2);
        assert_eq!(sb.rows(), 0);
        sb.push(&b, 2);
        assert_eq!(sb.finish().unwrap().num_rows(), 1);
    }
}
