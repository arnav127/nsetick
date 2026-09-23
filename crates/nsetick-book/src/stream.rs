//! Drive one symbol's book from its event stream, emitting periodic snapshots.
//!
//! Both replay paths - from the raw feed and from parsed parquet - do the same thing per
//! symbol: advance the snapshot clock to each event, capture the book as of each boundary it
//! crosses, then apply the event. This holds that loop in one place, together with the short
//! lookahead the book needs to recognise self-trade prevention: each event is announced to
//! the book as soon as it is read but applied only once the stream has moved
//! [`SELF_TRADE_WINDOW_MICROS`] past it, so a cancel the exchange issued a few hundred
//! microseconds after a match is already known when that match is considered.

use std::collections::VecDeque;

use crate::book::{BookStats, Fill, OrderBook, OrderEvent, SELF_TRADE_WINDOW_MICROS};
use crate::snapshot::{IntervalCounts, SnapshotBuilder};

/// Totals from driving a book, summed by the caller across symbols.
#[derive(Debug, Default, Clone, Copy)]
pub struct Progress {
    pub events: u64,
    pub fills: u64,
    pub snapshots: u64,
}

impl std::ops::AddAssign for Progress {
    fn add_assign(&mut self, o: Self) {
        self.events += o.events;
        self.fills += o.fills;
        self.snapshots += o.snapshots;
    }
}

/// The first snapshot time after `ts`: the next whole multiple of `interval` counted from
/// midnight, so snapshots fall on the clock (10:00:00, 10:01:00, ...) whatever time the first
/// event arrives. Timestamps are IST wall clock, so midnight is a multiple of a day.
fn first_boundary(ts: i64, interval: i64) -> i64 {
    const DAY: i64 = 86_400 * 1_000_000;
    let midnight = ts - ts.rem_euclid(DAY);
    midnight + ((ts - midnight) / interval + 1) * interval
}

pub struct SymbolReplay {
    pub book: OrderBook,
    /// Next snapshot boundary, in the feed's microseconds; `None` until the first event.
    next_snapshot: Option<i64>,
    /// Cumulative stats as of the previous snapshot, for per-interval deltas.
    last_stats: BookStats,
    /// Events read and announced but not yet applied.
    pending: VecDeque<OrderEvent>,
    lookahead_micros: i64,
    /// Every fill the book generates, when recording is on.
    recorded: Option<Vec<Fill>>,
}

impl SymbolReplay {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self::with_lookahead(symbol, SELF_TRADE_WINDOW_MICROS)
    }

    /// A lookahead of zero applies every event as it is read, which disables self-trade
    /// prevention and reproduces the book of earlier releases.
    pub fn with_lookahead(symbol: impl Into<String>, lookahead_micros: i64) -> Self {
        Self {
            book: OrderBook::new(symbol),
            next_snapshot: None,
            last_stats: BookStats::default(),
            pending: VecDeque::new(),
            lookahead_micros: lookahead_micros.max(0),
            recorded: None,
        }
    }

    /// Keep every fill the replay generates, for [`SymbolReplay::take_fills`].
    /// Previous session's close for this symbol, to break a tie between auction prices.
    pub fn with_previous_close(mut self, price: Option<i64>) -> Self {
        self.book.set_previous_close(price);
        self
    }

    pub fn record_fills(mut self) -> Self {
        self.recorded = Some(Vec::new());
        self
    }

    /// The fills recorded since the last call.
    pub fn take_fills(&mut self) -> Vec<Fill> {
        self.recorded
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default()
    }

    /// Read one event. Applies every buffered event the stream has now moved far enough past.
    pub fn feed(
        &mut self,
        ev: &OrderEvent,
        builder: &mut SnapshotBuilder,
        interval: i64,
    ) -> Progress {
        self.book.announce(ev);
        self.pending.push_back(*ev);
        let mut p = Progress::default();
        while let Some(front) = self.pending.front() {
            if front.timestamp + self.lookahead_micros >= ev.timestamp && self.lookahead_micros > 0
            {
                break;
            }
            let next = self.pending.pop_front().expect("front exists");
            p += self.apply(&next, builder, interval);
        }
        p
    }

    /// The stream has ended: apply everything still buffered.
    pub fn finish(&mut self, builder: &mut SnapshotBuilder, interval: i64) -> Progress {
        let mut p = Progress::default();
        while let Some(next) = self.pending.pop_front() {
            p += self.apply(&next, builder, interval);
        }
        p
    }

    fn apply(&mut self, ev: &OrderEvent, builder: &mut SnapshotBuilder, interval: i64) -> Progress {
        let mut p = Progress {
            events: 1,
            ..Default::default()
        };
        let next = self
            .next_snapshot
            .get_or_insert_with(|| first_boundary(ev.timestamp, interval));
        // Snapshot the state *before* applying an event that crosses the boundary, so a
        // snapshot reflects the book as of that instant.
        while ev.timestamp >= *next {
            let now = self.book.stats();
            builder.push_with(
                &self.book,
                *next,
                IntervalCounts::between(&self.last_stats, &now),
            );
            self.last_stats = now;
            *next += interval;
            p.snapshots += 1;
        }
        p.fills = self.book.apply(ev) as u64;
        if let Some(sink) = self.recorded.as_mut() {
            sink.extend_from_slice(self.book.last_fills());
        }
        p
    }
}

#[cfg(test)]
mod tests {
    /// 10:00 in the continuous session; earlier times belong to the pre-open auction.
    const T0: i64 = 10 * 3600 * 1_000_000;

    use super::*;
    use crate::book::{Side, CANCEL, ENTRY};

    fn event(activity: u8, id: u64, side: Side, price: i64, qty: i64, ts: i64) -> OrderEvent {
        OrderEvent {
            activity_type: activity,
            order_number: id,
            side,
            price,
            volume_disclosed: 0,
            volume_original: qty,
            timestamp: T0 + ts,
            algo_indicator: 1,
            client_identity: 3,
            ioc: false,
            market: false,
            stop_loss: false,
            trigger_price: 0,
        }
    }

    #[test]
    fn lookahead_lets_a_later_cancel_prevent_a_self_trade() {
        let mut r = SymbolReplay::new("TEST");
        let mut sb = SnapshotBuilder::new(1);
        let events = [
            event(ENTRY, 1, Side::Buy, 100_00, 10, 1_000),
            event(ENTRY, 2, Side::Sell, 100_00, 10, 5_000),
            event(CANCEL, 1, Side::Buy, 0, 0, 5_015),
        ];
        let mut p = Progress::default();
        for e in &events {
            p += r.feed(e, &mut sb, 1_000_000);
        }
        p += r.finish(&mut sb, 1_000_000);
        assert_eq!(
            p.events, 3,
            "every event is applied once, including the buffered tail"
        );
        assert_eq!(
            p.fills, 0,
            "the buy was cancelled by the exchange as the sell arrived"
        );
        assert_eq!(r.book.stats().self_trade_preventions, 1);
        assert_eq!(r.book.best_ask(), Some(100_00), "the sell rests");
    }

    #[test]
    fn snapshots_fall_on_whole_multiples_of_the_interval() {
        assert_eq!(first_boundary(T0 + 154_647, 60_000_000), T0 + 60_000_000);
        assert_eq!(
            first_boundary(T0 + 60_000_000, 60_000_000),
            T0 + 120_000_000
        );
        assert_eq!(first_boundary(T0 + 1, 1_000_000), T0 + 1_000_000);
        assert_eq!(first_boundary(T0 + 1, 250_000), T0 + 250_000);

        let mut r = SymbolReplay::new("TEST");
        let mut sb = SnapshotBuilder::new(1);
        let mut p = Progress::default();
        p += r.feed(
            &event(ENTRY, 1, Side::Buy, 100_00, 10, 1_234_567),
            &mut sb,
            1_000_000,
        );
        p += r.feed(
            &event(ENTRY, 2, Side::Sell, 101_00, 10, 3_500_000),
            &mut sb,
            1_000_000,
        );
        p += r.finish(&mut sb, 1_000_000);
        let batch = sb.finish().expect("batch");
        let times = batch
            .column_by_name("snapshot_time")
            .expect("column")
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .expect("timestamps")
            .values()
            .to_vec();
        assert_eq!(times, vec![T0 + 2_000_000, T0 + 3_000_000]);
        assert_eq!(p.snapshots, 2);
    }

    #[test]
    fn zero_lookahead_reproduces_the_plain_book() {
        let mut r = SymbolReplay::with_lookahead("TEST", 0);
        let mut sb = SnapshotBuilder::new(1);
        let mut p = Progress::default();
        for e in [
            event(ENTRY, 1, Side::Buy, 100_00, 10, 1_000),
            event(ENTRY, 2, Side::Sell, 100_00, 10, 5_000),
            event(CANCEL, 1, Side::Buy, 0, 0, 5_015),
        ] {
            p += r.feed(&e, &mut sb, 1_000_000);
        }
        p += r.finish(&mut sb, 1_000_000);
        assert_eq!(p.fills, 1);
        assert_eq!(r.book.stats().self_trade_preventions, 0);
    }
}
