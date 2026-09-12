//! A single-symbol limit order book driven by NSE order-level events.
//!
//! This is exchange mechanics, not trading logic: what the book looks like after a stream of
//! entry, modify and cancel events, including how NSE disclosed-quantity ("iceberg") orders
//! reveal themselves. Deciding what to *do* with the resulting book is the caller's business.
//!
//! Three behaviours are specific to this market and easy to get wrong:
//!
//! * **Disclosed quantity.** An order with `volume_disclosed` between 1 and
//!   `volume_original` shows only the disclosed amount. The remainder is real resting
//!   liquidity that is invisible to other participants.
//! * **Replenishment.** When the visible tranche is exhausted, the next tranche appears and
//!   **loses time priority**, going to the back of the queue at its price level.
//! * **Modify is cancel-then-enter.** Activity type 4 does not amend in place; it removes the
//!   order and re-enters it, so it loses queue position.
//!
//! Prices are integer paise throughout. Nothing here converts to floating point: comparing
//! prices is the core operation and integers make it exact.

use std::collections::{BTreeMap, HashMap, VecDeque};

/// Activity types in the NSE order feed.
pub const ENTRY: u8 = 1;
pub const CANCEL: u8 = 3;
pub const MODIFY: u8 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn from_byte(b: u8) -> Option<Side> {
        match b {
            b'B' => Some(Side::Buy),
            b'S' => Some(Side::Sell),
            _ => None,
        }
    }
}

/// One order event, as read from the orders feed.
#[derive(Debug, Clone, Copy)]
pub struct OrderEvent {
    pub activity_type: u8,
    pub order_number: u64,
    pub side: Side,
    /// Limit price in paise.
    pub price: i64,
    pub volume_disclosed: i64,
    pub volume_original: i64,
    /// Timestamp in the feed's own units (jiffies), kept opaque here.
    pub timestamp: i64,
    pub algo_indicator: u8,
    pub client_identity: u8,
}

#[derive(Debug, Clone)]
struct BookOrder {
    /// Distinguishes this placement from any earlier one with the same order number. A
    /// modify removes and re-enters the same id, so identifying a stale queue entry by
    /// absence from the order map is not enough.
    seq: u64,
    side: Side,
    price: i64,
    /// Quantity other participants can see.
    visible: i64,
    /// Size of each revealed tranche, which is the original disclosed quantity.
    tranche: i64,
    /// Quantity resting but not displayed.
    hidden: i64,
    /// Visible plus hidden.
    remaining: i64,
    is_iceberg: bool,
    algo_indicator: u8,
    client_identity: u8,
}

/// A price level: aggregate volumes plus the FIFO queue of order ids at that price.
#[derive(Debug, Default)]
struct Level {
    visible: i64,
    hidden: i64,
    /// (order id, placement sequence) in time priority. Removals leave the entry behind as a
    /// tombstone and it is skipped when reached, so cancelling is O(1) rather than a scan.
    queue: VecDeque<(u64, u64)>,
}

/// Aggregate book statistics that are cheap to maintain incrementally.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BookStats {
    pub events_applied: u64,
    pub entries: u64,
    pub cancels: u64,
    pub modifies: u64,
    /// Events ignored as unusable: non-positive price or quantity, unknown activity type.
    pub events_rejected: u64,
    /// Cancels and modifies naming an order the book has never seen. Normal at the start of
    /// a session when replay begins mid-stream; suspicious in bulk otherwise.
    pub unknown_order_refs: u64,
    /// Quantity matched against resting orders during entry.
    pub volume_matched: i64,
    pub trades_generated: u64,
    /// Times a hidden tranche became visible.
    pub replenishments: u64,
}

/// How the visible quantity at the touch divides up. Derivable only from the book itself.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TouchComposition {
    pub visible: i64,
    pub algo_visible: i64,
    pub custodian_visible: i64,
    pub iceberg_visible: i64,
    pub iceberg_hidden: i64,
    pub orders: u32,
}

/// A fill produced by matching an incoming order against the book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fill {
    pub price: i64,
    pub quantity: i64,
    pub resting_order: u64,
    pub incoming_order: u64,
    /// True when the resting side was a hidden tranche rather than displayed quantity.
    pub from_hidden: bool,
    pub timestamp: i64,
}

pub struct OrderBook {
    symbol: String,
    /// Keyed by price; best bid is the last key, best ask the first.
    bids: BTreeMap<i64, Level>,
    asks: BTreeMap<i64, Level>,
    orders: HashMap<u64, BookOrder>,
    stats: BookStats,
    resting_hidden: i64,
    active_icebergs: i64,
    /// Monotonic placement counter, used to invalidate stale queue entries.
    next_seq: u64,
    /// Fills from the most recent event, reused to avoid reallocating per event.
    fills: Vec<Fill>,
}

impl OrderBook {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            orders: HashMap::new(),
            stats: BookStats::default(),
            resting_hidden: 0,
            active_icebergs: 0,
            next_seq: 0,
            fills: Vec::new(),
        }
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn stats(&self) -> BookStats {
        self.stats
    }

    /// Fills generated by the most recent [`OrderBook::apply`] call.
    pub fn last_fills(&self) -> &[Fill] {
        &self.fills
    }

    pub fn resting_hidden_volume(&self) -> i64 {
        self.resting_hidden
    }

    pub fn active_icebergs(&self) -> i64 {
        self.active_icebergs
    }

    pub fn live_orders(&self) -> usize {
        self.orders.len()
    }

    /// Apply one event, returning the number of fills it generated.
    pub fn apply(&mut self, ev: &OrderEvent) -> usize {
        self.fills.clear();
        self.stats.events_applied += 1;
        match ev.activity_type {
            ENTRY => {
                self.stats.entries += 1;
                self.enter(ev);
            }
            CANCEL => {
                self.stats.cancels += 1;
                if self.remove(ev.order_number).is_none() {
                    self.stats.unknown_order_refs += 1;
                }
            }
            // A modify is a cancel followed by a fresh entry: the order loses queue position.
            MODIFY => {
                self.stats.modifies += 1;
                if self.remove(ev.order_number).is_none() {
                    self.stats.unknown_order_refs += 1;
                }
                self.enter(ev);
            }
            _ => self.stats.events_rejected += 1,
        }
        self.fills.len()
    }

    fn enter(&mut self, ev: &OrderEvent) {
        if ev.price <= 0 || ev.volume_original <= 0 {
            self.stats.events_rejected += 1;
            return;
        }

        let is_iceberg = ev.volume_disclosed > 0 && ev.volume_original > ev.volume_disclosed;
        let visible = if is_iceberg {
            ev.volume_disclosed
        } else {
            ev.volume_original
        };

        // Match the incoming order against the opposite side while it remains marketable.
        let mut remaining = ev.volume_original;
        remaining -= self.match_incoming(ev, remaining);

        if remaining <= 0 {
            return;
        }

        // Whatever is left rests. A partially filled iceberg keeps its tranche size, and
        // shows at most one tranche.
        let rest_visible = visible.min(remaining);
        self.next_seq += 1;
        let seq = self.next_seq;
        let order = BookOrder {
            seq,
            side: ev.side,
            price: ev.price,
            visible: rest_visible,
            tranche: if is_iceberg { ev.volume_disclosed } else { remaining },
            hidden: remaining - rest_visible,
            remaining,
            is_iceberg,
            algo_indicator: ev.algo_indicator,
            client_identity: ev.client_identity,
        };

        if order.hidden > 0 {
            self.resting_hidden += order.hidden;
            self.active_icebergs += 1;
        }

        let level = match ev.side {
            Side::Buy => self.bids.entry(ev.price).or_default(),
            Side::Sell => self.asks.entry(ev.price).or_default(),
        };
        level.visible += order.visible;
        level.hidden += order.hidden;
        level.queue.push_back((ev.order_number, seq));
        self.orders.insert(ev.order_number, order);
    }

    /// Match an incoming order against the resting book. Returns the quantity filled.
    fn match_incoming(&mut self, ev: &OrderEvent, mut remaining: i64) -> i64 {
        let mut filled = 0i64;

        while remaining > 0 {
            // Best opposing price, if it is still crossed with the incoming limit.
            let best = match ev.side {
                Side::Buy => self.asks.keys().next().copied(),
                Side::Sell => self.bids.keys().next_back().copied(),
            };
            let Some(price) = best else { break };
            let crossed = match ev.side {
                Side::Buy => ev.price >= price,
                Side::Sell => ev.price <= price,
            };
            if !crossed {
                break;
            }

            let taken = self.match_at_price(ev, price, remaining);
            if taken == 0 {
                // Nothing available here and the level is exhausted; drop it and continue so
                // the loop cannot spin on an empty level.
                self.drop_level(ev.side.opposite(), price);
                continue;
            }
            remaining -= taken;
            filled += taken;
        }

        self.stats.volume_matched += filled;
        filled
    }

    /// Consume up to `want` from one price level, honouring time priority.
    fn match_at_price(&mut self, ev: &OrderEvent, price: i64, want: i64) -> i64 {
        let mut taken = 0i64;
        let opposite = ev.side.opposite();

        while taken < want {
            let Some(&(id, seq)) = self
                .level_mut(opposite, price)
                .and_then(|l| l.queue.front())
            else {
                break;
            };

            // Stale entry: the order was cancelled, or this placement was superseded by a
            // modify that re-entered the same order number.
            let live = matches!(self.orders.get(&id), Some(o) if o.seq == seq);
            if !live {
                self.level_mut(opposite, price).unwrap().queue.pop_front();
                continue;
            }

            let order = self.orders.get(&id).expect("checked live");
            if order.visible <= 0 {
                if order.is_iceberg && order.remaining > 0 {
                    self.reveal_tranche(id, opposite, price);
                    // Revealed liquidity goes behind whatever is already queued.
                    let lvl = self.level_mut(opposite, price).expect("level exists");
                    lvl.queue.pop_front();
                    lvl.queue.push_back((id, seq));
                    continue;
                }
                self.level_mut(opposite, price).unwrap().queue.pop_front();
                continue;
            }

            let order = self.orders.get_mut(&id).expect("checked live");
            let fill = (want - taken).min(order.visible);
            if fill <= 0 {
                break;
            }
            order.visible -= fill;
            order.remaining -= fill;
            let exhausted = order.remaining <= 0;
            let reveal_next = order.visible <= 0 && order.is_iceberg && !exhausted;
            let rest_price = order.price;

            self.level_mut(opposite, price).expect("level exists").visible -= fill;
            taken += fill;
            self.fills.push(Fill {
                price: rest_price,
                quantity: fill,
                resting_order: id,
                incoming_order: ev.order_number,
                from_hidden: false,
                timestamp: ev.timestamp,
            });
            self.stats.trades_generated += 1;

            if exhausted {
                self.level_mut(opposite, price).unwrap().queue.pop_front();
                if let Some(o) = self.orders.remove(&id) {
                    if o.hidden > 0 {
                        self.resting_hidden -= o.hidden;
                        self.active_icebergs -= 1;
                        let lvl = self.level_mut(opposite, price).expect("level exists");
                        lvl.hidden -= o.hidden;
                    }
                }
            } else if reveal_next {
                self.reveal_tranche(id, opposite, price);
                let lvl = self.level_mut(opposite, price).expect("level exists");
                lvl.queue.pop_front();
                lvl.queue.push_back((id, seq));
            }
            // Otherwise the order still shows quantity and keeps the front of the queue;
            // `taken` has reached `want` by construction, so the loop ends.
        }

        // Drop a level with nothing left queued so the caller's sweep cannot spin on it.
        if let Some(lvl) = self.level_mut(opposite, price) {
            if lvl.queue.is_empty() {
                self.drop_level(opposite, price);
            }
        }
        taken
    }

    /// Make the next hidden tranche of an iceberg visible.
    fn reveal_tranche(&mut self, id: u64, side: Side, price: i64) {
        let Some(order) = self.orders.get_mut(&id) else { return };
        let reveal = order.tranche.min(order.remaining);
        if reveal <= 0 {
            return;
        }
        order.visible = reveal;
        order.hidden = order.remaining - reveal;
        let still_hidden = order.hidden > 0;
        if let Some(lvl) = self.level_mut(side, price) {
            lvl.visible += reveal;
            lvl.hidden -= reveal;
        }
        self.resting_hidden -= reveal;
        self.stats.replenishments += 1;
        if !still_hidden {
            self.active_icebergs -= 1;
        }
    }

    fn level_mut(&mut self, side: Side, price: i64) -> Option<&mut Level> {
        match side {
            Side::Buy => self.bids.get_mut(&price),
            Side::Sell => self.asks.get_mut(&price),
        }
    }

    fn drop_level(&mut self, side: Side, price: i64) {
        match side {
            Side::Buy => self.bids.remove(&price),
            Side::Sell => self.asks.remove(&price),
        };
    }

    fn remove(&mut self, order_number: u64) -> Option<()> {
        let order = self.orders.remove(&order_number)?;
        if order.hidden > 0 {
            self.resting_hidden -= order.hidden;
            self.active_icebergs -= 1;
        }
        let price = order.price;
        let side = order.side;
        if let Some(level) = self.level_mut(side, price) {
            level.visible -= order.visible;
            level.hidden -= order.hidden;
            // The id stays in the queue as a tombstone and is skipped when reached.
            if level.visible <= 0 && level.hidden <= 0 {
                self.drop_level(side, price);
            }
        }
        Some(())
    }

    // --- inspection ---------------------------------------------------------------------

    pub fn best_bid(&self) -> Option<i64> {
        self.bids.iter().next_back().map(|(p, _)| *p)
    }

    pub fn best_ask(&self) -> Option<i64> {
        self.asks.iter().next().map(|(p, _)| *p)
    }

    pub fn spread(&self) -> Option<i64> {
        Some(self.best_ask()? - self.best_bid()?)
    }

    /// Mid price in paise, as a float because a mid can land on a half-paise.
    pub fn mid_price(&self) -> Option<f64> {
        Some((self.best_bid()? as f64 + self.best_ask()? as f64) / 2.0)
    }

    pub fn total_visible(&self, side: Side) -> i64 {
        match side {
            Side::Buy => self.bids.values().map(|l| l.visible).sum(),
            Side::Sell => self.asks.values().map(|l| l.visible).sum(),
        }
    }

    /// Top `n` levels as (price, visible, hidden), best first.
    pub fn top_levels(&self, side: Side, n: usize) -> Vec<(i64, i64, i64)> {
        let take = |it: &mut dyn Iterator<Item = (&i64, &Level)>| -> Vec<(i64, i64, i64)> {
            it.filter(|(_, l)| l.visible > 0)
                .take(n)
                .map(|(p, l)| (*p, l.visible, l.hidden))
                .collect()
        };
        match side {
            Side::Buy => take(&mut self.bids.iter().rev()),
            Side::Sell => take(&mut self.asks.iter()),
        }
    }

    /// Visible quantity resting within `bps` of the mid, per side.
    pub fn depth_within_bps(&self, bps: f64) -> (i64, i64) {
        let Some(mid) = self.mid_price() else {
            return (0, 0);
        };
        let band = mid * bps / 10_000.0;
        let bid_floor = (mid - band) as i64;
        let ask_ceil = (mid + band) as i64;
        let bid = self
            .bids
            .range(bid_floor..)
            .map(|(_, l)| l.visible)
            .sum::<i64>();
        let ask = self
            .asks
            .range(..=ask_ceil)
            .map(|(_, l)| l.visible)
            .sum::<i64>();
        (bid, ask)
    }

    /// Composition of the touch: how the visible quantity at the best price divides by
    /// order source and beneficiary, plus the number of live orders queued there.
    ///
    /// This cannot be recovered from an L2 snapshot: it depends on which resting orders are
    /// at the touch, which only the book knows. Algo and custodian codes are as the feed
    /// defines them - algo_indicator 0 and 2 are algorithmic, client_identity 1 is a
    /// custodian (CP) order.
    pub fn touch_composition(&self, side: Side) -> TouchComposition {
        let mut out = TouchComposition::default();
        let Some(price) = (match side {
            Side::Buy => self.best_bid(),
            Side::Sell => self.best_ask(),
        }) else {
            return out;
        };
        let level = match side {
            Side::Buy => self.bids.get(&price),
            Side::Sell => self.asks.get(&price),
        };
        let Some(level) = level else { return out };

        for &(id, seq) in level.queue.iter() {
            let Some(o) = self.orders.get(&id) else { continue };
            if o.seq != seq || o.visible <= 0 {
                continue;
            }
            out.orders += 1;
            out.visible += o.visible;
            if matches!(o.algo_indicator, 0 | 2) {
                out.algo_visible += o.visible;
            }
            if o.client_identity == 1 {
                out.custodian_visible += o.visible;
            }
            if o.is_iceberg {
                out.iceberg_visible += o.visible;
                out.iceberg_hidden += o.hidden;
            }
        }
        out
    }

    /// True when the best bid is at or above the best ask, which should never persist in
    /// continuous trading and indicates the replay has diverged from the exchange.
    pub fn is_crossed(&self) -> bool {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => b >= a,
            _ => false,
        }
    }
}

impl Side {
    fn opposite(self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(activity: u8, id: u64, side: Side, price: i64, disclosed: i64, original: i64) -> OrderEvent {
        OrderEvent {
            activity_type: activity,
            order_number: id,
            side,
            price,
            volume_disclosed: disclosed,
            volume_original: original,
            timestamp: id as i64,
            algo_indicator: 1,
            client_identity: 3,
        }
    }

    fn limit(id: u64, side: Side, price: i64, qty: i64) -> OrderEvent {
        ev(ENTRY, id, side, price, 0, qty)
    }

    #[test]
    fn touch_composition_splits_the_best_level_by_source() {
        let mut b = OrderBook::new("TEST");
        // algo_indicator 0 is algorithmic, 1 is not; client_identity 1 is a custodian.
        let mut a = limit(1, Side::Buy, 100_00, 30);
        a.algo_indicator = 0;
        a.client_identity = 3;
        let mut c = limit(2, Side::Buy, 100_00, 20);
        c.algo_indicator = 1;
        c.client_identity = 1;
        b.apply(&a);
        b.apply(&c);
        // A deeper level must not be counted.
        b.apply(&limit(3, Side::Buy, 99_00, 100));

        let t = b.touch_composition(Side::Buy);
        assert_eq!(t.visible, 50);
        assert_eq!(t.algo_visible, 30);
        assert_eq!(t.custodian_visible, 20);
        assert_eq!(t.orders, 2);
        assert_eq!(b.touch_composition(Side::Sell), TouchComposition::default());
    }

    #[test]
    fn touch_composition_reports_iceberg_share_and_ignores_tombstones() {
        let mut b = OrderBook::new("TEST");
        b.apply(&ev(ENTRY, 1, Side::Sell, 100_00, 10, 100));
        b.apply(&limit(2, Side::Sell, 100_00, 40));
        b.apply(&ev(CANCEL, 2, Side::Sell, 100_00, 0, 40));
        let t = b.touch_composition(Side::Sell);
        assert_eq!(t.visible, 10, "cancelled order must not be counted");
        assert_eq!(t.iceberg_visible, 10);
        assert_eq!(t.iceberg_hidden, 90);
        assert_eq!(t.orders, 1);
    }

    #[test]
    fn resting_orders_form_a_two_sided_book() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Buy, 100_00, 50));
        b.apply(&limit(2, Side::Sell, 101_00, 40));
        assert_eq!(b.best_bid(), Some(100_00));
        assert_eq!(b.best_ask(), Some(101_00));
        assert_eq!(b.spread(), Some(100));
        assert_eq!(b.mid_price(), Some(100_50.0));
        assert!(!b.is_crossed());
    }

    #[test]
    fn a_marketable_order_matches_and_does_not_rest() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Sell, 100_00, 30));
        let fills = b.apply(&limit(2, Side::Buy, 100_00, 30));
        assert_eq!(fills, 1);
        assert_eq!(b.last_fills()[0].quantity, 30);
        assert_eq!(b.best_ask(), None, "resting sell should be fully consumed");
        assert_eq!(b.best_bid(), None, "incoming buy was fully filled");
        assert_eq!(b.stats().volume_matched, 30);
    }

    #[test]
    fn partial_fill_leaves_the_remainder_resting() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Sell, 100_00, 10));
        b.apply(&limit(2, Side::Buy, 100_00, 25));
        assert_eq!(b.best_ask(), None);
        assert_eq!(b.best_bid(), Some(100_00));
        assert_eq!(b.total_visible(Side::Buy), 15, "25 in, 10 filled, 15 rests");
    }

    #[test]
    fn a_non_marketable_order_rests_without_matching() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Sell, 101_00, 10));
        let fills = b.apply(&limit(2, Side::Buy, 100_00, 10));
        assert_eq!(fills, 0);
        assert_eq!(b.total_visible(Side::Buy), 10);
        assert_eq!(b.total_visible(Side::Sell), 10);
    }

    #[test]
    fn matching_walks_price_levels_best_first() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Sell, 100_00, 10));
        b.apply(&limit(2, Side::Sell, 101_00, 10));
        b.apply(&limit(3, Side::Sell, 102_00, 10));
        b.apply(&limit(4, Side::Buy, 101_00, 25));

        // Should sweep 100.00 and 101.00 fully, leave 102.00, and rest 5 at 101.00.
        let fills = b.last_fills();
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].price, 100_00);
        assert_eq!(fills[1].price, 101_00);
        assert_eq!(b.best_ask(), Some(102_00));
        assert_eq!(b.total_visible(Side::Buy), 5);
    }

    #[test]
    fn time_priority_is_respected_within_a_level() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Sell, 100_00, 10));
        b.apply(&limit(2, Side::Sell, 100_00, 10));
        b.apply(&limit(3, Side::Buy, 100_00, 10));
        let fills = b.last_fills();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].resting_order, 1, "the earlier order fills first");
    }

    #[test]
    fn an_iceberg_shows_only_its_disclosed_quantity() {
        let mut b = OrderBook::new("TEST");
        // 1000 total, 100 disclosed.
        b.apply(&ev(ENTRY, 1, Side::Sell, 100_00, 100, 1000));
        assert_eq!(b.total_visible(Side::Sell), 100, "only the tranche is visible");
        assert_eq!(b.resting_hidden_volume(), 900);
        assert_eq!(b.active_icebergs(), 1);
        let top = b.top_levels(Side::Sell, 1);
        assert_eq!(top[0], (100_00, 100, 900));
    }

    #[test]
    fn an_iceberg_replenishes_when_its_visible_tranche_is_exhausted() {
        let mut b = OrderBook::new("TEST");
        b.apply(&ev(ENTRY, 1, Side::Sell, 100_00, 100, 1000));
        b.apply(&limit(2, Side::Buy, 100_00, 100)); // exactly consumes the tranche

        assert_eq!(b.stats().replenishments, 1);
        assert_eq!(b.total_visible(Side::Sell), 100, "next tranche is now showing");
        assert_eq!(b.resting_hidden_volume(), 800);
        assert_eq!(b.best_ask(), Some(100_00));
    }

    #[test]
    fn a_replenished_tranche_loses_time_priority() {
        let mut b = OrderBook::new("TEST");
        b.apply(&ev(ENTRY, 1, Side::Sell, 100_00, 10, 100)); // iceberg, first in queue
        b.apply(&limit(2, Side::Sell, 100_00, 10)); // plain order behind it
        b.apply(&limit(3, Side::Buy, 100_00, 10)); // consumes the iceberg's tranche
        assert_eq!(b.last_fills()[0].resting_order, 1);

        // The iceberg revealed a new tranche, so the plain order should now be ahead of it.
        b.apply(&limit(4, Side::Buy, 100_00, 10));
        assert_eq!(
            b.last_fills()[0].resting_order, 2,
            "revealed liquidity must go behind orders already queued"
        );
    }

    #[test]
    fn an_iceberg_can_be_swept_across_tranches_by_one_large_order() {
        let mut b = OrderBook::new("TEST");
        b.apply(&ev(ENTRY, 1, Side::Sell, 100_00, 100, 500));
        b.apply(&limit(2, Side::Buy, 100_00, 500));
        assert_eq!(b.best_ask(), None, "the whole iceberg should be consumed");
        assert_eq!(b.resting_hidden_volume(), 0);
        assert_eq!(b.active_icebergs(), 0);
        assert_eq!(b.stats().volume_matched, 500);
        assert_eq!(b.total_visible(Side::Buy), 0, "incoming order fully filled");
    }

    #[test]
    fn cancel_removes_resting_liquidity() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Buy, 100_00, 50));
        b.apply(&ev(CANCEL, 1, Side::Buy, 100_00, 0, 50));
        assert_eq!(b.best_bid(), None);
        assert_eq!(b.live_orders(), 0);
        assert_eq!(b.stats().cancels, 1);
    }

    #[test]
    fn cancelling_an_iceberg_releases_its_hidden_quantity() {
        let mut b = OrderBook::new("TEST");
        b.apply(&ev(ENTRY, 1, Side::Sell, 100_00, 100, 1000));
        b.apply(&ev(CANCEL, 1, Side::Sell, 100_00, 100, 1000));
        assert_eq!(b.resting_hidden_volume(), 0);
        assert_eq!(b.active_icebergs(), 0);
        assert_eq!(b.best_ask(), None);
    }

    #[test]
    fn modify_loses_queue_position() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Sell, 100_00, 10));
        b.apply(&limit(2, Side::Sell, 100_00, 10));
        // Modify order 1: it is removed and re-entered, so it now sits behind order 2.
        b.apply(&ev(MODIFY, 1, Side::Sell, 100_00, 0, 10));
        b.apply(&limit(3, Side::Buy, 100_00, 10));
        assert_eq!(b.last_fills()[0].resting_order, 2);
        assert_eq!(b.stats().modifies, 1);
    }

    #[test]
    fn modify_can_reprice_an_order() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Buy, 100_00, 10));
        b.apply(&ev(MODIFY, 1, Side::Buy, 99_00, 0, 10));
        assert_eq!(b.best_bid(), Some(99_00));
        assert_eq!(b.total_visible(Side::Buy), 10);
    }

    #[test]
    fn cancelling_an_unknown_order_is_counted_not_fatal() {
        let mut b = OrderBook::new("TEST");
        b.apply(&ev(CANCEL, 999, Side::Buy, 100_00, 0, 10));
        assert_eq!(b.stats().unknown_order_refs, 1);
        assert_eq!(b.live_orders(), 0);
    }

    #[test]
    fn unusable_events_are_rejected_rather_than_corrupting_the_book() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Buy, 0, 10)); // zero price
        b.apply(&limit(2, Side::Buy, 100_00, 0)); // zero quantity
        b.apply(&ev(7, 3, Side::Buy, 100_00, 0, 10)); // unknown activity type
        assert_eq!(b.stats().events_rejected, 3);
        assert_eq!(b.live_orders(), 0);
    }

    #[test]
    fn top_levels_are_ordered_best_first_on_both_sides() {
        let mut b = OrderBook::new("TEST");
        for (i, p) in [98_00, 99_00, 100_00].iter().enumerate() {
            b.apply(&limit(i as u64 + 1, Side::Buy, *p, 10));
        }
        for (i, p) in [101_00, 102_00, 103_00].iter().enumerate() {
            b.apply(&limit(i as u64 + 10, Side::Sell, *p, 10));
        }
        let bids: Vec<i64> = b.top_levels(Side::Buy, 3).iter().map(|l| l.0).collect();
        let asks: Vec<i64> = b.top_levels(Side::Sell, 3).iter().map(|l| l.0).collect();
        assert_eq!(bids, vec![100_00, 99_00, 98_00], "bids descend from the best");
        assert_eq!(asks, vec![101_00, 102_00, 103_00], "asks ascend from the best");
    }

    #[test]
    fn depth_within_bps_counts_only_nearby_levels() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Buy, 100_00, 10));
        b.apply(&limit(2, Side::Buy, 90_00, 10)); // ~10% away
        b.apply(&limit(3, Side::Sell, 101_00, 10));
        b.apply(&limit(4, Side::Sell, 111_00, 10));
        // Mid is 100.50; a 200bp band is about 2 rupees either side.
        let (bid, ask) = b.depth_within_bps(200.0);
        assert_eq!(bid, 10, "only the 100.00 bid is within band");
        assert_eq!(ask, 10, "only the 101.00 ask is within band");
    }

    #[test]
    fn sweeping_a_level_empty_removes_it() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Sell, 100_00, 10));
        b.apply(&limit(2, Side::Sell, 101_00, 10));
        b.apply(&limit(3, Side::Buy, 100_00, 10));
        assert_eq!(b.best_ask(), Some(101_00), "emptied level must be gone");
    }

    #[test]
    fn the_book_never_stays_crossed_after_an_aggressive_entry() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Sell, 100_00, 10));
        // A buy far above the ask must match rather than rest above it.
        b.apply(&limit(2, Side::Buy, 105_00, 5));
        assert!(!b.is_crossed(), "bid {:?} ask {:?}", b.best_bid(), b.best_ask());
        assert_eq!(b.best_ask(), Some(100_00));
        assert_eq!(b.best_bid(), None);
    }

    #[test]
    fn many_cancels_do_not_degrade_into_a_queue_scan() {
        // Tombstoned cancels must not accumulate into O(n) work per match. This is a
        // behavioural check: 50k orders queued and cancelled, then one match.
        let mut b = OrderBook::new("TEST");
        for i in 1..=50_000u64 {
            b.apply(&limit(i, Side::Sell, 100_00, 1));
        }
        for i in 1..=49_999u64 {
            b.apply(&ev(CANCEL, i, Side::Sell, 100_00, 0, 1));
        }
        b.apply(&limit(100_001, Side::Buy, 100_00, 1));
        assert_eq!(b.last_fills()[0].resting_order, 50_000);
        assert_eq!(b.best_ask(), None);
    }
}
