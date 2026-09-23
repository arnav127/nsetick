//! A single-symbol limit order book driven by NSE order-level events.
//!
//! This is exchange mechanics, not trading logic: what the book looks like after a stream of
//! entry, modify and cancel events, including how NSE disclosed-quantity ("iceberg") orders
//! reveal themselves. Deciding what to *do* with the resulting book is the caller's business.
//!
//! Every rule here was established from the exchange's own trade file, which names the buy
//! and sell order of every execution, and the replay reproduces that file record for record:
//! `fills::replay_fills` regenerates the records, `tools/verify_replay.py` scores them, and
//! `examples/oracle_diff` finds the first point where they part. The wiki page "Matching
//! Engine" walks through all of it.
//!
//! The session:
//!
//! * **Pre-open call auction** (09:00 to about 09:08): orders are collected, not matched, then
//!   matched at one equilibrium price. See the `auction` module.
//! * **Continuous trading** from 09:15, by price-time priority, every trade at the resting
//!   order's price.
//! * **Post-close session** (from 15:40): every order trades at the closing price, the
//!   15:00-15:30 volume-weighted average truncated to whole paise and rounded to the tick.
//!
//! Order types:
//!
//! * **Market orders** carry a limit price of zero. They take liquidity at any price and
//!   never rest.
//! * **Immediate-or-cancel** orders never rest; the unfilled part is discarded on entry. The
//!   feed writes a cancel for it microseconds later, which the book recognises.
//! * **Stop-loss** orders are held off the book until the *feed* says they triggered: the
//!   exchange writes a second entry record for the same order number at that moment. A modify
//!   that clears the stop flag converts the order; a modify that sets it takes it off the book.
//!
//! Self-trade prevention: when an incoming order reaches a resting order of the same client,
//! the exchange cancels one of them - usually the resting order, sometimes the incoming one.
//! The feed has no client identifier and shows only the cancel, stamped one jiffy (1/65536 s)
//! per message of the incoming order's match; the book looks ahead for a cancel in exactly
//! that slot (see [`OrderBook::announce`]). The cancel is a message of its own, so a trade after it is a new
//! record even between the same two orders.
//!
//! Disclosed quantity ("icebergs"):
//!
//! * An order with `volume_disclosed` between 1 and `volume_original` shows one tranche at a
//!   time. The exchange counts down what is left of the current tranche, and every trade the
//!   order takes part in, as resting order or as aggressor, counts against it: a trade that
//!   takes the whole tranche leaves a fresh full one, a smaller trade reduces it.
//! * When a tranche is used up, the next one joins the **back** of the queue. An iceberg
//!   alone at its price comes straight back to the front, so an incoming order can run on
//!   into it; that is still one trade record.
//!
//! Modifies (activity type 4, which carries the *remaining* quantity):
//!
//! * At the same price, an order keeps its place **unless its displayed quantity grows**.
//! * An iceberg keeps what is left of its current tranche when the disclosed quantity is
//!   unchanged, or is cut to exactly what remained - unless the modify raises the quantity of
//!   an order that was showing all it had left. Otherwise it shows a fresh tranche. See
//!   `carries_tranche`.
//! * Any other modify is cancel-then-enter: the order goes to the back and may trade at once.
//!
//! Prices are integer paise throughout. Nothing here converts to floating point: comparing
//! prices is the core operation and integers make it exact.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

#[path = "auction.rs"]
mod auction;

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
    /// Microseconds since the Unix epoch, as decoded from the feed's jiffies. Intervals and
    /// the self-trade-prevention window are measured in these units.
    pub timestamp: i64,
    pub algo_indicator: u8,
    pub client_identity: u8,
    /// Immediate-or-cancel: whatever does not fill on entry must not rest.
    pub ioc: bool,
    /// Market order. The feed records these with a limit price of zero; they take liquidity
    /// at any price and never rest.
    pub market: bool,
    /// Stop-loss order: held off the book until the feed records its trigger (a second entry
    /// record for the same order number), then entered as a limit (or market) order.
    pub stop_loss: bool,
    /// Trigger price in paise for a stop-loss order; zero otherwise.
    pub trigger_price: i64,
}

impl OrderEvent {
    /// Does this event place the order in the waiting (untriggered) stop-loss state?
    fn is_pending_stop(&self) -> bool {
        self.stop_loss && self.trigger_price > 0
    }
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
    /// What is left of the current tranche. Visible is this capped by what remains of the
    /// order; the exchange keeps the uncapped figure, which a modify can carry over.
    tranche_left: i64,
    /// Quantity resting but not displayed.
    hidden: i64,
    /// Visible plus hidden.
    remaining: i64,
    is_iceberg: bool,
    /// The visible quantity is a tranche revealed after entry: it was hidden when placed.
    revealed: bool,
    /// When this placement joined its queue: the entry, or the reveal of the current tranche.
    placed_at: i64,
    algo_indicator: u8,
    client_identity: u8,
}

/// A price level: aggregate volumes plus the FIFO queue of order ids at that price.
#[derive(Debug, Default, Clone)]
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
    /// Market orders entered. Earlier releases rejected these as zero-priced.
    pub market_orders: u64,
    /// Quantity of IOC and market orders left unfilled on entry and therefore not rested.
    pub unrested_quantity: i64,
    /// Cancels the feed writes for an IOC or market remainder the book already discarded.
    /// Counted here rather than as unknown references, which would bury real problems.
    pub remainder_cancels: u64,
    /// Stop-loss orders placed in the waiting state, on entry or by a modify re-arming them.
    pub stops_held: u64,
    /// Stop-loss orders the feed reported as triggered (a repeated entry record) or a modify
    /// converted to an ordinary order, and which therefore entered the book.
    pub stops_triggered: u64,
    /// Modifies that only reduced quantity at an unchanged price, keeping queue priority.
    pub amended_in_place: u64,
    /// Resting orders withdrawn instead of matched because the exchange cancelled them as
    /// the incoming order arrived - self-trade prevention. Requires announced events.
    pub self_trade_preventions: u64,
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
    /// True when some of the quantity came from a tranche that was hidden when the resting
    /// order was placed and revealed later, rather than from its original displayed quantity.
    pub from_hidden: bool,
    pub timestamp: i64,
    /// Side of the incoming (aggressing) order.
    pub aggressor: Side,
}

impl Fill {
    /// The buying order of the pair, as the trade file records it.
    pub fn buy_order(&self) -> u64 {
        match self.aggressor {
            Side::Buy => self.incoming_order,
            Side::Sell => self.resting_order,
        }
    }

    /// The selling order of the pair.
    pub fn sell_order(&self) -> u64 {
        match self.aggressor {
            Side::Buy => self.resting_order,
            Side::Sell => self.incoming_order,
        }
    }
}

/// An execution known from the exchange's trade file, used to drive a match instead of the
/// matching rules. See [`OrderBook::force_next_match`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KnownTrade {
    pub resting_order: u64,
    pub price: i64,
    pub quantity: i64,
}

/// One live order in a price level's queue, front first. See [`OrderBook::queue`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueuedOrder {
    pub order_number: u64,
    pub visible: i64,
    pub hidden: i64,
    pub is_iceberg: bool,
    /// The visible quantity is a tranche revealed after the order was placed.
    pub revealed: bool,
    /// When this placement joined the queue (entry, re-entry, or tranche reveal).
    pub placed_at: i64,
    pub algo_indicator: u8,
    pub client_identity: u8,
}

#[derive(Clone)]
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
    /// Stop-loss orders waiting for their trigger, by order number.
    stops: HashMap<u64, OrderEvent>,
    /// Price of the most recent fill the book generated.
    last_trade_price: Option<i64>,
    /// IOC and market orders whose unfilled remainder was discarded on entry. The feed
    /// follows each with a cancel a few microseconds later; this lets that cancel be
    /// recognised instead of counted as a reference to an unknown order.
    discarded_remainders: HashSet<u64>,
    /// Timestamps of cancels already read from the feed but not yet applied, per order. The
    /// caller announces events slightly ahead of applying them (see [`OrderBook::announce`]),
    /// which is what lets the book see a self-trade-prevention cancel before it matches.
    scheduled_cancels: HashMap<u64, VecDeque<i64>>,
    /// Resting orders withdrawn at match time because the exchange cancelled them; their own
    /// cancel arrives later and is recognised rather than counted as unknown.
    preempted: HashSet<u64>,
    /// Messages the current incoming order's match has produced so far: trades plus
    /// self-trade cancels. Locates each step of the sweep in the feed's clock.
    sweep_steps: usize,
    /// Traded value and quantity from 15:00 to 15:30, from which the closing price is set.
    close_vwap: (i128, i128),
    /// The closing price, once the post-close session has begun.
    closing_price: Option<i64>,
    /// Orders collected in the pre-open session, until the call auction runs.
    pre_open: HashMap<u64, auction::PreOrder>,
    /// Time of the last pre-open event: auction trades are stamped with it.
    pre_open_last: i64,
    auction_done: bool,
    /// Previous session's close, to break ties between auction prices.
    previous_close: Option<i64>,
    /// The next fill starts a new trade record even if it continues the last one's pair.
    print_break: bool,
    /// Self-trade prevention cancelled the incoming order during the current match.
    incoming_withdrawn: bool,
    /// Visible quantity a re-entering iceberg keeps from its previous placement.
    carry_visible: Option<i64>,
    /// Executions to use instead of the matching rules for the next event, if set.
    forced: Option<Vec<KnownTrade>>,
    /// Known executions naming a resting order the book does not hold.
    forced_unknown: u64,
    /// Quantity of known executions the book could not supply.
    forced_shortfall: i64,
}

const DAY_MICROS: i64 = 86_400 * 1_000_000;
const fn tod(h: i64, m: i64) -> i64 {
    (h * 3600 + m * 60) * 1_000_000
}
/// Continuous trading starts at 09:15; before it, orders are collected for the call auction.
const CONTINUOUS_FROM: i64 = tod(9, 15);
/// Trades from 15:00 up to 15:30 set the closing price.
const CLOSE_VWAP_FROM: i64 = tod(15, 0);
const CLOSE_VWAP_TO: i64 = tod(15, 30);
/// The post-close session: continuous trading has ended (15:30) and closing-price orders are
/// taken from 15:40. Anything from 15:35 belongs to it.
const CLOSING_SESSION_FROM: i64 = tod(15, 35);
/// Price step in paise for Capital Market equities.
const TICK: i64 = 5;

/// One tick of the feed's clock: 1/65536 of a second, in microseconds.
///
/// The exchange stamps each message a matching event produces - every trade, and every order
/// it cancels along the way - one tick after the previous one. A sweep through 300 resting
/// orders therefore spans 300 ticks, about 4.6 ms, in the feed.
pub const JIFFY_MICROS: f64 = 1_000_000.0 / 65_536.0;

/// How far ahead of the event being applied the replay reads, so that cancels the exchange
/// issued while processing it are already known. Covers sweeps of several thousand steps.
///
/// NSE prevents a client trading with itself by cancelling the *resting* order when an
/// incoming order from the same client reaches it. The feed carries no client identifier; it
/// shows the resting order's cancel, stamped at the step of the sweep where the incoming
/// order reached it. The lookahead lets the book see that cancel before it matches.
pub const SELF_TRADE_WINDOW_MICROS: i64 = 100_000;

/// An order about to rest, as [`OrderBook::insert_resting`] takes it.
struct Resting {
    order_number: u64,
    side: Side,
    price: i64,
    remaining: i64,
    tranche: i64,
    tranche_left: i64,
    is_iceberg: bool,
    placed_at: i64,
    algo_indicator: u8,
    client_identity: u8,
}

/// Does a modify keep what is left of an iceberg's current tranche, rather than starting a
/// fresh one? The exchange keeps counting the current tranche down when the disclosed quantity
/// is unchanged, or when it is cut to exactly the quantity remaining before the modify. Either
/// way, raising the order's quantity while it is showing all it has left starts a fresh
/// tranche instead - for an unchanged disclosed quantity, whenever the order was down to less
/// than one tranche. Any other change of disclosed quantity shows a fresh tranche of the new
/// size. Found and checked trade by trade against the trade file.
fn carries_tranche(o: &BookOrder, ev: &OrderEvent) -> bool {
    if !o.is_iceberg || ev.volume_disclosed <= 0 {
        return false;
    }
    let raised = ev.volume_original > o.remaining;
    let same = ev.volume_disclosed == o.tranche && !(raised && o.remaining < o.tranche);
    let cut_to_remainder = ev.volume_disclosed < o.tranche
        && (ev.volume_disclosed == o.remaining
            || (ev.volume_original == o.remaining && ev.volume_disclosed >= ev.volume_original))
        && !(raised && o.tranche_left >= o.remaining);
    same || cut_to_remainder
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
            stops: HashMap::new(),
            last_trade_price: None,
            discarded_remainders: HashSet::new(),
            scheduled_cancels: HashMap::new(),
            preempted: HashSet::new(),
            sweep_steps: 0,
            pre_open: HashMap::new(),
            pre_open_last: 0,
            auction_done: false,
            previous_close: None,
            close_vwap: (0, 0),
            closing_price: None,
            print_break: false,
            incoming_withdrawn: false,
            carry_visible: None,
            forced: None,
            forced_unknown: 0,
            forced_shortfall: 0,
        }
    }

    /// Match the next applied event against these executions instead of by the matching
    /// rules: each named resting order gives up the stated quantity, revealing iceberg tranches
    /// as needed, and whatever the incoming order does not fill rests or is discarded as usual.
    ///
    /// This is how a replay follows the exchange's trade file exactly. Applies to the next
    /// [`OrderBook::apply`] call only; an event that does not match ignores it.
    pub fn force_next_match(&mut self, trades: Vec<KnownTrade>) {
        self.forced = Some(trades);
    }

    /// Execute a known trade between two resting orders, as the pre-open call auction does.
    /// Returns the quantity the book could supply on each side.
    pub fn force_cross(
        &mut self,
        buy: u64,
        sell: u64,
        quantity: i64,
        timestamp: i64,
    ) -> (i64, i64) {
        self.fills.clear();
        let b = self.take_resting(buy, quantity, sell, Side::Sell, timestamp);
        let s = self.take_resting(sell, quantity, buy, Side::Buy, timestamp);
        self.stats.volume_matched += quantity.min(b).min(s);
        (b, s)
    }

    /// Known executions that named an unknown order, and the quantity the book could not
    /// supply, across every forced match so far.
    pub fn forced_misses(&self) -> (u64, i64) {
        (self.forced_unknown, self.forced_shortfall)
    }

    /// The live orders queued at one price, in priority order.
    pub fn queue(&self, side: Side, price: i64) -> Vec<QueuedOrder> {
        let level = match side {
            Side::Buy => self.bids.get(&price),
            Side::Sell => self.asks.get(&price),
        };
        let Some(level) = level else {
            return Vec::new();
        };
        level
            .queue
            .iter()
            .filter_map(|&(id, seq)| {
                let o = self.orders.get(&id).filter(|o| o.seq == seq)?;
                Some(QueuedOrder {
                    order_number: id,
                    visible: o.visible,
                    hidden: o.hidden,
                    is_iceberg: o.is_iceberg,
                    revealed: o.revealed,
                    placed_at: o.placed_at,
                    algo_indicator: o.algo_indicator,
                    client_identity: o.client_identity,
                })
            })
            .collect()
    }

    /// Price levels on one side, best first.
    pub fn prices(&self, side: Side) -> Vec<i64> {
        match side {
            Side::Buy => self.bids.keys().rev().copied().collect(),
            Side::Sell => self.asks.keys().copied().collect(),
        }
    }

    /// Tell the book about an event it will be asked to apply shortly.
    ///
    /// Only cancels are recorded. A resting order the exchange is about to cancel - within
    /// [`SELF_TRADE_WINDOW_MICROS`] of an incoming order that would otherwise match it - is
    /// withdrawn instead of traded, which is how self-trade prevention shows up in the feed.
    /// Callers that never announce get the plain behaviour: every resting order is matchable.
    pub fn announce(&mut self, ev: &OrderEvent) {
        if ev.activity_type == CANCEL {
            self.scheduled_cancels
                .entry(ev.order_number)
                .or_default()
                .push_back(ev.timestamp);
        }
    }

    /// Did the exchange cancel this resting order at `step` of the sweep that began at `now`?
    ///
    /// Step *k* of a matching event is stamped *k*+1 ticks after the incoming order. A resting
    /// order cancelled at the step where the sweep reached it is a self-trade prevention. The
    /// tolerance is two ticks either side plus rounding: timestamps arrive truncated to whole
    /// microseconds. A client cancelling its own remainder after being filled does so hundreds
    /// of microseconds later and cannot satisfy this, which a fixed time window could not
    /// distinguish.
    fn cancelled_at_step(&self, order_number: u64, now: i64, step: usize) -> bool {
        let expected = (step as f64 + 1.0) * JIFFY_MICROS;
        let tolerance = 0.5 * JIFFY_MICROS + 1.0;
        self.scheduled_cancels.get(&order_number).is_some_and(|ts| {
            ts.iter().any(|&t| {
                let offset = (t - now) as f64;
                offset > 0.0 && (offset - expected).abs() <= tolerance
            })
        })
    }

    /// Stop-loss orders currently held awaiting their trigger.
    pub fn pending_stops(&self) -> usize {
        self.stops.len()
    }

    /// Price of the last fill the book generated, if any.
    pub fn last_trade_price(&self) -> Option<i64> {
        self.last_trade_price
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
        let adjusted = self.closing_session_view(ev);
        let ev = &adjusted;
        let time_of_day = ev.timestamp.rem_euclid(DAY_MICROS);
        if time_of_day < CONTINUOUS_FROM {
            if self.collect_pre_open(ev, time_of_day) {
                self.forced = None;
                return 0;
            }
            // After the auction and before 09:15 nothing matches on arrival.
            if self.forced.is_none() {
                self.forced = Some(Vec::new());
            }
        } else if !self.auction_done {
            self.run_auction();
        }
        match ev.activity_type {
            ENTRY => {
                self.stats.entries += 1;
                if self.stops.remove(&ev.order_number).is_some() {
                    // A second entry record for a waiting stop-loss order is the exchange
                    // reporting its trigger. It enters the book now, with priority from now.
                    self.stats.stops_triggered += 1;
                    let mut live = *ev;
                    live.stop_loss = false;
                    self.enter(&live);
                } else if ev.is_pending_stop() {
                    self.hold_stop(ev);
                } else {
                    self.enter(ev);
                }
            }
            CANCEL => {
                self.stats.cancels += 1;
                if let Some(q) = self.scheduled_cancels.get_mut(&ev.order_number) {
                    q.pop_front();
                    if q.is_empty() {
                        self.scheduled_cancels.remove(&ev.order_number);
                    }
                }
                if self.preempted.remove(&ev.order_number) {
                    // Withdrawn at match time as a self-trade prevention; this is its cancel.
                } else if self.stops.remove(&ev.order_number).is_some() {
                    // A stop cancelled before it triggered: it was never on the book.
                } else if self.remove(ev.order_number).is_some() {
                } else if self.discarded_remainders.remove(&ev.order_number) {
                    self.stats.remainder_cancels += 1;
                } else {
                    self.stats.unknown_order_refs += 1;
                }
            }
            MODIFY => {
                self.stats.modifies += 1;
                let was_waiting = self.stops.remove(&ev.order_number).is_some();
                if ev.is_pending_stop() {
                    // A modify that sets the stop-loss flag leaves - or puts - the order in the
                    // waiting state, off the book, even if it had already triggered.
                    if !was_waiting && self.remove(ev.order_number).is_none() {
                        self.note_unknown_modify(ev.order_number);
                    }
                    self.hold_stop(ev);
                } else if was_waiting {
                    // A modify clearing the flag converts a waiting stop into an ordinary order.
                    self.stats.stops_triggered += 1;
                    self.enter(ev);
                } else if self.amend_in_place(ev) {
                    // Quantity reduced at the same price: keeps its place in the queue.
                } else if self.orders.contains_key(&ev.order_number) {
                    // Anything else is cancel-then-enter: the order goes to the back. An
                    // iceberg keeping its disclosed quantity keeps what is left of its
                    // current tranche; one changing it starts a fresh tranche.
                    self.carry_visible = self.carried_tranche(ev);
                    self.remove(ev.order_number);
                    self.enter(ev);
                } else {
                    self.note_unknown_modify(ev.order_number);
                    self.enter(ev);
                }
            }
            _ => self.stats.events_rejected += 1,
        }
        self.forced = None;
        self.carry_visible = None;
        let tod = ev.timestamp.rem_euclid(DAY_MICROS);
        if (CLOSE_VWAP_FROM..CLOSE_VWAP_TO).contains(&tod) {
            for f in &self.fills {
                self.close_vwap.0 += f.price as i128 * f.quantity as i128;
                self.close_vwap.1 += f.quantity as i128;
            }
        }
        if let Some(f) = self.fills.last() {
            self.last_trade_price = Some(f.price);
        }
        self.fills.len()
    }

    fn hold_stop(&mut self, ev: &OrderEvent) {
        self.stats.stops_held += 1;
        self.stops.insert(ev.order_number, *ev);
    }

    /// A modify naming an order the book does not hold. After a discarded market remainder
    /// it is the exchange converting that remainder to a limit order, which is legitimate.
    fn note_unknown_modify(&mut self, order_number: u64) {
        if !self.discarded_remainders.remove(&order_number) {
            self.stats.unknown_order_refs += 1;
        }
    }

    /// Apply a modify that only reduces a resting order's quantity at an unchanged price and
    /// display size, keeping its place in the queue. Returns false for anything else - a price
    /// change, an increase, a change of disclosed quantity - which is cancel-then-enter.
    fn amend_in_place(&mut self, ev: &OrderEvent) -> bool {
        let Some(o) = self.orders.get(&ev.order_number) else {
            return false;
        };
        if o.side != ev.side || o.price != ev.price || ev.market || ev.volume_original <= 0 {
            return false;
        }
        // What the order shows after the modify. An iceberg keeping its disclosed quantity
        // keeps what is left of its current tranche; one changing it shows a fresh tranche;
        // an ordinary order shows its whole quantity.
        let still_iceberg = ev.volume_disclosed > 0 && ev.volume_disclosed < ev.volume_original;
        let new_remaining = ev.volume_original;
        let carried = carries_tranche(o, ev);
        let (new_left, new_tranche) = if carried {
            (o.tranche_left, ev.volume_disclosed)
        } else if still_iceberg {
            (ev.volume_disclosed, ev.volume_disclosed)
        } else {
            (new_remaining, new_remaining)
        };
        let new_visible = new_left.min(new_remaining);
        // Priority survives unless the displayed quantity grows.
        if new_visible > o.visible {
            return false;
        }

        let (side, price) = (o.side, o.price);
        let new_hidden = new_remaining - new_visible;
        let (d_visible, d_hidden) = (o.visible - new_visible, o.hidden - new_hidden);
        let had_hidden = o.hidden > 0;

        let o = self
            .orders
            .get_mut(&ev.order_number)
            .expect("checked above");
        o.remaining = new_remaining;
        o.visible = new_visible;
        o.hidden = new_hidden;
        o.tranche = new_tranche;
        o.tranche_left = new_left;
        o.is_iceberg = still_iceberg || carried;

        self.resting_hidden -= d_hidden;
        match (had_hidden, new_hidden > 0) {
            (true, false) => self.active_icebergs -= 1,
            (false, true) => self.active_icebergs += 1,
            _ => {}
        }
        if let Some(level) = self.level_mut(side, price) {
            level.visible -= d_visible;
            level.hidden -= d_hidden;
        }
        self.stats.amended_in_place += 1;
        true
    }

    fn enter(&mut self, ev: &OrderEvent) {
        // A market order carries a limit price of zero in the feed. Rejecting it as an
        // unusable price - as every earlier release did - dropped 3.4% of entries, the most
        // aggressive flow in the session, and left the liquidity they consumed in the book.
        if ev.volume_original <= 0 || (ev.price <= 0 && !ev.market) {
            self.stats.events_rejected += 1;
            return;
        }

        // An order keeping its disclosed quantity through a modify stays tranche-managed even
        // when what remains no longer exceeds it.
        let is_iceberg = (ev.volume_disclosed > 0 && ev.volume_original > ev.volume_disclosed)
            || self.carry_visible.is_some();
        let visible = if is_iceberg {
            ev.volume_disclosed
        } else {
            ev.volume_original
        };

        // Match the incoming order against the opposite side while it remains marketable. A
        // market order is marketable at every price, so it is matched with its limit moved to
        // the far end of the book; fills still print at the resting order's price.
        let taking = if ev.market {
            self.stats.market_orders += 1;
            let mut t = *ev;
            t.price = match ev.side {
                Side::Buy => i64::MAX,
                Side::Sell => 0,
            };
            t
        } else {
            *ev
        };
        let mut remaining = ev.volume_original;
        remaining -= self.match_incoming(&taking, remaining);

        if remaining <= 0 {
            return;
        }
        if std::mem::take(&mut self.incoming_withdrawn) {
            // Self-trade prevention cancelled the incoming order; its cancel follows.
            self.stats.unrested_quantity += remaining;
            self.preempted.insert(ev.order_number);
            return;
        }

        // IOC and market orders never rest. The feed confirms this: every unfilled or partly
        // filled IOC is followed by a cancel within tens of microseconds, and fully filled
        // ones never are. Resting the remainder for those microseconds lets later orders
        // trade against liquidity that was already gone.
        if ev.ioc || ev.market {
            self.stats.unrested_quantity += remaining;
            self.discarded_remainders.insert(ev.order_number);
            return;
        }

        // Whatever is left rests. An iceberg's own trades use up its displayed tranche just as
        // trades against it do: it starts with a full tranche (or, re-entered by a modify
        // that kept its disclosed quantity, what was left of its tranche), each trade that
        // takes the whole tranche leaves a fresh full one, and a smaller trade reduces it.
        let tranche = if is_iceberg { visible } else { remaining };
        let tranche_left = if is_iceberg {
            let mut tl = self.carry_visible.take().unwrap_or(tranche);
            let mut left = ev.volume_original;
            for f in self
                .fills
                .iter()
                .filter(|f| f.incoming_order == ev.order_number)
            {
                let vis = tl.min(left);
                left -= f.quantity;
                tl = if f.quantity >= vis {
                    tranche
                } else {
                    tl - f.quantity
                };
            }
            tl
        } else {
            remaining
        };
        self.insert_resting(Resting {
            order_number: ev.order_number,
            side: ev.side,
            price: ev.price,
            remaining,
            tranche,
            tranche_left,
            is_iceberg,
            placed_at: ev.timestamp,
            algo_indicator: ev.algo_indicator,
            client_identity: ev.client_identity,
        });
    }

    /// Put an order at the back of its price's queue.
    fn insert_resting(&mut self, r: Resting) {
        let visible = r.tranche_left.min(r.remaining);
        self.next_seq += 1;
        let seq = self.next_seq;
        let order = BookOrder {
            seq,
            side: r.side,
            price: r.price,
            visible,
            tranche: r.tranche,
            tranche_left: r.tranche_left,
            hidden: r.remaining - visible,
            remaining: r.remaining,
            is_iceberg: r.is_iceberg,
            revealed: false,
            placed_at: r.placed_at,
            algo_indicator: r.algo_indicator,
            client_identity: r.client_identity,
        };

        if order.hidden > 0 {
            self.resting_hidden += order.hidden;
            self.active_icebergs += 1;
        }

        let level = match r.side {
            Side::Buy => self.bids.entry(r.price).or_default(),
            Side::Sell => self.asks.entry(r.price).or_default(),
        };
        level.visible += order.visible;
        level.hidden += order.hidden;
        level.queue.push_back((r.order_number, seq));
        self.orders.insert(r.order_number, order);
    }

    /// Match an incoming order against the resting book. Returns the quantity filled.
    fn match_incoming(&mut self, ev: &OrderEvent, mut remaining: i64) -> i64 {
        self.incoming_withdrawn = false;
        self.print_break = false;
        if let Some(trades) = self.forced.take() {
            return self.match_forced(ev, trades);
        }
        let mut filled = 0i64;
        self.sweep_steps = 0;

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
            if self.incoming_withdrawn {
                filled += taken;
                break;
            }
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

            // Self-trade prevention: the exchange cancelled this resting order at the step of
            // the match that reached it. It must not trade; withdraw it now, as the exchange
            // did. The feed has no client identifier, and the participant category is not one:
            // the same client trades as custodian and non-custodian, and the exchange stops
            // those trading with each other. The cancel's slot, to half a jiffy, is the
            // evidence. Coming back to the order this incoming order is already trading with
            // (its next tranche) continues that trade and cannot be a prevention.
            let continuing = self
                .fills
                .last()
                .is_some_and(|f| f.resting_order == id && f.incoming_order == ev.order_number);
            if !continuing && self.cancelled_at_step(id, ev.timestamp, self.sweep_steps) {
                self.remove(id);
                self.preempted.insert(id);
                self.stats.self_trade_preventions += 1;
                self.sweep_steps += 1;
                // The cancel is a message of its own: whatever trades next is a new record.
                self.print_break = true;
                match self.level_mut(opposite, price) {
                    Some(lvl) => {
                        lvl.queue.pop_front();
                    }
                    None => break,
                }
                continue;
            }
            // Or the exchange cancels the incoming order instead: its own cancel sits in this
            // step's slot. Nothing more of it trades and nothing of it rests.
            if !continuing
                && self.cancelled_at_step(ev.order_number, ev.timestamp, self.sweep_steps)
            {
                self.incoming_withdrawn = true;
                self.stats.self_trade_preventions += 1;
                break;
            }

            let order = self.orders.get(&id).expect("checked live");
            if order.visible <= 0 {
                if order.is_iceberg && order.remaining > 0 {
                    self.reveal_tranche(id, opposite, price, ev.timestamp);
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
            order.tranche_left -= fill;
            order.remaining -= fill;
            let exhausted = order.remaining <= 0;
            let reveal_next = order.visible <= 0 && order.is_iceberg && !exhausted;
            let rest_price = order.price;
            let from_hidden = order.revealed;

            self.level_mut(opposite, price)
                .expect("level exists")
                .visible -= fill;
            taken += fill;
            // Consecutive fills of the same pair are one trade. When an incoming order works
            // through a disclosed-quantity order that is alone at its price, each revealed
            // tranche comes straight back to the front, and the exchange prints the whole
            // execution as a single trade rather than one per tranche. If anything else traded
            // in between, the fills are genuinely separate trades and stay separate.
            let mut continued = false;
            let may_merge = !std::mem::take(&mut self.print_break);
            match self.fills.last_mut() {
                Some(last)
                    if may_merge
                        && last.resting_order == id
                        && last.incoming_order == ev.order_number
                        && last.price == rest_price =>
                {
                    last.quantity += fill;
                    last.from_hidden |= from_hidden;
                    continued = true;
                }
                _ => {
                    self.fills.push(Fill {
                        price: rest_price,
                        quantity: fill,
                        resting_order: id,
                        incoming_order: ev.order_number,
                        from_hidden,
                        timestamp: ev.timestamp,
                        aggressor: ev.side,
                    });
                    self.stats.trades_generated += 1;
                    self.sweep_steps += 1;
                }
            }

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
                self.reveal_tranche(id, opposite, price, ev.timestamp);
                let lvl = self.level_mut(opposite, price).expect("level exists");
                lvl.queue.pop_front();
                lvl.queue.push_back((id, seq));
            } else if continued {
                // This trade used up a whole tranche and went on into the next one. The
                // exchange then shows a fresh full tranche, not what is left of the one it
                // broke into.
                self.refresh_tranche(id, opposite, price);
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
    /// Fill an incoming order from the known executions instead of the matching rules.
    fn match_forced(&mut self, ev: &OrderEvent, trades: Vec<KnownTrade>) -> i64 {
        let mut filled = 0;
        for t in trades {
            if !self.orders.contains_key(&t.resting_order) {
                self.forced_unknown += 1;
                self.forced_shortfall += t.quantity;
                continue;
            }
            // Each known trade is one record of the exchange's: never merged with the last.
            self.print_break = true;
            let got = self.take_resting(
                t.resting_order,
                t.quantity,
                ev.order_number,
                ev.side,
                ev.timestamp,
            );
            self.forced_shortfall += t.quantity - got;
            filled += got;
        }
        self.stats.volume_matched += filled;
        filled
    }

    /// Take up to `want` from one resting order wherever it is queued, revealing tranches as
    /// they run out. Returns the quantity taken.
    fn take_resting(
        &mut self,
        id: u64,
        mut want: i64,
        incoming: u64,
        aggressor: Side,
        ts: i64,
    ) -> i64 {
        let mut taken = 0;
        while want > 0 {
            let Some(o) = self.orders.get_mut(&id) else {
                break;
            };
            let (side, price) = (o.side, o.price);
            if o.visible <= 0 {
                if o.hidden > 0 {
                    self.reveal_tranche(id, side, price, ts);
                    self.requeue_back(id);
                    continue;
                }
                break;
            }
            let fill = want.min(o.visible);
            o.visible -= fill;
            o.tranche_left -= fill;
            o.remaining -= fill;
            let from_hidden = o.revealed;
            let exhausted = o.remaining <= 0;
            let reveal_next = o.visible <= 0 && o.hidden > 0;
            if let Some(l) = self.level_mut(side, price) {
                l.visible -= fill;
            }
            let mut continued = false;
            let may_merge = !std::mem::take(&mut self.print_break);
            match self.fills.last_mut() {
                Some(last)
                    if may_merge
                        && last.resting_order == id
                        && last.incoming_order == incoming
                        && last.price == price =>
                {
                    last.quantity += fill;
                    last.from_hidden |= from_hidden;
                    continued = true;
                }
                _ => {
                    self.fills.push(Fill {
                        price,
                        quantity: fill,
                        resting_order: id,
                        incoming_order: incoming,
                        from_hidden,
                        timestamp: ts,
                        aggressor,
                    });
                    self.stats.trades_generated += 1;
                }
            }
            taken += fill;
            want -= fill;
            if exhausted {
                self.orders.remove(&id);
                if let Some(l) = self.level_mut(side, price) {
                    if l.visible <= 0 && l.hidden <= 0 {
                        self.drop_level(side, price);
                    }
                }
                break;
            }
            if reveal_next {
                self.reveal_tranche(id, side, price, ts);
                self.requeue_back(id);
            } else if continued {
                self.refresh_tranche(id, side, price);
            }
        }
        taken
    }

    /// Show a full tranche again: visible becomes the tranche size (or what remains), taken
    /// from the hidden quantity.
    fn refresh_tranche(&mut self, id: u64, side: Side, price: i64) {
        let Some(o) = self.orders.get_mut(&id) else {
            return;
        };
        if !o.is_iceberg {
            return;
        }
        o.tranche_left = o.tranche;
        let full = o.tranche.min(o.remaining);
        let delta = full - o.visible;
        if delta <= 0 {
            return;
        }
        let had_hidden = o.hidden > 0;
        o.visible = full;
        o.hidden -= delta;
        let now_hidden = o.hidden > 0;
        self.resting_hidden -= delta;
        if had_hidden && !now_hidden {
            self.active_icebergs -= 1;
        }
        if let Some(l) = self.level_mut(side, price) {
            l.visible += delta;
            l.hidden -= delta;
        }
    }

    /// What is left of the current tranche, if a modify re-enters an iceberg that carries its
    /// tranche over (see [`carries_tranche`]).
    fn carried_tranche(&self, ev: &OrderEvent) -> Option<i64> {
        let o = self.orders.get(&ev.order_number)?;
        (carries_tranche(o, ev) && o.tranche_left > 0).then_some(o.tranche_left)
    }

    /// The closing price: the volume-weighted average price of trades from 15:00 to 15:30,
    /// truncated to whole paise and rounded to the nearest tick. Checked against the
    /// post-close trades of 240 security-sessions, all exact. Falls back to the last traded
    /// price when nothing traded in that half hour.
    pub fn closing_price(&self) -> Option<i64> {
        let (pq, q) = self.close_vwap;
        if q > 0 {
            let paise = (pq / q) as i64;
            Some((paise + TICK / 2) / TICK * TICK)
        } else {
            self.last_trade_price
        }
    }

    /// In the post-close session every order trades at the closing price, in time priority,
    /// and a market order waits for a counterparty like any other. The continuous session's
    /// orders take no part: the exchange cancels them as the session opens, and the book
    /// clears them here so the cancels, when they arrive, are recognised.
    fn closing_session_view(&mut self, ev: &OrderEvent) -> OrderEvent {
        if ev.timestamp.rem_euclid(DAY_MICROS) < CLOSING_SESSION_FROM {
            return *ev;
        }
        if self.closing_price.is_none() {
            let Some(price) = self.closing_price() else {
                return *ev;
            };
            self.closing_price = Some(price);
            for (id, _) in self.orders.drain() {
                self.preempted.insert(id);
            }
            self.bids.clear();
            self.asks.clear();
            self.resting_hidden = 0;
            self.active_icebergs = 0;
        }
        let mut e = *ev;
        if matches!(e.activity_type, ENTRY | MODIFY) {
            e.price = self.closing_price.expect("set above");
            e.market = false;
        }
        e
    }

    /// Give an order a new placement at the back of its price's queue.
    fn requeue_back(&mut self, id: u64) {
        self.next_seq += 1;
        let seq = self.next_seq;
        let Some(o) = self.orders.get_mut(&id) else {
            return;
        };
        o.seq = seq;
        let (side, price) = (o.side, o.price);
        if let Some(l) = self.level_mut(side, price) {
            l.queue.push_back((id, seq));
        }
    }

    fn reveal_tranche(&mut self, id: u64, side: Side, price: i64, ts: i64) {
        let Some(order) = self.orders.get_mut(&id) else {
            return;
        };
        let reveal = order.tranche.min(order.remaining);
        if reveal <= 0 {
            return;
        }
        order.tranche_left = order.tranche;
        order.visible = reveal;
        order.hidden = order.remaining - reveal;
        order.revealed = true;
        order.placed_at = ts;
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
            let Some(o) = self.orders.get(&id) else {
                continue;
            };
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
    pub fn opposite(self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

#[cfg(test)]
mod tests {
    /// 10:00 in the continuous session; earlier times belong to the pre-open auction.
    const T0: i64 = 10 * 3600 * 1_000_000;

    use super::*;

    fn ev(
        activity: u8,
        id: u64,
        side: Side,
        price: i64,
        disclosed: i64,
        original: i64,
    ) -> OrderEvent {
        OrderEvent {
            activity_type: activity,
            order_number: id,
            side,
            price,
            volume_disclosed: disclosed,
            volume_original: original,
            timestamp: T0 + id as i64,
            algo_indicator: 1,
            client_identity: 3,
            ioc: false,
            market: false,
            stop_loss: false,
            trigger_price: 0,
        }
    }

    fn limit(id: u64, side: Side, price: i64, qty: i64) -> OrderEvent {
        ev(ENTRY, id, side, price, 0, qty)
    }

    fn market(id: u64, side: Side, qty: i64) -> OrderEvent {
        let mut e = ev(ENTRY, id, side, 0, 0, qty);
        e.market = true;
        e
    }

    fn stop(id: u64, side: Side, limit_price: i64, trigger: i64, qty: i64) -> OrderEvent {
        let mut e = limit(id, side, limit_price, qty);
        e.stop_loss = true;
        e.trigger_price = trigger;
        e
    }

    fn cancel(id: u64, side: Side) -> OrderEvent {
        ev(CANCEL, id, side, 0, 0, 0)
    }

    #[test]
    fn market_order_sweeps_levels_at_resting_prices_and_never_rests() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Sell, 100_00, 10));
        b.apply(&limit(2, Side::Sell, 100_05, 10));
        // Zero-priced in the feed: must trade, not be rejected.
        assert_eq!(b.apply(&market(3, Side::Buy, 25)), 2);
        let prices: Vec<i64> = b.last_fills().iter().map(|f| f.price).collect();
        assert_eq!(prices, vec![100_00, 100_05]);
        assert_eq!(b.stats().volume_matched, 20);
        assert_eq!(b.stats().events_rejected, 0);
        assert_eq!(b.stats().market_orders, 1);
        // The unfilled 5 does not rest anywhere.
        assert_eq!(b.best_bid(), None);
        assert_eq!(b.best_ask(), None);
        assert_eq!(b.stats().unrested_quantity, 5);
    }

    #[test]
    fn market_sell_hits_the_bid() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Buy, 99_95, 10));
        assert_eq!(b.apply(&market(2, Side::Sell, 4)), 1);
        assert_eq!(b.last_fills()[0].price, 99_95);
        assert_eq!(b.best_bid(), Some(99_95));
    }

    #[test]
    fn ioc_remainder_does_not_rest_and_its_cancel_is_recognised() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Sell, 100_00, 10));
        let mut ioc = limit(2, Side::Buy, 100_00, 30);
        ioc.ioc = true;
        b.apply(&ioc);
        assert_eq!(b.stats().volume_matched, 10);
        assert_eq!(b.best_bid(), None, "IOC remainder must not rest");
        // The feed follows with a cancel for the remainder; that is not an unknown order.
        b.apply(&cancel(2, Side::Buy));
        assert_eq!(b.stats().remainder_cancels, 1);
        assert_eq!(b.stats().unknown_order_refs, 0);
    }

    #[test]
    fn stop_loss_waits_for_the_feeds_trigger_record() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Sell, 101_00, 10));
        // Buy stop, trigger 100.50, limit 101.00. Marketable on paper, but it must not trade:
        // it waits off the book until the exchange reports the trigger.
        b.apply(&stop(2, Side::Buy, 101_00, 100_50, 10));
        assert_eq!(b.stats().volume_matched, 0);
        assert_eq!(b.pending_stops(), 1);
        assert_eq!(b.best_bid(), None, "a waiting stop is not on the book");

        // Trades through the trigger price do not release it on their own: the replay does
        // not guess triggers, it follows the feed.
        b.apply(&limit(3, Side::Sell, 100_60, 1));
        b.apply(&limit(4, Side::Buy, 100_60, 1));
        assert_eq!(b.pending_stops(), 1);

        // The feed reports the trigger as a second entry record for the same order number.
        assert_eq!(b.apply(&stop(2, Side::Buy, 101_00, 100_50, 10)), 1);
        assert_eq!(b.pending_stops(), 0);
        assert_eq!(b.stats().stops_triggered, 1);
        assert_eq!(
            b.best_ask(),
            None,
            "the triggered stop lifted the 101.00 offer"
        );
    }

    #[test]
    fn a_modify_clearing_the_stop_flag_converts_it_to_an_ordinary_order() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Buy, 99_00, 10));
        b.apply(&stop(2, Side::Sell, 99_00, 98_50, 10));
        assert_eq!(b.pending_stops(), 1);
        let mut plain = limit(2, Side::Sell, 99_00, 10);
        plain.activity_type = MODIFY;
        assert_eq!(
            b.apply(&plain),
            1,
            "converted, it trades against the 99.00 bid"
        );
        assert_eq!(b.pending_stops(), 0);
    }

    #[test]
    fn a_modify_setting_the_stop_flag_takes_a_live_order_off_the_book() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Buy, 99_00, 10));
        let mut rearm = stop(1, Side::Buy, 99_00, 99_50, 10);
        rearm.activity_type = MODIFY;
        b.apply(&rearm);
        assert_eq!(b.best_bid(), None);
        assert_eq!(b.pending_stops(), 1);
        assert_eq!(b.stats().unknown_order_refs, 0);
    }

    #[test]
    fn cancelling_or_modifying_a_held_stop_does_not_touch_the_book() {
        let mut b = OrderBook::new("TEST");
        b.apply(&stop(1, Side::Buy, 101_00, 100_50, 10));
        let mut moved = stop(1, Side::Buy, 102_00, 101_50, 10);
        moved.activity_type = MODIFY;
        b.apply(&moved);
        assert_eq!(
            b.pending_stops(),
            1,
            "a modified stop stays held under its new trigger"
        );
        b.apply(&cancel(1, Side::Buy));
        assert_eq!(b.pending_stops(), 0);
        assert_eq!(b.stats().unknown_order_refs, 0);
        assert_eq!(b.live_orders(), 0);
    }

    #[test]
    fn a_resting_order_the_exchange_cancels_on_contact_does_not_trade() {
        // Self-trade prevention, as it appears in the feed: a resting buy, an incoming sell
        // that would match it, and the buy's cancel 150 microseconds after the sell arrives.
        let mut b = OrderBook::new("TEST");
        let mut resting = limit(1, Side::Buy, 100_00, 10);
        resting.timestamp = T0 + 1_000;
        let mut other = limit(2, Side::Buy, 100_00, 10);
        other.timestamp = T0 + 1_001;
        let mut incoming = limit(3, Side::Sell, 100_00, 10);
        incoming.timestamp = T0 + 5_000;
        // Stamped one feed tick after the incoming order: the first step of its sweep.
        let mut its_cancel = cancel(1, Side::Buy);
        its_cancel.timestamp = T0 + 5_015;

        for e in [&resting, &other, &incoming, &its_cancel] {
            b.announce(e);
        }
        b.apply(&resting);
        b.apply(&other);
        b.apply(&incoming);
        // It traded with the order behind, not the one being cancelled.
        assert_eq!(b.last_fills().len(), 1);
        assert_eq!(b.last_fills()[0].resting_order, 2);
        assert_eq!(b.stats().self_trade_preventions, 1);

        b.apply(&its_cancel);
        assert_eq!(b.stats().unknown_order_refs, 0);
        assert_eq!(b.live_orders(), 0);
    }

    #[test]
    fn a_cancel_well_after_the_match_does_not_prevent_it() {
        let mut b = OrderBook::new("TEST");
        let mut resting = limit(1, Side::Buy, 100_00, 10);
        resting.timestamp = T0 + 1_000;
        let mut incoming = limit(2, Side::Sell, 100_00, 4);
        incoming.timestamp = T0 + 5_000;
        // A client cancelling its remainder after being filled: 336 microseconds later, well
        // after the sweep, as in the case that exposed the fixed-window rule.
        let mut later = cancel(1, Side::Buy);
        later.timestamp = T0 + 5_336;
        for e in [&resting, &incoming, &later] {
            b.announce(e);
        }
        b.apply(&resting);
        b.apply(&incoming);
        assert_eq!(
            b.last_fills()[0].resting_order,
            1,
            "an ordinary later cancel is not a prevention"
        );
        assert_eq!(b.stats().self_trade_preventions, 0);
    }

    #[test]
    fn inputs_without_order_type_flags_behave_as_before() {
        // Old parquet has no flag columns, so every event arrives with them false. A zero
        // price is then still unusable, as it always was.
        let mut b = OrderBook::new("TEST");
        b.apply(&ev(ENTRY, 1, Side::Buy, 0, 0, 10));
        assert_eq!(b.stats().events_rejected, 1);
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
        assert_eq!(
            b.total_visible(Side::Sell),
            100,
            "only the tranche is visible"
        );
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
        assert_eq!(
            b.total_visible(Side::Sell),
            100,
            "next tranche is now showing"
        );
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
            b.last_fills()[0].resting_order,
            2,
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
        // Alone at its price, each tranche comes straight back to the front: one trade.
        assert_eq!(b.last_fills().len(), 1);
        assert_eq!(b.last_fills()[0].quantity, 500);
        assert!(b.last_fills()[0].from_hidden);
    }

    #[test]
    fn from_hidden_marks_only_revealed_tranches() {
        let mut b = OrderBook::new("TEST");
        b.apply(&ev(ENTRY, 1, Side::Sell, 100_00, 100, 300));
        b.apply(&limit(2, Side::Buy, 100_00, 100));
        assert!(
            !b.last_fills()[0].from_hidden,
            "the first tranche was displayed"
        );
        b.apply(&limit(3, Side::Buy, 100_00, 50));
        assert!(
            b.last_fills()[0].from_hidden,
            "the second tranche was hidden at entry"
        );
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
    fn a_price_change_loses_queue_position() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Sell, 100_00, 10));
        b.apply(&limit(2, Side::Sell, 100_05, 10));
        b.apply(&limit(3, Side::Sell, 100_00, 10));
        // Order 2 reprices to 100.00: it is removed and re-entered, behind order 3.
        b.apply(&ev(MODIFY, 2, Side::Sell, 100_00, 0, 10));
        b.apply(&limit(4, Side::Buy, 100_00, 30));
        let order: Vec<u64> = b.last_fills().iter().map(|f| f.resting_order).collect();
        assert_eq!(order, vec![1, 3, 2]);
        assert_eq!(b.stats().amended_in_place, 0);
    }

    #[test]
    fn a_quantity_increase_loses_queue_position() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Sell, 100_00, 10));
        b.apply(&limit(2, Side::Sell, 100_00, 10));
        b.apply(&ev(MODIFY, 1, Side::Sell, 100_00, 0, 15));
        b.apply(&limit(3, Side::Buy, 100_00, 10));
        assert_eq!(b.last_fills()[0].resting_order, 2);
    }

    #[test]
    fn a_quantity_reduction_keeps_queue_position() {
        // Confirmed against the trade file: keeping priority on a same-price reduction
        // reproduces more of the exchange's actual trades on every security tested.
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Sell, 100_00, 10));
        b.apply(&limit(2, Side::Sell, 100_00, 10));
        b.apply(&ev(MODIFY, 1, Side::Sell, 100_00, 0, 4));
        assert_eq!(b.total_visible(Side::Sell), 14);
        b.apply(&limit(3, Side::Buy, 100_00, 6));
        let fills: Vec<(u64, i64)> = b
            .last_fills()
            .iter()
            .map(|f| (f.resting_order, f.quantity))
            .collect();
        assert_eq!(fills, vec![(1, 4), (2, 2)]);
        assert_eq!(b.stats().amended_in_place, 1);
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
        assert_eq!(
            bids,
            vec![100_00, 99_00, 98_00],
            "bids descend from the best"
        );
        assert_eq!(
            asks,
            vec![101_00, 102_00, 103_00],
            "asks ascend from the best"
        );
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
        assert!(
            !b.is_crossed(),
            "bid {:?} ask {:?}",
            b.best_bid(),
            b.best_ask()
        );
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

    fn at(h: i64, m: i64, sec: i64) -> i64 {
        ((h * 60 + m) * 60 + sec) * 1_000_000
    }

    fn timed(mut e: OrderEvent, ts: i64) -> OrderEvent {
        e.timestamp = ts;
        e
    }

    fn client(mut e: OrderEvent, algo: u8, client: u8) -> OrderEvent {
        e.algo_indicator = algo;
        e.client_identity = client;
        e
    }

    #[test]
    fn an_iceberg_shows_a_fresh_tranche_after_a_trade_runs_into_its_next_one() {
        let mut b = OrderBook::new("TEST");
        b.apply(&ev(ENTRY, 1, Side::Sell, 100_00, 10, 100));
        b.apply(&limit(2, Side::Buy, 100_00, 15)); // 10, then 5 of the next tranche
        assert_eq!(b.last_fills().len(), 1, "one trade record");
        assert_eq!(b.last_fills()[0].quantity, 15);
        assert_eq!(
            b.top_levels(Side::Sell, 1)[0],
            (100_00, 10, 75),
            "a full tranche shows, not the 5 left of the one it broke into"
        );
    }

    #[test]
    fn an_icebergs_own_trades_on_entry_use_up_its_tranche() {
        let mut b = OrderBook::new("TEST");
        b.apply(&limit(1, Side::Sell, 100_00, 3));
        b.apply(&ev(ENTRY, 2, Side::Buy, 100_00, 10, 50));
        assert_eq!(b.top_levels(Side::Buy, 1)[0], (100_00, 7, 40));
    }

    #[test]
    fn a_modify_keeping_the_disclosed_quantity_keeps_what_is_left_of_the_tranche() {
        let mut b = OrderBook::new("TEST");
        b.apply(&ev(ENTRY, 1, Side::Sell, 100_05, 10, 100));
        b.apply(&limit(2, Side::Buy, 100_05, 4));
        b.apply(&ev(MODIFY, 1, Side::Sell, 100_10, 10, 96));
        assert_eq!(b.top_levels(Side::Sell, 1)[0], (100_10, 6, 90));
    }

    #[test]
    fn a_modify_changing_the_disclosed_quantity_shows_a_fresh_tranche() {
        let mut b = OrderBook::new("TEST");
        b.apply(&ev(ENTRY, 1, Side::Sell, 100_05, 10, 100));
        b.apply(&limit(2, Side::Buy, 100_05, 4));
        b.apply(&ev(MODIFY, 1, Side::Sell, 100_10, 20, 96));
        assert_eq!(b.top_levels(Side::Sell, 1)[0], (100_10, 20, 76));
    }

    #[test]
    fn priority_survives_a_modify_that_does_not_grow_the_display() {
        let mut b = OrderBook::new("TEST");
        b.apply(&ev(ENTRY, 1, Side::Sell, 100_00, 10, 100));
        b.apply(&limit(2, Side::Sell, 100_00, 10));
        b.apply(&ev(MODIFY, 1, Side::Sell, 100_00, 5, 100)); // shows 5 instead of 10
        b.apply(&limit(3, Side::Buy, 100_00, 1));
        assert_eq!(
            b.last_fills()[0].resting_order,
            1,
            "still first in the queue"
        );

        b.apply(&ev(MODIFY, 1, Side::Sell, 100_00, 20, 99)); // shows 20: grows
        b.apply(&limit(4, Side::Buy, 100_00, 1));
        assert_eq!(b.last_fills()[0].resting_order, 2, "now behind order 2");
    }

    #[test]
    fn self_trade_prevention_can_cancel_the_incoming_order() {
        let mut b = OrderBook::new("TEST");
        b.apply(&timed(
            client(limit(1, Side::Sell, 100_00, 10), 0, 2),
            T0 + 1_000,
        ));
        let incoming = timed(client(limit(2, Side::Buy, 100_00, 5), 1, 2), T0 + 5_000);
        // The exchange cancels the incoming buy one tick after it arrives.
        let its_cancel = timed(cancel(2, Side::Buy), T0 + 5_015);
        b.announce(&its_cancel);
        b.apply(&incoming);
        assert!(b.last_fills().is_empty(), "the orders must not trade");
        assert_eq!(b.best_bid(), None, "and the buy does not rest");
        assert_eq!(b.best_ask(), Some(100_00), "the resting sell is untouched");
        b.apply(&its_cancel);
        assert_eq!(b.stats().unknown_order_refs, 0, "its cancel is recognised");
    }

    #[test]
    fn a_self_trade_cancel_ends_the_trade_record() {
        let mut b = OrderBook::new("TEST");
        b.apply(&timed(ev(ENTRY, 1, Side::Sell, 100_00, 5, 20), T0 + 1_000));
        b.apply(&timed(
            client(limit(2, Side::Sell, 100_00, 5), 1, 2),
            T0 + 1_001,
        ));
        // Buy 7: takes order 1's tranche (step 0), meets order 2 of its own client (step 1,
        // cancelled by the exchange), then order 1 again.
        let incoming = timed(client(limit(3, Side::Buy, 100_00, 7), 1, 2), T0 + 5_000);
        b.announce(&timed(cancel(2, Side::Sell), T0 + 5_030));
        b.apply(&incoming);
        let q: Vec<(u64, i64)> = b
            .last_fills()
            .iter()
            .map(|f| (f.resting_order, f.quantity))
            .collect();
        assert_eq!(q, vec![(1, 5), (1, 2)], "two records, split by the cancel");
        assert_eq!(
            b.top_levels(Side::Sell, 1)[0],
            (100_00, 3, 10),
            "the second record is an ordinary trade: 3 of the tranche left"
        );
    }

    #[test]
    fn the_post_close_session_trades_at_the_closing_price_in_time_order() {
        let mut b = OrderBook::new("TEST");
        let t = at(15, 10, 0);
        b.apply(&timed(limit(1, Side::Sell, 100_00, 10), t));
        b.apply(&timed(limit(2, Side::Buy, 100_00, 10), t + 1));
        b.apply(&timed(limit(3, Side::Sell, 100_07, 10), t + 2));
        b.apply(&timed(limit(4, Side::Buy, 100_07, 10), t + 3));
        // Average 100.035: truncated to 100.03, then the nearest tick, 100.05.
        assert_eq!(b.closing_price(), Some(100_05));

        b.apply(&timed(limit(5, Side::Sell, 101_00, 50), t + 4)); // a day order, left over
        b.apply(&timed(market(6, Side::Buy, 3), at(15, 40, 1)));
        assert!(b.last_fills().is_empty(), "the day's orders take no part");
        assert_eq!(
            b.best_bid(),
            Some(100_05),
            "a market order waits at the close"
        );
        b.apply(&timed(market(7, Side::Sell, 2), at(15, 40, 2)));
        let f = b.last_fills()[0];
        assert_eq!((f.buy_order(), f.price, f.quantity), (6, 100_05, 2));
        b.apply(&timed(cancel(5, Side::Sell), at(15, 40, 3)));
        assert_eq!(b.stats().unknown_order_refs, 0);
    }

    #[test]
    fn the_pre_open_auction_matches_at_one_price_in_priority_order() {
        let mut b = OrderBook::new("TEST");
        let t = at(9, 1, 0);
        b.apply(&timed(limit(1, Side::Buy, 101_00, 10), t));
        b.apply(&timed(market(2, Side::Buy, 5), t + 1));
        b.apply(&timed(limit(3, Side::Sell, 99_00, 8), t + 2));
        b.apply(&timed(limit(4, Side::Sell, 100_00, 10), t + 3));
        b.apply(&timed(limit(5, Side::Buy, 100_00, 3), t + 4));
        b.apply(&timed(limit(6, Side::Buy, 98_00, 7), t + 5));
        assert_eq!(
            b.best_bid(),
            None,
            "nothing matches or rests before the auction"
        );

        b.close_pre_open();
        let trades: Vec<(u64, u64, i64, i64)> = b
            .last_fills()
            .iter()
            .map(|f| (f.buy_order(), f.sell_order(), f.quantity, f.price))
            .collect();
        // 18 can trade at 100.00, more than at any other price. Limit buys by price, then
        // the market buy; sells by price.
        assert_eq!(
            trades,
            vec![
                (1, 3, 8, 100_00),
                (1, 4, 2, 100_00),
                (5, 4, 3, 100_00),
                (2, 4, 5, 100_00),
            ]
        );
        assert_eq!(b.best_bid(), Some(98_00), "the unmatched buy rests");
        assert_eq!(b.best_ask(), None);
    }

    #[test]
    fn a_pre_open_modify_that_does_not_raise_quantity_keeps_time_priority() {
        let mut b = OrderBook::new("TEST");
        let t = at(9, 1, 0);
        b.apply(&timed(limit(1, Side::Buy, 100_00, 10), t));
        b.apply(&timed(limit(2, Side::Buy, 100_00, 10), t + 1));
        b.apply(&timed(ev(MODIFY, 1, Side::Buy, 100_00, 0, 8), t + 2));
        b.apply(&timed(limit(3, Side::Sell, 100_00, 1), t + 3));
        b.close_pre_open();
        assert_eq!(b.last_fills()[0].buy_order(), 1);
    }
}
