//! The pre-open call auction.
//!
//! From 09:00 NSE collects orders without matching them. The session closes at a random moment
//! between 09:07 and 09:08, and every order is then matched at one price, the equilibrium
//! price. Continuous trading begins at 09:15 from the book the auction leaves behind. Each
//! rule here reproduces the exchange's auction trades exactly, trade by trade, in all 240
//! security-sessions examined (90,736 trades):
//!
//! * **Equilibrium price.** The limit price at which the most quantity can trade; among
//!   those, the one leaving the smallest imbalance between buying and selling; among those,
//!   the one closest to the previous close. The previous close is not in a day's files, but
//!   the feed usually settles the tie anyway: the exchange converts each unfilled market order
//!   into a limit order *at the equilibrium price* moments after the auction, and the book
//!   takes the price from the first such conversion. Only when there is no conversion to read
//!   is the previous close needed ([`OrderBook::set_previous_close`]); without it the lowest
//!   candidate is taken.
//! * **Who trades.** Buy orders at or above the equilibrium price and sell orders at or below
//!   it, plus every market order.
//! * **In what order.** Buys best price first, sells best price first. Market orders come
//!   after every limit order, effectively at the equilibrium price behind the limit orders
//!   there. Within a price, earlier orders first. The two queues are then paired off in order,
//!   each trade as large as the smaller of the two remainders.
//! * **Time priority through a modify** follows the same principle as the continuous session:
//!   an order keeps its time if the modify leaves its price (and market flag) unchanged and
//!   does not raise its quantity. Otherwise its time becomes the time of the modify.
//! * **What is left** rests in the continuous book at its limit, in priority order. An
//!   unfilled market order does not rest: the exchange converts it to a limit order at the
//!   equilibrium price with a modify, written to the feed moments after the auction.

use std::collections::HashMap;

use super::{Fill, OrderBook, OrderEvent, Side, CANCEL, ENTRY, MODIFY};

/// An order collected during the pre-open session.
#[derive(Debug, Clone, Copy)]
pub(super) struct PreOrder {
    order_number: u64,
    side: Side,
    price: i64,
    volume: i64,
    disclosed: i64,
    market: bool,
    /// Time priority: entry, or the last modify that lost priority.
    priority: i64,
    algo_indicator: u8,
    client_identity: u8,
}

impl OrderBook {
    /// Previous session's closing price, used only to break a tie between equally good
    /// auction prices.
    pub fn set_previous_close(&mut self, price: Option<i64>) {
        self.previous_close = price;
    }

    /// Run the call auction now if it has not run, as when the feed moves on to the
    /// continuous session. Its trades are then the book's [`OrderBook::last_fills`].
    pub fn close_pre_open(&mut self) -> usize {
        self.fills.clear();
        if !self.auction_done {
            self.run_auction();
        }
        self.fills.len()
    }

    /// Is this modify the exchange converting an unfilled market order after the auction?
    /// Order entry closes at a random moment between 09:07 and 09:08, and the feed does not
    /// mark it. A trader may also turn a market order into a limit order before the close, so
    /// the test is exact: the auction as it would run now must price at this modify's limit
    /// and leave exactly this modify's quantity of the order unfilled.
    fn is_conversion(&self, ev: &OrderEvent, time_of_day: i64) -> bool {
        if ev.activity_type != MODIFY || ev.market || time_of_day < super::tod(9, 7) {
            return false;
        }
        if !self
            .pre_open
            .get(&ev.order_number)
            .is_some_and(|p| p.market)
        {
            return false;
        }
        let orders = self.pre_open_sorted();
        if !self.equilibrium_candidates(&orders).contains(&ev.price) {
            return false;
        }
        let plan = self.plan_auction(&orders, Some(ev.price));
        plan.left.get(&ev.order_number) == Some(&ev.volume_original)
    }

    /// Collect a pre-open event. Returns false once the auction has run, or when this event
    /// shows it has happened, in which case the auction is run first and the event is left
    /// for normal processing.
    pub(super) fn collect_pre_open(&mut self, ev: &OrderEvent, time_of_day: i64) -> bool {
        if self.auction_done {
            return false;
        }
        if self.is_conversion(ev, time_of_day) {
            // The conversion's limit is the equilibrium price the exchange chose, which also
            // settles a tie between equally good prices.
            self.run_auction_at(Some(ev.price));
            return false;
        }
        if time_of_day >= super::tod(9, 8) {
            self.run_auction();
            return false;
        }
        self.pre_open_last = self.pre_open_last.max(ev.timestamp);
        match ev.activity_type {
            ENTRY => {
                self.stats.entries += 1;
                self.pre_open.insert(
                    ev.order_number,
                    PreOrder {
                        order_number: ev.order_number,
                        side: ev.side,
                        price: ev.price,
                        volume: ev.volume_original,
                        disclosed: ev.volume_disclosed,
                        market: ev.market,
                        priority: ev.timestamp,
                        algo_indicator: ev.algo_indicator,
                        client_identity: ev.client_identity,
                    },
                );
            }
            MODIFY => {
                self.stats.modifies += 1;
                match self.pre_open.get_mut(&ev.order_number) {
                    Some(p) => {
                        let keeps = ev.price == p.price
                            && ev.market == p.market
                            && ev.volume_original <= p.volume;
                        if !keeps {
                            p.priority = ev.timestamp;
                        }
                        p.price = ev.price;
                        p.volume = ev.volume_original;
                        p.disclosed = ev.volume_disclosed;
                        p.market = ev.market;
                    }
                    None => self.stats.unknown_order_refs += 1,
                }
            }
            CANCEL => {
                self.stats.cancels += 1;
                if self.pre_open.remove(&ev.order_number).is_none() {
                    self.stats.unknown_order_refs += 1;
                }
            }
            _ => self.stats.events_rejected += 1,
        }
        true
    }

    /// The equilibrium price for these orders, if anything can trade.
    fn equilibrium(&self, orders: &[PreOrder]) -> Option<i64> {
        let candidates = self.equilibrium_candidates(orders);
        match self.previous_close {
            Some(pc) => candidates.into_iter().min_by_key(|&p| ((p - pc).abs(), p)),
            None => candidates.into_iter().min(),
        }
    }

    /// Every price that trades the most and leaves the smallest imbalance. Usually one.
    fn equilibrium_candidates(&self, orders: &[PreOrder]) -> Vec<i64> {
        let mut prices: Vec<i64> = orders
            .iter()
            .filter(|o| !o.market && o.price > 0)
            .map(|o| o.price)
            .collect();
        prices.sort_unstable();
        prices.dedup();
        let at = |p: i64| {
            let demand: i64 = orders
                .iter()
                .filter(|o| o.side == Side::Buy && (o.market || o.price >= p))
                .map(|o| o.volume)
                .sum();
            let supply: i64 = orders
                .iter()
                .filter(|o| o.side == Side::Sell && (o.market || o.price <= p))
                .map(|o| o.volume)
                .sum();
            (demand.min(supply), (demand - supply).abs())
        };
        let scored: Vec<(i64, i64, i64)> = prices
            .into_iter()
            .map(|p| {
                let (v, imb) = at(p);
                (p, v, imb)
            })
            .filter(|&(_, v, _)| v > 0)
            .collect();
        let Some(best_volume) = scored.iter().map(|s| s.1).max() else {
            return Vec::new();
        };
        let best_imbalance = scored
            .iter()
            .filter(|s| s.1 == best_volume)
            .map(|s| s.2)
            .min()
            .unwrap_or(0);
        scored
            .iter()
            .filter(|s| s.1 == best_volume && s.2 == best_imbalance)
            .map(|s| s.0)
            .collect()
    }

    fn pre_open_sorted(&self) -> Vec<PreOrder> {
        let mut orders: Vec<PreOrder> = self.pre_open.values().copied().collect();
        orders.sort_by_key(|o| (o.priority, o.order_number));
        orders
    }

    /// The auction's price, trades and what each order has left, without changing the book.
    fn plan_auction(&self, orders: &[PreOrder], price: Option<i64>) -> AuctionPlan {
        let mut left: HashMap<u64, i64> =
            orders.iter().map(|o| (o.order_number, o.volume)).collect();
        let mut trades = Vec::new();
        let price = price.or_else(|| self.equilibrium(orders));
        if let Some(eq) = price {
            let mut buys: Vec<&PreOrder> = orders
                .iter()
                .filter(|o| o.side == Side::Buy && (o.market || o.price >= eq))
                .collect();
            let mut sells: Vec<&PreOrder> = orders
                .iter()
                .filter(|o| o.side == Side::Sell && (o.market || o.price <= eq))
                .collect();
            buys.sort_by_key(|o| (o.market, -o.price, o.priority, o.order_number));
            sells.sort_by_key(|o| (o.market, o.price, o.priority, o.order_number));
            let (mut i, mut j) = (0, 0);
            while i < buys.len() && j < sells.len() {
                let (b, s) = (buys[i].order_number, sells[j].order_number);
                let q = left[&b].min(left[&s]);
                *left.get_mut(&b).expect("present") -= q;
                *left.get_mut(&s).expect("present") -= q;
                trades.push((b, s, q));
                if left[&b] == 0 {
                    i += 1;
                }
                if left[&s] == 0 {
                    j += 1;
                }
            }
        }
        AuctionPlan {
            price,
            trades,
            left,
        }
    }

    /// Run the call auction on the collected orders and hand what is left to the continuous
    /// book. Auction trades are reported as fills of the current event.
    pub(super) fn run_auction(&mut self) {
        self.run_auction_at(None);
    }

    /// Run the auction at a price already known to be the exchange's, or the computed one.
    fn run_auction_at(&mut self, price: Option<i64>) {
        self.auction_done = true;
        let orders = self.pre_open_sorted();
        self.pre_open.clear();
        if orders.is_empty() {
            return;
        }
        let plan = self.plan_auction(&orders, price);
        if let Some(eq) = plan.price {
            for &(b, s, q) in &plan.trades {
                self.fills.push(Fill {
                    price: eq,
                    quantity: q,
                    resting_order: s,
                    incoming_order: b,
                    from_hidden: false,
                    timestamp: self.pre_open_last,
                    aggressor: Side::Buy,
                });
                self.stats.trades_generated += 1;
                self.stats.volume_matched += q;
            }
        }

        // Leftovers join the continuous book in priority order; market orders wait for the
        // exchange's conversion.
        for o in &orders {
            let remaining = plan.left[&o.order_number];
            if remaining <= 0 {
                continue;
            }
            if o.market || o.price <= 0 {
                self.discarded_remainders.insert(o.order_number);
                continue;
            }
            let is_iceberg = o.disclosed > 0 && o.disclosed < remaining;
            let tranche = if is_iceberg { o.disclosed } else { remaining };
            self.insert_resting(super::Resting {
                order_number: o.order_number,
                side: o.side,
                price: o.price,
                remaining,
                tranche,
                tranche_left: tranche,
                is_iceberg,
                placed_at: o.priority,
                algo_indicator: o.algo_indicator,
                client_identity: o.client_identity,
            });
        }
    }
}

/// What the auction would do with the orders collected so far.
struct AuctionPlan {
    price: Option<i64>,
    trades: Vec<(u64, u64, i64)>,
    left: HashMap<u64, i64>,
}
