//! Replay one security against the exchange's trade file, event by event, and report every
//! event where the matching rules trade differently from the exchange.
//!
//! Two books run in lockstep: one by the matching rules, one driven by the exchange's own
//! trade records. Wherever they trade differently, the event is written out with the queue as
//! it stood, and the rules book is reset from the other. So every reported divergence is a
//! root cause, not the knock-on effect of an earlier one: the way to find the next rule to
//! fix. The pre-open auction's trades are compared separately and reported on stderr.
//!
//!     python tools/label_trades.py PARSED_ROOT 25012022 TCS labelled.csv
//!     cargo run --release -p nsetick-book --example oracle_diff -- //!         PARSED_ROOT/cash_orders/date=25012022/symbol=TCS/part-000.parquet labelled.csv out.csv
//!
//! `STOP_AFTER=n` stops after the first n divergences.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};

use anyhow::{Context, Result};
use nsetick_book::book::{KnownTrade, OrderBook, OrderEvent, Side, CANCEL, ENTRY, MODIFY};
use nsetick_book::replay::events_by_symbol;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// 09:15 IST as a time of day in microseconds; the feed's timestamps are IST wall clock.
const CONTINUOUS_FROM: i64 = (9 * 3600 + 15 * 60) * 1_000_000;
const DAY: i64 = 86_400 * 1_000_000;

fn fmt(trades: &[KnownTrade]) -> String {
    trades
        .iter()
        .map(|t| format!("{}:{}:{}", t.resting_order, t.price, t.quantity))
        .collect::<Vec<_>>()
        .join("|")
}

fn crosses(book: &OrderBook, ev: &OrderEvent) -> bool {
    if ev.activity_type != ENTRY && ev.activity_type != MODIFY {
        return false;
    }
    match ev.side {
        Side::Buy => book.best_ask().is_some_and(|a| ev.market || ev.price >= a),
        Side::Sell => book.best_bid().is_some_and(|b| ev.market || ev.price <= b),
    }
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [orders, actual, out] = &args[..] else {
        anyhow::bail!("usage: oracle_diff <orders.parquet> <actual.csv> <out.csv>");
    };

    let mut events: Vec<OrderEvent> = Vec::new();
    let mut symbol = String::new();
    for batch in ParquetRecordBatchReaderBuilder::try_new(File::open(orders)?)?
        .with_batch_size(64 * 1024)
        .build()?
    {
        for (s, evs) in events_by_symbol(&batch?)? {
            symbol = s;
            events.extend(evs);
        }
    }

    let mut continuous: HashMap<(u64, i64), Vec<KnownTrade>> = HashMap::new();
    let mut auction: Vec<(u64, u64, i64, i64)> = Vec::new();
    let mut auction_price: Option<i64> = None;
    for line in BufReader::new(File::open(actual)?).lines() {
        let line = line?;
        let f: Vec<&str> = line.split(',').collect();
        let num = |i: usize| -> Result<i64> {
            f[i].parse::<i64>()
                .with_context(|| format!("field {i} of {line}"))
        };
        match f[0] {
            "C" => continuous
                .entry((num(2)? as u64, num(1)?))
                .or_default()
                .push(KnownTrade {
                    resting_order: num(3)? as u64,
                    price: num(4)?,
                    quantity: num(5)?,
                }),
            "A" => {
                auction.push((num(8)? as u64, num(9)? as u64, num(5)?, num(7)?));
                auction_price = Some(num(4)?);
            }
            _ => {}
        }
    }

    // Two books in lockstep: `truth` is always driven by the exchange's executions, `rules`
    // by the matching rules. When they trade differently, `rules` is reset from `truth`, so
    // every divergence reported is a root cause rather than a consequence of an earlier one.
    let mut truth = OrderBook::new(symbol.clone());
    let mut rules: Option<OrderBook> = None;
    let window = nsetick_book::book::SELF_TRADE_WINDOW_MICROS;
    let mut announced = 0usize;
    let stop_after: usize = std::env::var("STOP_AFTER")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(usize::MAX);

    let mut w = BufWriter::new(File::create(out)?);
    writeln!(
        w,
        "ts,order,side,act,market,ioc,stop,price,volume,disclosed,kind,first_diff,ours,actual,queue_price,queue"
    )?;

    let (mut checked, mut agreed, mut diverged) = (0u64, 0u64, 0u64);
    let mut auction_done = false;
    let mut auction_fills: Vec<nsetick_book::book::Fill> = Vec::new();
    for ev in &events {
        while announced < events.len() && events[announced].timestamp <= ev.timestamp + window {
            truth.announce(&events[announced]);
            if let Some(r) = rules.as_mut() {
                r.announce(&events[announced]);
            }
            announced += 1;
        }
        if ev.timestamp.rem_euclid(DAY) < CONTINUOUS_FROM {
            // The pre-open session: the engine collects orders and runs the call auction.
            truth.apply(ev);
            auction_fills.extend(truth.last_fills().iter().copied());
            continue;
        }
        if !auction_done {
            truth.close_pre_open();
            auction_fills.extend(truth.last_fills().iter().copied());
            auction_done = true;
            let ours: Vec<(u64, u64, i64, i64)> = auction_fills
                .iter()
                .map(|f| (f.buy_order(), f.sell_order(), f.quantity, f.price))
                .collect();
            let exact = ours.len() == auction.len()
                && ours
                    .iter()
                    .zip(&auction)
                    .all(|(o, a)| o.0 == a.0 && o.1 == a.1 && o.2 == a.2);
            let price_ok = ours.first().map(|o| o.3) == auction_price;
            eprintln!(
                "{symbol}: auction {} trades vs exchange {}; pairs and quantities {}; price {}",
                ours.len(),
                auction.len(),
                if exact { "identical" } else { "DIFFER" },
                if price_ok {
                    "identical"
                } else {
                    "DIFFERS (tie: needs previous close)"
                }
            );
        }
        let r = rules.get_or_insert_with(|| truth.clone());

        // Compared record for record: the exchange's trade records as printed.
        let actual = continuous
            .remove(&(ev.order_number, ev.timestamp))
            .unwrap_or_default();
        let trading = !actual.is_empty() || crosses(&truth, ev);

        // Context for a report, taken before either book changes: the queue at every price
        // either side might trade at.
        let opposite = ev.side.opposite();
        let context: Vec<(i64, String)> = if trading {
            let mut prices: Vec<i64> = actual.iter().map(|t| t.price).collect();
            prices.extend(truth.prices(opposite).into_iter().take(3));
            prices.dedup();
            prices
                .into_iter()
                .map(|p| {
                    let q = truth
                        .queue(opposite, p)
                        .iter()
                        .map(|q| {
                            format!(
                                "{}:{}:{}:{}:{}:{}:{}{}",
                                q.order_number,
                                q.visible,
                                q.hidden,
                                q.is_iceberg as u8,
                                q.revealed as u8,
                                q.placed_at,
                                q.algo_indicator,
                                q.client_identity
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("|");
                    (p, q)
                })
                .collect()
        } else {
            Vec::new()
        };

        r.apply(ev);
        let ours: Vec<KnownTrade> = r
            .last_fills()
            .iter()
            .map(|f| KnownTrade {
                resting_order: f.resting_order,
                price: f.price,
                quantity: f.quantity,
            })
            .collect();
        truth.force_next_match(actual.clone());
        truth.apply(ev);
        if !trading {
            continue;
        }
        checked += 1;
        if ours == actual {
            agreed += 1;
            continue;
        }
        diverged += 1;

        let i = ours
            .iter()
            .zip(&actual)
            .position(|(a, b)| a != b)
            .unwrap_or(ours.len().min(actual.len()));
        let in_queue = |t: &KnownTrade| {
            context
                .iter()
                .any(|(p, q)| *p == t.price && q.contains(&format!("{}:", t.resting_order)))
        };
        let kind = if actual.is_empty() {
            "we_traded_exchange_did_not"
        } else if ours.is_empty() {
            "exchange_traded_we_did_not"
        } else if !actual.iter().all(in_queue) {
            "actual_resting_order_not_in_our_queue"
        } else {
            "different_allocation"
        };
        let qp = actual.get(i).or(ours.get(i)).map_or(0, |t| t.price);
        let queue = context
            .iter()
            .find(|(p, _)| *p == qp)
            .map_or(String::new(), |(_, q)| q.clone());
        writeln!(
            w,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            ev.timestamp,
            ev.order_number,
            if ev.side == Side::Buy { "B" } else { "S" },
            ev.activity_type,
            ev.market as u8,
            ev.ioc as u8,
            ev.stop_loss as u8,
            ev.price,
            ev.volume_original,
            ev.volume_disclosed,
            kind,
            i,
            fmt(&ours),
            fmt(&actual),
            qp,
            queue
        )?;
        if diverged as usize >= stop_after {
            break;
        }
        rules = Some(truth.clone());
    }
    let unused: usize = continuous.values().map(|v| v.len()).sum();
    let _ = CANCEL;

    let (unknown, shortfall) = truth.forced_misses();
    eprintln!(
        "{symbol}: {checked} trading events checked, {agreed} agree, {diverged} diverge ({:.3}%); \
         actual trades never matched to an event: {unused}; forced trades naming unknown orders: {unknown}, \
         shortfall {shortfall}",
        100.0 * diverged as f64 / checked.max(1) as f64
    );
    Ok(())
}
