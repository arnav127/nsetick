//! Replay one symbol's parsed orders and write every fill the book generates as CSV.
//!
//! A diagnostic for checking the replay against the exchange's trade file: the trade file
//! names the order number on each side of every execution, so joining the two on order number
//! shows exactly which orders the replay fills differently from the exchange.
//!
//!     cargo run --release -p nsetick-book --example dump_fills -- \
//!         data/parsed/cash_orders/date=25012022/symbol=RELIANCE/part-0.parquet fills.csv

use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufWriter, Write};

use anyhow::{Context, Result};
use nsetick_book::replay::events_by_symbol;
use nsetick_book::{OrderBook, OrderEvent};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let input = args
        .next()
        .context("usage: dump_fills <orders.parquet> <fills.csv>")?;
    let output = args
        .next()
        .context("usage: dump_fills <orders.parquet> <fills.csv>")?;

    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&input)?)?
        .with_batch_size(64 * 1024)
        .build()?;

    let mut out = BufWriter::new(File::create(&output)?);
    writeln!(
        out,
        "symbol,timestamp,price,quantity,incoming_order,resting_order,from_hidden"
    )?;

    // Same lookahead as the replay itself: announce each event on reading, apply it once the
    // stream has moved the self-trade window past it.
    let window = nsetick_book::book::SELF_TRADE_WINDOW_MICROS;
    let mut books: std::collections::HashMap<String, (OrderBook, VecDeque<OrderEvent>)> =
        Default::default();
    let emit = |sym: &str,
                book: &mut OrderBook,
                ev: &OrderEvent,
                out: &mut BufWriter<File>|
     -> Result<()> {
        book.apply(ev);
        for f in book.last_fills() {
            writeln!(
                out,
                "{sym},{},{},{},{},{},{}",
                f.timestamp, f.price, f.quantity, f.incoming_order, f.resting_order, f.from_hidden
            )?;
        }
        Ok(())
    };
    for batch in reader {
        for (sym, events) in events_by_symbol(&batch?)? {
            let (book, pending) = books
                .entry(sym.clone())
                .or_insert_with(|| (OrderBook::new(sym.clone()), VecDeque::new()));
            for ev in &events {
                book.announce(ev);
                pending.push_back(*ev);
                while pending
                    .front()
                    .is_some_and(|f| f.timestamp + window < ev.timestamp)
                {
                    let next = pending.pop_front().expect("front exists");
                    emit(&sym, book, &next, &mut out)?;
                }
            }
        }
    }
    for (sym, (book, pending)) in books.iter_mut() {
        while let Some(next) = pending.pop_front() {
            emit(sym, book, &next, &mut out)?;
        }
    }
    let books: std::collections::HashMap<String, OrderBook> =
        books.into_iter().map(|(k, (b, _))| (k, b)).collect();
    for (sym, book) in &books {
        eprintln!("{sym}: {:?}", book.stats());
    }
    Ok(())
}
