# Validation and Accuracy

What has been checked, how, and what the remaining error is. Numbers are from release 0.2.0.

## Decoding

**Two independent decoders agree.** The package ships a deliberately simple pure-Python
decoder (`nsetick.reference`) written to be obviously correct. The Rust decoder was compared
against it field by field on real files:

| Files | Records compared | Fields | Disagreements |
|---|---|---|---|
| CM orders and trades for 27 Jan, 30 Jun and 29 Dec 2022 | 1,200,000 | every field | 0 |

**Every layout version is checked against the file.** The observed record length must match
the selected specification, or the file is refused. `python spec/validate_layouts.py` checks
the specifications themselves: fields contiguous, lengths summing to the record length, no
overlapping versions.

**Known decoding traps are tested:** symbols containing `&` (`M&M`), the literal-`b` padding
byte, the four-decimal Currency Derivatives prices, IST timestamps without a timezone, and
multi-member gzip files that a standard reader truncates.

## Order book reconstruction

The exchange publishes no book, but it does publish every execution: the trade file records
each trade with the **order number of the buyer and of the seller**. The replay produces the
same record for every trade it generates. So the check is not "is the volume about right?"
but "**did the same two orders trade, at the same price, for the same quantity, in the same
record?**", one trade at a time.

`tools/verify_replay.py` reports, for each session and security:

| Measure | Question it answers |
|---|---|
| **missed** | Number of the exchange's trade records the replay does not reproduce exactly |
| **exact** | Share of the exchange's trade records reproduced exactly: same buy order, sell order, price and quantity |
| **pairs** | Share of (buy order, sell order) pairs that trade the same total quantity at the same average price |
| **orders** | Share of orders that execute exactly the right total quantity over the day |
| **volume** | Replay volume as a share of the exchange's |

### Results

24 sessions spread across 2022 × 10 large Capital Market securities, the whole trading day
(pre-open auction, continuous session, post-close session): **33,379,259 trade records, none
missed.**

| Security | Sessions | Trades from 09:15 | Missed |
|---|---:|---:|---:|
| APOLLOHOSP | 24 | 1,557,269 | 0 |
| BPCL | 24 | 1,456,143 | 0 |
| CIPLA | 24 | 1,803,303 | 0 |
| DIVISLAB | 24 | 1,527,205 | 0 |
| EICHERMOT | 24 | 1,172,360 | 0 |
| HDFCBANK | 24 | 5,045,399 | 0 |
| ICICIBANK | 24 | 5,600,806 | 0 |
| INFY | 24 | 5,164,815 | 0 |
| RELIANCE | 24 | 6,211,796 | 0 |
| TCS | 24 | 3,749,427 | 0 |
| **All** | **240** | **33,288,523** | **0** |
| Pre-open auctions | 240 | 90,736 | 0 |

Every one of the exchange's trade records in those sessions is reproduced: the same two
orders, the same price, the same quantity, split into records the same way. Every order
executes exactly the quantity the exchange executed for it, and total volume is exact. The
pre-open auctions match too, price included: every tied auction was settled from the feed, so
no previous close was needed.

Earlier releases were measured the same way: 0.1.0 matched 91–97% of volume and was not
checked order by order; the first version of 0.2.0 reproduced 94% of trades exactly.

### How the rules were found

One loop, repeated until nothing was left:

1. **Replay a security while reading its trade file alongside.** Label each of the
   exchange's trades with the order event that caused it (`tools/label_trades.py`), and run
   two books in lockstep (`examples/oracle_diff`): one matched by the rules, one driven by the
   exchange's own trades.
2. **Stop at every event where the two trade differently**, and record the queue as it stood.
   The rules book is then reset from the exchange-driven one, so every difference found is a
   root cause, never the knock-on effect of an earlier one.
3. **Find what the differences have in common, write a rule, test it against every case in
   the data** (not just the ones that prompted it), keep it only if it removes errors without
   adding any.

Starting from about 4% of trading events differing, the rules below took it to zero.

### The rules, and the evidence for each

| Rule | Evidence in the files |
|---|---|
| Market orders match at any price | 3.7M entries in one session (3.4%) carry a limit price of 0 and the market flag; over 500,000 of them trade within 15 µs of entry. |
| IOC remainders never rest | Every unfilled or partly filled IOC is followed by a cancel within 15–31 µs; fully filled IOCs never are. |
| Stop-loss orders follow the feed's trigger record | A stop-loss order is the only kind that ever has two entry records. Triggering from the replay's own last price fired stops early. |
| Self-trade prevention, by the cancel's slot alone | The exchange stamps each message of a match one jiffy after the last; the cancel lands in the slot of the step that reached the order. It is sometimes the *incoming* order that is cancelled. Filtering by participant category or algo flag hid genuine cases: the same client trades as custodian and non-custodian. |
| A self-trade cancel ends the trade record | The two prints either side of it are 2 jiffies apart, and the iceberg's tranche accounting treats them as separate trades. |
| Iceberg tranches go to the back | Keeping a replenished tranche at the front drops exact trades from about 95% to about 65%. |
| Every trade counts against the tranche; one that uses it up leaves a fresh one | Whenever the exchange moved from an iceberg to the next order at a price, it had just taken exactly the modelled visible amount: 100.00% of about 125,000 such moments in TCS and INFY, against 93% for the textbook rule. The iceberg's own trades as aggressor count too. |
| Modifies keep their place unless the display grows | Keep-vs-lose labels from the trade file: reducing an iceberg's disclosed quantity keeps its place (139 of 154 cases); the exceptions were all cases where the new display was larger than what was left. |
| A modify can carry the current tranche over | Measured on every tranche used up after a modify (about 81,000): carried when the disclosed quantity is unchanged or cut to the remainder, fresh otherwise, with the exceptions in [Matching Engine § 6](Matching-Engine#6-modify). |
| Pre-open call auction | Maximum volume, then minimum imbalance, then closest to the previous close; market orders after limit orders; priority kept through non-increasing modifies. 240 of 240 sessions, all 90,736 auction trades, identical in pairs, quantities and order. |
| Post-close session | The closing price is the 15:00–15:30 VWAP truncated to paise and rounded to the tick: 240 of 240. |
| A modify carries the remaining quantity | Fills after a modify never exceed its quantity. |

Two further checks:

- **The raw-file and parsed-Parquet replay paths produce identical books**, snapshot for
  snapshot.
- **Every rule has a unit test** built from a small hand-worked book.

### What can still differ

- **A tied auction with no conversion to read.** When two prices are equally good for the
  pre-open auction, the exchange takes the one closest to the previous close. The replay
  usually reads the exchange's choice from the feed itself (it converts unfilled market orders
  into limit orders at that price), but if nothing is left to convert, it needs the previous
  close; see [Order Book Reconstruction](Order-Book-Reconstruction#what-the-replay-needs-from-you).
  Without it, only the auction's price can differ, not its pairs.
- **Days and securities not checked.** The rules were found on 10 large securities over 24
  sessions of 2022. Other securities, other years (layouts and exchange behaviour change) and
  special sessions (muhurat trading, special pre-open sessions for IPOs and relistings) have
  not been checked. Run the check below on your own data: it takes minutes.

## Doing the check yourself

Parse a session's orders **and** trades, then:

```bash
python tools/verify_replay.py parsed/                          # everything found
python tools/verify_replay.py parsed/ --symbols TCS --dates 2022-01-25
python tools/verify_replay.py parsed/ --continuous-only        # from 09:15
```

It prints one line per session and security, and a total. The script needs `duckdb`
(`pip install duckdb`) and a source checkout (it lives in `tools/`).

Or work with the replay's trades directly:

```python
import duckdb, nsetick

replay = nsetick.replay_fills("parsed/segment=cm/kind=orders/date=2022-01-25", symbols=["TCS"])
actual = duckdb.sql("""SELECT * FROM read_parquet(
    'parsed/segment=cm/kind=trades/date=2022-01-25/symbol=TCS/*.parquet')""")

# trades the exchange made that the replay did not reproduce exactly
duckdb.sql("""
    SELECT a.buy_order_number, a.sell_order_number, a.trade_price, a.trade_quantity
    FROM actual a ANTI JOIN replay r
      USING (buy_order_number, sell_order_number, trade_price, trade_quantity)
""").show()
```

### If something differs: finding the cause

```bash
python tools/label_trades.py parsed/ 25012022 TCS labelled.csv
cargo run --release -p nsetick-book --example oracle_diff -- \
    parsed/cash_orders/date=25012022/symbol=TCS/part-000.parquet labelled.csv diffs.csv
```

`diffs.csv` has one row per event where the rules and the exchange trade differently, with
the replay's trades, the exchange's, and the queue at the first price where they part. If you
find a case, please open an issue with that row.

## Tests

- 145 Rust unit tests covering the decoder, filter language, writer and every matching rule.
- Python tests of the bindings; those needing real data run when `NSETICK_TEST_FILE` points at
  an NSE file, and pass on both orders and trades files.
- Continuous integration runs everything on Linux, macOS and Windows for every change.
