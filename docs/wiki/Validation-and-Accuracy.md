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
same record for every trade it generates. So the check is not only "is the volume about
right?" but "**did the same two orders trade, at the same price, for the same quantity?**",
one trade at a time.

`tools/verify_replay.py` reports four measures for each session and security (continuous
session, from 09:15):

| Measure | Question it answers |
|---|---|
| **exact** | Share of the exchange's trades that the replay reproduces exactly: same buy order, sell order, price and quantity |
| **pairs** | Share of (buy order, sell order) pairs that trade the same total quantity at the same average price |
| **orders** | Share of orders that execute exactly the right total quantity over the session |
| **volume** | Replay volume as a share of the exchange's |

### Results

24 sessions spread across 2022 × 10 large Capital Market securities: **33.3 million trades**.

| Security | Trades | Exact | Pairs | Orders | Volume (range over sessions) |
|---|---:|---:|---:|---:|---|
| APOLLOHOSP | 1,557,269 | 97.1% | 97.4% | 99.5% | 99.9–100.3% |
| BPCL | 1,456,143 | 96.5% | 96.6% | 99.4% | 99.9–102.4% |
| CIPLA | 1,803,303 | 95.7% | 95.9% | 99.3% | 99.9–102.8% |
| DIVISLAB | 1,527,205 | 96.4% | 96.8% | 99.5% | 99.9–100.5% |
| EICHERMOT | 1,172,360 | 97.0% | 97.4% | 99.5% | 99.9–100.6% |
| HDFCBANK | 5,045,399 | 93.6% | 93.4% | 99.1% | 99.8–100.9% |
| ICICIBANK | 5,600,806 | 93.3% | 92.9% | 99.1% | 99.9–100.8% |
| INFY | 5,164,815 | 93.0% | 92.9% | 99.0% | 99.8–100.2% |
| RELIANCE | 6,211,796 | 93.0% | 92.9% | 99.2% | 99.8–101.9% |
| TCS | 3,749,427 | 93.9% | 93.9% | 99.2% | 99.8–101.5% |
| **All** | **33,288,523** | **94.0%** | **94.0%** | **99.2%** | **100.2%** |

Across the 240 session-security pairs, exact trades range from 89.5% to 98.9% (median
95.0%), and orders with exactly the right executed quantity from 98.4% to 99.8%.

**How to read this.** More than 99% of orders execute exactly the quantity the exchange
executed for them. The book therefore holds the right orders at the right sizes almost all the
time. Where exact trades fall short, the usual reason is that the right volume went through
the right orders, but was split between counterparties differently: typically the exchange
carried on into an iceberg's next tranche where the replay moved on to the next order in the
queue, or the other way round (see
[Matching Engine § 11](Matching-Engine#11-what-the-engine-does-not-model)). This matters little
for depth and spreads, but is visible when you study who traded with whom.

### How each rule was established

Each rule in the [Matching Engine](Matching-Engine) was found or confirmed from the data. Most
came from a trade-by-trade mismatch that the rule then removed:

| Rule | Evidence in the files |
|---|---|
| Market orders match at any price | 3.7M entries in one session (3.4%) carry a limit price of 0 and the market flag; over 500,000 of them trade within 15 µs of entry. |
| IOC remainders never rest | Every unfilled or partly filled IOC is followed by a cancel within 15–31 µs; fully filled IOCs never are. |
| Stop-loss orders follow the feed's trigger record | A stop-loss order is the only kind that ever has two entry records. Triggering from the replay's own last price fired stops early and produced trades the exchange never made. |
| Self-trade prevention, by sweep slot | The exchange stamps each message of one matching event one jiffy after the last. Cancels of resting orders land exactly in the slot of the sweep step that reached them. A fixed 1 ms window both missed deep-sweep cancels and withdrew orders their owners cancelled after being filled. |
| Iceberg tranches go to the back | Keeping a replenished tranche at the front dropped exact trades from about 95% to about 65%. |
| Same-price reductions keep priority | Treating them as cancel-then-enter put orders behind others they had in fact traded ahead of. |
| One trade per pair and price | An incoming order working through an iceberg alone at its price prints as one trade in the exchange's file, not one per tranche. |
| A modify carries the remaining quantity | Fills after a modify never exceed its quantity; lifetime fills exceed it for 81% of partly filled orders. |

Two further checks:

- **The raw-file and parsed-Parquet replay paths produce identical books**, snapshot
  for snapshot.
- **The engine has unit tests for every rule**, each built from a small hand-worked book.

### What remains

- **Iceberg continuation** is the main known gap (above).
- **The pre-open auction** is replayed as continuous matching and is approximate. Use
  snapshots and trades from 09:15.

**In practice:** spreads, depth, hidden quantity, queue composition and per-order executions
are reliable for research use. For studies of exactly which counterparties traded, allow for a
few percent of trades being split differently from the exchange, or compare with the trade
file directly as below.

## Doing the check yourself

Parse a session's orders **and** trades, then:

```bash
python tools/verify_replay.py parsed/                          # everything found
python tools/verify_replay.py parsed/ --symbols TCS --dates 2022-01-25
```

It prints one line per session and security with the four measures, and a total. Or work
with the replay's trades directly:

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
    WHERE CAST(a.txn_time AS TIME) >= TIME '09:15:00'
""").show()
```

The script needs `duckdb` (`pip install duckdb`) and a source checkout (it lives in `tools/`).

## Tests

- 135+ Rust unit tests covering the decoder, filter language, writer and every matching rule.
- Python tests of the bindings; those needing real data run when `NSETICK_TEST_FILE` points at
  an NSE file, and pass on both orders and trades files.
- Continuous integration runs everything on Linux, macOS and Windows for every change.
