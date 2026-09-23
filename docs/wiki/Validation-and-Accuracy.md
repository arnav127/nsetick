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
each trade with the order number on each side. If the replay applies NSE's matching rules
correctly, the volume it matches should equal the volume the trade file records.

**Continuous-session volume matched by the replay, as a share of volume actually traded**
(25 January 2022):

| Security | 0.1.0 | 0.2.0 |
|---|---|---|
| RELIANCE | 94.4% | 101.4% |
| HDFCBANK | 93.6% | 100.7% |
| INFY | 93.9% | 100.4% |
| TCS | 92.5% | 101.0% |
| SBIN | 92.8% | 102.1% |
| ITC | 94.0% | 100.9% |
| M&M | 97.5% | 100.5% |
| ABAN | 91.3% | 100.3% |
| IDEA | 101.3% | 102.2% |
| YESBANK | 120.6% | 108.5% |

0.1.0 fell short because it dropped market orders, and over-shot where it let stop-loss orders
and self-trade-prevented orders trade. Each cause was identified from the data before it was
fixed:

| Rule | Evidence in the files |
|---|---|
| Market orders match at any price | 3.7M entries in one session (3.4%) carry a limit price of 0 and the market flag; over 500,000 of them trade within 15 µs of entry. |
| IOC remainders never rest | Every unfilled or partly filled IOC is followed by a cancel within 15–31 µs; fully filled IOCs never are. |
| Stop-loss orders wait for a trigger | Those that trade first fill a median 22.6 s after entry, against 0.4 s for ordinary limit orders. |
| Self-trade prevention | Resting orders the exchange cancels within 1 ms of an incoming order that would have matched them account for the excess volume in the worst-affected security. |
| A modify carries the remaining quantity | Fills after a modify never exceed its quantity; lifetime fills exceed it for 81% of partly filled orders. |

Two further checks:

- **The raw-file and parsed-Parquet replay paths produce identical books** — 487,583 snapshot
  rows compared, zero differences.
- **Queue priority on a same-price quantity reduction** (kept, as some exchanges do, or lost,
  as the replay does) made no measurable difference to the comparison (103.51% against
  103.55% over 22 securities), so the documented cancel-then-enter rule is kept.

### Remaining error

For most securities the replay matches within about 0.3–2.3% of actual volume, always slightly
over. The worst case examined was YESBANK at +8.5%: a heavily traded, low-priced security with
very deep queues at a tick that is large relative to its price, where a small error in queue
position turns into a large error in volume. The pre-open auction is replayed as continuous
matching and is approximate; use snapshots from 09:15.

**In practice:** spreads, depth, hidden quantity and touch composition are reliable for
research use. For studies that depend on exact fill volumes in very deep-queue, low-priced
securities, check against the trade file.

## Doing the check yourself

`examples/dump_fills` writes every fill the replay generates:

```bash
cargo run --release -p nsetick-book --example dump_fills -- \
    parsed/segment=cm/kind=orders/date=2022-01-25/symbol=TCS/part-000.parquet fills.csv
```

Then compare with the trade file, for instance in DuckDB:

```sql
WITH replay AS (SELECT SUM(quantity) q FROM read_csv_auto('fills.csv')),
     actual AS (SELECT SUM(trade_quantity) q
                FROM read_parquet('parsed/segment=cm/kind=trades/date=2022-01-25/symbol=TCS/*.parquet'))
SELECT replay.q, actual.q, 100.0 * replay.q / actual.q AS pct FROM replay, actual;
```

Joining the fills to the trade file on order number (`incoming_order` / `resting_order` against
`buy_order_number` / `sell_order_number`) shows exactly which orders the replay fills
differently from the exchange.

## Tests

- 130+ Rust unit tests covering the decoder, filter language, writer and every matching rule.
- Python tests of the bindings; those needing real data run when `NSETICK_TEST_FILE` points at
  an NSE file, and pass on both orders and trades files.
- Continuous integration runs everything on Linux, macOS and Windows for every change.
