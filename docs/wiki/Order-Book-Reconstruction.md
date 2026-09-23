# Order Book Reconstruction

NSE publishes every order event but not the order book itself. `nsetick` reconstructs it by
replaying each security's events through the exchange's matching rules, and writes a snapshot
of the book at a fixed interval.

```bash
nsetick book data/parquet/segment=cm/kind=orders/date=2022-01-25 --out data/books --interval 1 --levels 20
```

```python
nsetick.build_books(input, out="data/books", interval_secs=1.0, levels=20, symbols=["TCS"])
```

**Input.** Either parsed Capital Market orders (the `date=...` directory holding `symbol=*`
partitions), or a raw `CASH_Orders_*.DAT.gz`. Prefer parsed input: every symbol is already
separated, so symbols replay in parallel with no decompression. Both paths produce identical
books; parse with all fields (the default) so the order-type flags are available.

**Output.** Parquet under `out/segment=cm/kind=book_snapshots/date=.../symbol=.../`.

## Snapshot columns

One row per symbol per interval, showing the book **as of** `snapshot_time`: events at or after
that instant are not yet applied.

| Column | Meaning |
|---|---|
| `symbol`, `snapshot_time` | Security and instant (IST wall clock, no timezone). |
| `best_bid`, `best_ask` | Best prices, paise. Null when that side is empty. |
| `mid_price` | Mean of the best prices, paise, as a float (a mid can land on a half paisa). |
| `spread` | `best_ask - best_bid`, paise. |
| `bid_px_k`, `ask_px_k` | Price at level *k* (1 = best), for *k* = 1 … `levels`. |
| `bid_qty_k`, `ask_qty_k` | **Visible** quantity at level *k*: what other participants could see. |
| `bid_hidden_k`, `ask_hidden_k` | Undisclosed quantity resting at level *k* behind disclosed-quantity orders. |
| `touch_{bid,ask}_algo_qty` | Visible quantity at the best price from algorithmic orders. |
| `touch_{bid,ask}_custodian_qty` | … from custodian (institutional) orders. |
| `touch_{bid,ask}_iceberg_qty` | … from disclosed-quantity orders, visible part. |
| `touch_{bid,ask}_iceberg_hidden` | Hidden quantity behind those orders at the best price. |
| `touch_{bid,ask}_orders` | Number of orders at the best price. |
| `interval_entries`, `interval_cancels`, `interval_modifies` | Events since the previous snapshot. |
| `interval_fills`, `interval_volume_matched` | Fills and quantity matched since the previous snapshot. |
| `interval_replenishments` | Hidden tranches revealed since the previous snapshot. |
| `total_bid_visible`, `total_ask_visible` | Visible quantity across the whole side, not only the captured levels. |
| `resting_hidden`, `active_icebergs` | Hidden quantity and disclosed-quantity orders resting anywhere in the book. |
| `live_orders` | Orders resting in the book. |
| `events_applied` | Cumulative events applied so far. |
| `is_crossed` | `'Y'` if the best bid is at or above the best ask. Should always be `'N'`. |

The touch composition and interval counts cannot be derived from an L2 feed after the fact,
which is why they are captured here.

## Matching rules

The replay follows NSE's matching rules, including the exchange-specific ones: market orders
priced at zero, IOC remainders that never rest, stop-loss orders held until the feed reports
their trigger, self-trade prevention, disclosed-quantity (iceberg) orders whose new tranches
lose priority, and modifies that keep or lose queue position. The
**[Matching Engine](Matching-Engine)** page explains every step, with diagrams and a worked
example.

In short:

- **Price-time priority**, with every fill at the resting order's price.
- An order that can trade on arrival does so at once. Whatever is left **rests** at the back
  of its price's queue, unless it is IOC or a market order, in which case it is **discarded**.
- **Icebergs** show one tranche at a time. Each new tranche goes to the back of the queue.
- A **modify** that only lowers the quantity keeps the order's place. Any other modify is
  cancel-then-enter.
- **Stop-loss** orders wait off the book until the feed's trigger record.
- **Self-trade prevention**: a resting order the exchange cancelled when a same-client order
  reached it is withdrawn, not traded.

## What the replay cannot know

- **Pre-open auction.** Orders entered 09:00–09:08 are matched by call auction at a single
  price. The replay matches them continuously as they arrive, so the book before 09:15 is
  approximate. Use snapshots from 09:15 onward.
- **Iceberg continuation.** When an incoming order uses up an iceberg's visible tranche, the
  exchange sometimes continues into the same iceberg's next tranche and sometimes moves on to
  the next order. The replay always moves on. This changes who trades with whom for a few
  percent of trades. It rarely changes depth or how much each order executes.

Measured against the exchange's trade file over 33 million trades, the replay reproduces
**94%** of trades exactly (same buy order, sell order, price and quantity), and **99.2%** of
orders execute exactly the right quantity. See [Validation and Accuracy](Validation-and-Accuracy).

## Choosing the interval and depth

- `levels=20` sees most of the resting book in a liquid name; `levels=5` sees only the front of
  the queue.
- A finer `interval_secs` resolves more in active securities and mostly repeats identical rows
  in quiet ones: at 5 s, between 12% (most liquid) and 69% (least liquid) of consecutive
  snapshots show an unchanged top of book. Output size scales with 1/interval.

## Performance

A full Capital Market session (about 500 securities, 560 million events) replays from parsed
Parquet in roughly 8 minutes on an 8-core laptop. The replay parallelises across symbols, so
it scales with cores.

## Checking the replay yourself

`nsetick.replay_fills` returns every trade the replay generates, with the buy and sell order
numbers, in the trade file's column names. `tools/verify_replay.py` compares them with the
parsed trade file:

```bash
python tools/verify_replay.py parsed/ --symbols TCS --dates 2022-01-25
```

See [Validation and Accuracy](Validation-and-Accuracy#doing-the-check-yourself).
