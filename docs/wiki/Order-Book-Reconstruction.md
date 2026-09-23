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

The replay implements these NSE behaviours. Each was checked against the exchange's trade
file — see [Validation and Accuracy](Validation-and-Accuracy).

**Price–time priority.** Better prices first; at a price, earlier orders first. Fills print at
the resting order's price.

**Disclosed quantity.** An order with `0 < volume_disclosed < volume_original` shows only the
disclosed amount. The rest is real liquidity that nobody else can see.

**Replenishment loses priority.** When the visible tranche of a disclosed-quantity order is
used up, the next tranche is revealed and joins the **back** of the queue at its price.

**A modify is cancel-then-enter.** Activity type 4 removes the order and re-enters it, so it
loses its place in the queue. The modify carries the order's *remaining* quantity, not its
original total.

**Market orders** have a limit price of zero in the file. They take liquidity at any price and
never rest.

**Immediate-or-cancel** orders never rest: whatever does not fill on entry is discarded. The
file records a cancel for the remainder a few microseconds later; the replay recognises it.

**Stop-loss** orders wait off the book until the last traded price reaches their trigger (at or
above it for a buy, at or below for a sell), then enter as ordinary orders.

**Self-trade prevention.** When an incoming order would match a resting order from the same
client, NSE cancels the resting order instead. The file carries no client identifier, but it
records that cancel a few hundred microseconds after the incoming order. The replay reads 1 ms
ahead of the event it is applying and withdraws a resting order the exchange is about to cancel.

## What the replay cannot know

- **Pre-open auction.** Orders entered 09:00–09:08 are matched by call auction at a single
  price. The replay matches them continuously as they arrive, so the book before 09:15 is
  approximate. Use snapshots from 09:15 onward.
- **Queue position after events it cannot see.** Anything the exchange did that leaves no
  record in the orders file cannot be reproduced.
- **Residual over-matching.** After all of the above, the replay matches about 0.3–2.3% more
  volume than the exchange printed for most securities, and up to 8.5% in the worst security
  examined. The error is largest in heavily traded, low-priced securities with deep queues at a
  large tick relative to price. Treat depth and fills as highly accurate but not exact.

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

`examples/dump_fills` writes every fill the replay generates, with the incoming and resting
order numbers. The trade file carries the order number on each side of every execution, so the
two can be joined:

```bash
cargo run --release -p nsetick-book --example dump_fills -- \
    data/parquet/segment=cm/kind=orders/date=2022-01-25/symbol=TCS/part-000.parquet fills.csv
```

See [Validation and Accuracy](Validation-and-Accuracy) for how the published figures were
produced.
