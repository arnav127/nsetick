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

The replay follows NSE's matching rules through the whole day, and reproduces the exchange's
own trade records essentially exactly. The **[Matching Engine](Matching-Engine)** page explains
every step, with diagrams and a worked example. In short:

- **Pre-open call auction** (09:00 to about 09:08): orders are collected, then matched at one
  equilibrium price; what is left forms the opening book.
- **Continuous session** (09:15 to 15:30): price-time priority, every fill at the resting
  order's price. An order that can trade on arrival does so at once; what is left rests at
  the back of its price's queue, unless it is IOC or a market order, in which case it is
  discarded.
- **Icebergs** show one tranche at a time; the exchange counts each tranche down trade by
  trade, and a fresh tranche goes to the back of the queue.
- A **modify** keeps the order's place unless it changes the price or makes the order show
  more.
- **Stop-loss** orders wait off the book until the feed's trigger record.
- **Self-trade prevention**: when an order reaches one of the same client's, the exchange
  cancels one of them, and the replay does the same.
- **Post-close session** (from 15:40): every order trades at the closing price, in time order.

## What the replay needs from you

Nothing, in practice. The one input a day's files lack is the **previous day's close**: when two
prices are equally good for the pre-open auction, the exchange picks the one closest to it. The
replay normally reads the exchange's choice from the feed instead (moments after the auction,
the exchange converts unfilled market orders into limit orders at that price), and in all 240
security-sessions checked that settled every tie. If an auction ties and leaves nothing to
convert, pass the previous close:

```python
nsetick.build_books(orders, out="books", previous_close={"TCS": 376990, "INFY": 172215})
```

```bash
nsetick book ORDERS_DIR --out books --previous-close closes.csv   # lines of symbol,price (paise)
```

Without it, such a tie takes the lowest candidate price. Only the auction's trade price is
affected; the book it leaves behind is the same.

See [Validation and Accuracy](Validation-and-Accuracy) for how closely the replay matches the
exchange.

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
