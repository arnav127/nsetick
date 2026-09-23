# Python API

```python
import nsetick
```

All functions accept a path to a `.DAT.gz` (or uncompressed `.DAT`) file. Unless given, the
layout and session date are inferred from the NSE file name, e.g. `CASH_Orders_25012022.DAT.gz`
→ `cm_orders`, 2022-01-25.

Arguments common to the reading functions:

| Argument | Meaning |
|---|---|
| `layout` | Layout id such as `"cm_orders"`. Inferred from the file name when omitted. |
| `date` | Session date, `"YYYY-MM-DD"`. Selects the layout version and resolves time literals in filters. |
| `select` | List of fields to return. Default: all. Output keeps the file's field order. |
| `where` | A [filter expression](Filter-Language), applied before decoding. |
| `strict` | `True` (default) stops at the first malformed record; `False` counts it and continues. |

## Reading into memory

### `iter_batches(input, *, layout=None, date=None, select=None, where=None, strict=True, chunk_mb=8)`

Streams `pyarrow.RecordBatch` objects. Memory stays flat however large the session. The
returned reader has a `.schema` and, once iterated, a `.stats` dict (`rows_read`,
`rows_emitted`, `rows_malformed`, ...).

```python
reader = nsetick.iter_batches(path, where="symbol == 'INFY'", select=["txn_time", "limit_price"])
for batch in reader:
    ...
print(reader.stats)
```

A filter that matches nothing yields no batches, not a stream of empty ones.

### `read_table(input, *, ..., max_rows=None)` → `pyarrow.Table`

Everything matching, as one table. `max_rows` caps the result, which is useful for a first look
at a large file.

### `to_pandas(input, **kwargs)` / `to_polars(input, **kwargs)`

As `read_table`, converted. Need `pandas` or `polars` installed.

## Writing Parquet

### `parse(input, out, *, layout=None, date=None, select=None, where=None, partition_by="symbol", compression="snappy", threads=None, strict=True, verify_trigger=True, memory_limit_mb=None, max_records=None, row_group_rows=None, note=None)` → `dict`

Parses one file into a Hive-partitioned Parquet dataset under `out` and returns a summary
(rows read and written, partitions, timings, memory used).

| Argument | Meaning |
|---|---|
| `partition_by` | Column to partition on, or `None` / `"none"` for one file per session. |
| `compression` | `"snappy"` (default), `"zstd"` (about a third smaller, slower to write) or `"none"`. |
| `threads` | Decode and write workers. Default: the machine's cores. |
| `verify_trigger` | Check the file's size against its `.trg` companion when one exists. |
| `memory_limit_mb` | Ceiling on the run's memory. Default: derived from free memory. The run stops with an explanation rather than exhausting the machine. |
| `max_records` | Stop after about this many records — for smoke tests on large files. |
| `note` | Free text recorded in the run's manifest. |

### `run_spec(path)` → `list[dict]`

Runs every job in a JSON run spec. See [Parsing](Parsing#run-specs).

## Order books

### `build_books(input, out, *, date=None, where=None, interval_secs=1.0, levels=20, threads=None, compression="snappy", max_records=None, symbols=None, previous_close=None)` → `dict`

Replays order events into a limit order book per symbol and writes periodic snapshots as
Parquet, partitioned by symbol.

- `input`: a directory of parsed orders (the `date=...` directory containing `symbol=*`
  partitions), or a raw `CASH_Orders` file. Parsed input is much faster.
- `interval_secs`: seconds between snapshots; fractions are allowed.
- `levels`: price levels captured per side.
- `symbols`: restrict parsed input to these symbols.
- `where`: filter applied to raw input (default `series == 'EQ'`).
- `previous_close`: `{symbol: price_in_paise}`, each symbol's previous closing price. Only used
  to break a tie between equally good pre-open auction prices; see
  [Order Book Reconstruction](Order-Book-Reconstruction#what-the-replay-needs-from-you).

Returns a report with `symbols`, `snapshots`, `events_applied`, `fills_generated`,
`replenishments`, `crossed_symbols` and timings. See
[Order Book Reconstruction](Order-Book-Reconstruction) for the output columns and the matching
rules.

### `replay_fills(input, *, symbols=None, previous_close=None)` → `pyarrow.Table`

Replays parsed orders and returns **every trade the replay generates**, one row per trade,
named like the exchange's trade file so the two can be joined:

| Column | Meaning |
|---|---|
| `symbol` | Security |
| `txn_time` | Time of the incoming order's event (IST, no timezone) |
| `trade_price` | Price in paise: always the resting order's price |
| `trade_quantity` | Shares |
| `buy_order_number`, `sell_order_number` | The two orders, as in the trade file |
| `aggressor` | `"B"` or `"S"`: the side of the incoming order |

- `input`: a parsed-orders `date=...` directory, or one `part-*.parquet` file.
- `symbols`: restrict to these symbols (all if omitted).
- `previous_close`: as for `build_books`.

Auction trades are included, stamped at the last pre-open event.

The exchange stamps each of its trade records slightly after the order that caused it (one
tick of about 15 µs per trade), so join on the order numbers, not on time.

```python
fills = nsetick.replay_fills("parsed/segment=cm/kind=orders/date=2022-01-25", symbols=["TCS"])
fills.to_pandas().head()
```

See [Validation and Accuracy](Validation-and-Accuracy#doing-the-check-yourself) for a full
comparison against the trade file.

## Inspecting layouts and files

| Function | Returns |
|---|---|
| `layouts()` | The layout ids available. |
| `describe(layout_id, date=None)` | The layout version in force on `date`: fields with offset, length, type, scale and padding; `record_length`; whether it is verified against real data. |
| `probe(input, layout=None, date=None)` | The layout and version a file selects and its observed record length. |
| `check_filter(expr, layout_id, date=None)` | `True`, or raises `ValueError` explaining what is wrong — without opening any data. |
| `memory_estimate(partitions=0)` | The memory a run would plan for on this machine. |

Validate a filter before a long run:

```python
nsetick.check_filter("series == 'EQ' and symbl == 'TCS'", "cm_orders")
# ValueError: unknown field "symbl"; this layout has: activity_type, algo_indicator, ...
```

## Types in the output

| Spec type | Arrow type | Notes |
|---|---|---|
| strings | `utf8` | Padding removed. Symbols keep characters such as `&` and `-`. |
| integers | `uint8` / `uint64` | |
| prices | `int64` | Raw integer units; the column's `scale` metadata gives the decimal places (2 for CM and FAO, 4 for CD). |
| times | `timestamp[us]` | IST wall clock, no timezone. |
| `Y`/`N` flags | `bool` | |

Blank fields are null.

## The reference decoder

`nsetick.reference` is a deliberately simple pure-Python decoder, used as the oracle the Rust
core is tested against. It is slow; use it to check a decoded value, not to parse sessions.
