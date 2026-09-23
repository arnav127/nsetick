# An Advanced Run Spec

A run spec is a JSON file describing one or more parse jobs. This page walks through a large
spec, [`examples/advanced.json`](https://github.com/arnav127/nsetick/blob/main/examples/advanced.json),
that uses every option and most of the filter language. It is a single study kept as a file:
expiry day, 27 January 2022, turned into nine datasets in one command.

```bash
nsetick run examples/advanced.json --dry-run   # show what each job resolves to
nsetick run examples/advanced.json             # run them, one after another
```

```python
nsetick.run_spec("examples/advanced.json")
```

The spec was checked against real NSE files: every Capital Market job was run end to end, and
the FAO and index filters were compiled against their layouts.

## The whole spec

```json
{
  "defaults": {
    "out": "../data/parquet",
    "date": "2022-01-27",
    "compression": "zstd",
    "partition_by": "symbol",
    "threads": 6,
    "strict": true,
    "verify_trigger": true,
    "chunk_mb": 16,
    "row_group_rows": 1048576,
    "data_page_kb": 1024,
    "max_buffered_mb": 6144,
    "note": "Expiry day 27 Jan 2022: order flow and liquidity around the close"
  },
  "jobs": [
    {
      "input": "../data/raw/CASH_Orders_27012022.DAT.gz",
      "out": "../data/parquet/icebergs",
      "where": "series == 'EQ' and symbol in ('RELIANCE', 'TCS', 'INFY', 'HDFCBANK', 'M&M') and activity_type in (1, 4) and volume_disclosed > 0 and volume_original > volume_disclosed",
      "select": ["symbol", "order_number", "txn_time", "buy_sell", "activity_type", "limit_price", "volume_original", "volume_disclosed", "algo_indicator", "client_identity"],
      "note": "Iceberg entries and modifies for five large caps. Field-to-field comparison keeps only orders that hide part of their size."
    },
    {
      "input": "../data/raw/CASH_Orders_27012022.DAT.gz",
      "out": "../data/parquet/closing_aggression",
      "where": "series == 'EQ' and txn_time >= '15:00:00' and txn_time < '15:30:00' and activity_type == 1 and (mkt_order_flag == true or ioc_flag == true) and algo_indicator in (0, 2)",
      "select": ["symbol", "order_number", "txn_time", "buy_sell", "limit_price", "volume_original", "mkt_order_flag", "ioc_flag", "algo_indicator"],
      "note": "Aggressive algorithmic entries (market or IOC) in the last half hour, across the whole EQ universe."
    },
    {
      "input": "../data/raw/CASH_Orders_27012022.DAT.gz",
      "out": "../data/parquet/stops",
      "where": "series == 'EQ' and stop_loss_flag == true and ((buy_sell == 'B' and trigger_price <= limit_price) or (buy_sell == 'S' and trigger_price >= limit_price)) and not (client_identity == 2)",
      "note": "Stop-loss orders with a limit on the far side of the trigger, excluding proprietary flow. All columns."
    },
    {
      "input": "../data/raw/CASH_Orders_27012022.DAT.gz",
      "out": "../data/parquet/other_series",
      "where": "series not in ('EQ', 'BE') and activity_type = 1",
      "partition_by": "series",
      "compression": "snappy",
      "note": "New orders in every series other than EQ and BE, partitioned by series instead of symbol."
    },
    {
      "input": "../data/raw/CASH_Trades_27012022.DAT.gz",
      "out": "../data/parquet/block_algo_trades",
      "where": "series == 'EQ' and trade_quantity >= 5000 and trade_price >= 100000 and buy_algo_indicator in (0, 2) and sell_algo_indicator in (0, 2) and buy_client_identity != sell_client_identity",
      "partition_by": null,
      "note": "Large algo-to-algo trades between different participant categories, above Rs 1,000, in one file."
    },
    {
      "input": "../data/raw/CASH_Trades_27012022.DAT.gz",
      "out": "../data/parquet/buyer_initiated",
      "where": "symbol == 'RELIANCE' and buy_order_number > sell_order_number",
      "select": ["txn_time", "trade_number", "trade_price", "trade_quantity", "buy_order_number", "sell_order_number"],
      "partition_by": null,
      "note": "RELIANCE trades whose buy order is the newer of the two: roughly, trades a buyer initiated."
    },
    {
      "input": "../data/raw/FAO_Orders_27012022_01.DAT.gz",
      "layout": "fao_orders",
      "out": "../data/parquet/nifty_expiry_options",
      "where": "instrument == 'OPTIDX' and symbol == 'NIFTY' and expiry_date == '27JAN2022' and option_type in ('CE', 'PE') and strike_price >= 1700000 and strike_price <= 1760000 and spread_type != 'S'",
      "select": ["symbol", "strike_price", "option_type", "order_number", "txn_time", "buy_sell", "activity_type", "limit_price", "volume_original", "ioc_flag", "algo_indicator"],
      "partition_by": "option_type",
      "note": "Expiring near-the-money NIFTY options, spread orders excluded, one partition for calls and one for puts."
    },
    {
      "input": "../data/raw/CASH_Index_27012022.DAT.gz",
      "out": "../data/parquet/index",
      "partition_by": null,
      "compression": "snappy",
      "note": "NIFTY 50 and NIFTY Next 50, one row per second, unfiltered."
    },
    {
      "input": "../data/raw/CASH_Orders_27012022.DAT.gz",
      "out": "../data/parquet/smoke",
      "where": "symbol == 'M&M'",
      "max_records": 5000000,
      "strict": false,
      "verify_trigger": false,
      "partition_by": null,
      "note": "Smoke test: the first five million records only, malformed ones counted rather than fatal."
    }
  ]
}
```

## How a spec is read

* **`defaults` apply to every job**, and a job overrides any of them. Here, every job writes
  ZSTD partitioned by symbol unless it says otherwise.
* **Relative paths resolve against the spec file**, not the directory you run from. The spec
  lives in `examples/`, so `../data/raw` is the repository's `data/raw`. Move the spec with its
  data and it keeps working.
* **Unknown keys are an error.** `"partition-by"` or `"wehre"` stops the run before anything is
  read, so a typo cannot silently change what was produced.
* **`layout` and `date` are inferred from the file name** (`CASH_Orders_27012022` →
  `cm_orders`, 2022-01-27). Setting them, as the defaults and the FAO job do here, documents the
  choice and is required for files that have been renamed.
* **Each job's `note` goes into its manifest**, next to the output. Months later, the output
  directory still says why it exists.

## Every option, and where it is used

| Option | Used in | What it does |
|---|---|---|
| `input` | every job | The raw `.DAT.gz` (or `.DAT`) file |
| `out` | defaults, overridden per job | Output root. Hive directories `segment=/kind=/date=` go under it |
| `layout` | FAO job | Layout id; inferred from the file name when omitted |
| `date` | defaults | Session date. Selects the layout version and resolves time literals such as `'15:00:00'` |
| `select` | icebergs, closing, buyer-initiated, options | Columns to keep; omitted means all |
| `where` | most jobs | The filter, applied before any column is built |
| `partition_by` | defaults `symbol`; `series`, `option_type`, `null` per job | One directory per value of a string column, or `null` for a single file |
| `compression` | defaults `zstd`; `snappy` for two jobs | `zstd`, `snappy` or `none` |
| `threads` | defaults | Decode and write workers |
| `strict` | defaults `true`; smoke job `false` | Stop at the first malformed record, or count it and carry on |
| `verify_trigger` | defaults `true`; smoke job `false` | Check the file size against its `.trg` companion |
| `chunk_mb` | defaults | Size of decompressed chunks handed to decoders |
| `row_group_rows` | defaults | Parquet row group size |
| `data_page_kb` | defaults | Parquet data page size |
| `max_buffered_mb` | defaults | The run's memory ceiling (see below); omit to derive it from free memory |
| `max_records` | smoke job | Stop after this many records |
| `note` | every job | Free text for the manifest |

## The filters, feature by feature

| Feature | Example from the spec |
|---|---|
| String equality and `in` lists | `series == 'EQ'`, `symbol in ('RELIANCE', 'TCS', 'M&M')` |
| `not in` | `series not in ('EQ', 'BE')` |
| Integer comparisons and `in` | `activity_type in (1, 4)`, `algo_indicator in (0, 2)` |
| `=` as a synonym for `==` | `activity_type = 1` |
| Prices in raw units | `trade_price >= 100000` is ₹1,000.00; `strike_price >= 1700000` is 17,000.00 |
| Time windows | `txn_time >= '15:00:00' and txn_time < '15:30:00'` |
| Booleans | `mkt_order_flag == true`, `stop_loss_flag == true` |
| **Field against field** | `volume_original > volume_disclosed`, `trigger_price <= limit_price`, `buy_client_identity != sell_client_identity`, `buy_order_number > sell_order_number` |
| Parentheses and `or` | `(mkt_order_flag == true or ioc_flag == true)` |
| Nested groups | `((buy_sell == 'B' and trigger_price <= limit_price) or (buy_sell == 'S' and trigger_price >= limit_price))` |
| `not` | `not (client_identity == 2)` |
| Date fields | `expiry_date == '27JAN2022'` |

Some things to know, all of which the spec respects:

* **Field names are exact** (`symbol`, not `SYMBOL`). Keywords are not: `AND`, `Not` and `IN` all
  work.
* **A field compared with another field must have the same scale.** `trigger_price <=
  limit_price` is fine. `limit_price > volume_original` is refused, because it compares money
  with shares.
* **Date and text fields compare as written in the file.** `expiry_date` holds `27JAN2022`,
  so compare it for equality, or use `in (...)` for several expiries. The index file's
  `txn_time_hms` is text in the same way. Ranges (`<`, `>=`) are for numbers, prices and
  `txn_time`.
* **`buy_order_number > sell_order_number`** works because NSE numbers orders as they arrive,
  so the larger number is usually the newer order: the one that crossed the spread. It is a
  quick proxy for the aggressor side, not an exact one (a modified order keeps its old number).

Check any filter without touching data:

```python
nsetick.check_filter("series == 'EQ' and trigger_price <= limit_price", "cm_orders")
```

## Partitioning choices

* **`symbol`** (the default) suits almost everything. NSE writes each symbol's records together
  and in time order, so every partition comes out time-sorted with no sort step, and the order
  book replay reads one symbol at a time.
* **Another string column** (`series`, `option_type`, `buy_sell`, `instrument`) groups by that
  instead. Use it when the study splits along that column.
* **`null`** writes one file. Use it for small or single-symbol outputs. Readers such as DuckDB
  still skip row groups efficiently using the statistics in the file.

A partition column must be in the output. The `buyer_initiated` job selects no `symbol`, so it
sets `"partition_by": null`. Without that, it inherits `symbol` from the defaults and nsetick
stops with:

```text
cannot partition by "symbol" because it is not in the output schema.
Either include it in --select or pass --partition-by none.
```

## Memory

Each open partition costs about 2 MB for the whole run. The closing-window job touches every
EQ symbol, about 1,450 partitions and 3.4 GB at peak, so the spec sets `max_buffered_mb` to 6144.
With 2048, the run stops as soon as the next partition would cross the limit, and explains
why, instead of exhausting the machine:

```text
memory guard: 648 open partitions would need about 2.0 GB, over the 2.0 GB limit for this run.
```

Leave `max_buffered_mb` out and nsetick sizes the limit from the machine's free memory. See
[Performance and Memory](Performance-and-Memory).

## Running part of a spec

There is no job selector. For a quick check, copy the spec and set `"max_records"` in the
defaults, which is what the smoke job does for one job. `--dry-run` prints every job fully
resolved (paths, layout, date, filter) without reading any data. It is the fastest way to catch
a wrong path or a mistyped field before a long run.
