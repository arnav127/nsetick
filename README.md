# nsetick

[![CI](https://github.com/arnav127/nsetick/actions/workflows/ci.yml/badge.svg)](https://github.com/arnav127/nsetick/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/arnav127/nsetick)](https://github.com/arnav127/nsetick/releases)

**Turn NSE historical tick data into analysis-ready Parquet and DataFrames, and rebuild the
limit order book from it.**

NSE's historical order and trade files are huge gzipped fixed-width text: one day of Capital
Market orders is about 8 GB compressed and 700 million records. `nsetick` reads them for you:

- **Parse** a session to Parquet in minutes, keeping only the rows and columns you ask for.
- **Stream** straight into pandas, Polars, PyArrow or DuckDB, without writing anything to disk.
- **Rebuild the order book** at any interval: depth, spreads, and the hidden quantity behind
  iceberg orders, following NSE's matching rules from the opening auction to the close.
- **Trust the output.** Every byte offset is defined once and checked against each file. The
  rebuilt book reproduces the exchange's own trade records exactly: every one of 33 million
  trades checked, order for order.

It works as a command-line tool and as a Python package. You don't need Rust or a compiler.

📖 **Full guide: [the wiki](https://github.com/arnav127/nsetick/wiki)**

---

## Install

### Python package (Python 3.9+, Linux / macOS / Windows)

```bash
pip install nsetick
```

or straight from a GitHub release:

```bash
pip install nsetick --find-links https://github.com/arnav127/nsetick/releases/expanded_assets/v0.2.0
```

`--find-links` points pip at the release page, and pip picks the right wheel for your machine.
For pandas or Polars output, add `pip install pandas` or `pip install polars`.

### Command-line tool

Download the archive for your platform from the
[latest release](https://github.com/arnav127/nsetick/releases/latest), unpack it, and put
`nsetick` on your `PATH`:

| Platform | File |
|---|---|
| Linux x86_64 | `nsetick-<version>-x86_64-unknown-linux-musl.tar.gz` |
| Linux ARM | `nsetick-<version>-aarch64-unknown-linux-musl.tar.gz` |
| macOS Apple silicon | `nsetick-<version>-aarch64-apple-darwin.tar.gz` |
| macOS Intel | `nsetick-<version>-x86_64-apple-darwin.tar.gz` |
| Windows | `nsetick-<version>-x86_64-pc-windows-msvc.zip` |

```bash
tar xzf nsetick-0.2.0-x86_64-unknown-linux-musl.tar.gz
./nsetick-0.2.0-x86_64-unknown-linux-musl/nsetick --version
```

The Linux builds are static, so they run on any distribution, including old cluster nodes.
Offline installs, checksums and building from source are covered on the
[Installation](https://github.com/arnav127/nsetick/wiki/Installation) page.

---

## Quick tour

### Look at a file

```bash
nsetick inspect CASH_Orders_25012022.DAT.gz --n 5      # decode the first five records
nsetick describe cm_orders                             # what fields the file has
```

nsetick works out the file type and session date from the NSE file name.

### Parse to Parquet

```bash
nsetick parse CASH_Orders_25012022.DAT.gz --out data/parquet
```

Only the columns and rows you need:

```bash
nsetick parse CASH_Orders_25012022.DAT.gz --out data/parquet \
  --select symbol,txn_time,buy_sell,limit_price,volume_original \
  --where "series == 'EQ' and symbol in ('RELIANCE', 'TCS', 'M&M')"
```

The output is ordinary partitioned Parquet, one directory per symbol, already in time order:

```text
data/parquet/segment=cm/kind=orders/date=2022-01-25/symbol=RELIANCE/part-000.parquet
```

Read it with any tool:

```sql
-- DuckDB
SELECT symbol, COUNT(*) FROM read_parquet('data/parquet/**/*.parquet', hive_partitioning = 1)
GROUP BY symbol;
```

### Straight into Python

```python
import nsetick

df = nsetick.to_pandas(
    "CASH_Trades_25012022.DAT.gz",
    where="symbol == 'INFY' and txn_time >= '15:00:00'",
    select=["txn_time", "trade_price", "trade_quantity"],
)
```

For a whole session, stream it in batches. Memory stays flat however big the file is:

```python
for batch in nsetick.iter_batches("CASH_Orders_25012022.DAT.gz", where="series == 'EQ'"):
    ...  # a pyarrow.RecordBatch
```

### Filters

Filters run before any data is decoded, so selective ones are nearly free:

```text
series == 'EQ' and activity_type == 1                     new EQ orders
txn_time >= '09:15:00' and txn_time < '09:30:00'          a time window
mkt_order_flag == true or ioc_flag == true                aggressive orders
volume_original > volume_disclosed and volume_disclosed > 0    iceberg orders
symbol not in ('IDEA', 'YESBANK')
```

Prices are in paise: `limit_price > 250000` means above ₹2,500. A misspelt field name is an
error before the file is opened, not an empty result an hour later. See the
[Filter Language](https://github.com/arnav127/nsetick/wiki/Filter-Language).

### Rebuild the order book

From parsed orders, a snapshot of each symbol's book every second, 20 levels deep:

```bash
nsetick book data/parquet/segment=cm/kind=orders/date=2022-01-25 --out data/books --interval 1 --levels 20
```

```python
nsetick.build_books("data/parquet/segment=cm/kind=orders/date=2022-01-25",
                    out="data/books", interval_secs=1.0, levels=20, symbols=["TCS", "INFY"])
```

Each snapshot row has best bid and ask, spread, price and quantity per level, the **hidden**
quantity behind iceberg orders at each level, the make-up of the best quote (algorithmic,
institutional, iceberg), and counts of what happened since the previous snapshot. See
[Order Book Reconstruction](https://github.com/arnav127/nsetick/wiki/Order-Book-Reconstruction).

The book follows NSE's rules, including the non-obvious ones: the pre-open call auction,
market orders, IOC, stop-loss, iceberg tranches and their priority, self-trade prevention, and
the post-close session. The
[Matching Engine](https://github.com/arnav127/nsetick/wiki/Matching-Engine) page explains each
step with diagrams.

### Check the book against the exchange

The trade file names the buy and sell order of every trade. `replay_fills` gives you the same
for the rebuilt book, so you can compare them directly:

```python
fills = nsetick.replay_fills("data/parquet/segment=cm/kind=orders/date=2022-01-25", symbols=["TCS"])
```

Over 24 sessions and 10 large stocks, **every one of the exchange's 33 million trade records**
is reproduced: the same two orders, the same price, the same quantity. See
[Validation and Accuracy](https://github.com/arnav127/nsetick/wiki/Validation-and-Accuracy).

### Keep a study reproducible

Put the runs in a JSON file under version control instead of shell history:

```json
{
  "defaults": { "out": "data/parquet", "where": "series == 'EQ'", "note": "expiry-day study" },
  "jobs": [
    { "input": "data/raw/CASH_Orders_27012022.DAT.gz" },
    { "input": "data/raw/CASH_Trades_27012022.DAT.gz" }
  ]
}
```

```bash
nsetick run study.json --dry-run    # check what will run
nsetick run study.json
```

Each output directory gets a manifest recording the source file, filter, columns and your note.
For a spec that uses every option, see
[Advanced Run Spec](https://github.com/arnav127/nsetick/wiki/Advanced-Run-Spec).

---

## Supported data

| Segment | Orders | Trades | Index |
|---|---|---|---|
| Capital Market (`CASH_*`) | ✓ | ✓ | ✓ |
| Futures & Options (`FAO_*`) | ✓ (2022 layouts verified) | ✓ (2022 layouts verified) | |
| Currency Derivatives (`CD_*`) | from the specification | from the specification | |

Order book reconstruction is available for Capital Market orders. The files differ between
segments and have changed over the years. nsetick picks the right layout by date and checks it
against the file, and refuses the file rather than guessing if they disagree.

## Speed

A full Capital Market session parses in a few minutes on a laptop, 3–4× faster than DuckDB's
fixed-width parsing, and uses about 2 MB of memory per output partition. Book reconstruction
for a full session takes about 8 minutes on 8 cores. See
[Performance and Memory](https://github.com/arnav127/nsetick/wiki/Performance-and-Memory).

## Help

- [Troubleshooting](https://github.com/arnav127/nsetick/wiki/Troubleshooting) covers the common
  errors and what they mean.
- Found a bug or a file nsetick won't read? [Open an issue](https://github.com/arnav127/nsetick/issues)
  with the file name and the output of `nsetick --version`.

## Data licence

NSE historical data is licensed. nsetick contains no data and none can be shared with it.
Obtain the files through your institution's NSE subscription.

## Licence

MIT. See [LICENSE](LICENSE).
