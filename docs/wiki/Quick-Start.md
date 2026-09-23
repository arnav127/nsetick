# Quick Start

From a raw NSE file to a DataFrame, a Parquet dataset and a reconstructed order book. The
examples use `CASH_Orders_25012022.DAT.gz`; substitute any session you have.

## 1. Look before you parse

```python
import nsetick

path = "data/raw/CASH_Orders_25012022.DAT.gz"

nsetick.probe(path)
# {'layout': 'cm_orders', 'session_date': '2022-01-25',
#  'observed_record_length': 87, 'spec_version': '1.0', 'verified': True}
```

`probe` reads a few records, identifies the layout from the file name and checks that the
record length in the file matches that layout's specification. If it does not, parsing
refuses to start rather than producing shifted, plausible-looking columns.

To see what is in the file:

```bash
nsetick inspect data/raw/CASH_Orders_25012022.DAT.gz --n 3
nsetick describe cm_orders --date 2022-01-25     # every field, offset and type
```

## 2. Pull what you need straight into Python

For a question about a handful of securities, there is no need to write anything to disk:

```python
df = nsetick.to_pandas(
    path,
    where="symbol in ('RELIANCE', 'TCS') and activity_type == 1",
    select=["symbol", "txn_time", "buy_sell", "limit_price", "volume_original"],
)
```

The filter runs on the raw bytes before any column is decoded, so reading two securities out
of a 700-million-record session runs at the speed of decompression. A few things to know
about the values:

- **Prices are integer paise** in the Capital Market segment (scale 2): `limit_price == 80525`
  is ₹805.25. Divide by 100. The scale is stored in each price column's metadata.
- **Times are IST wall clock** and carry no timezone. Do not localise them to UTC.
- **`activity_type`**: 1 = new order, 3 = cancel, 4 = modify.
- A field left blank in the file is null, not zero.

For larger reads, stream batch by batch so memory stays flat:

```python
for batch in nsetick.iter_batches(path, where="series == 'EQ'", select=["symbol", "limit_price"]):
    ...  # a pyarrow.RecordBatch
```

## 3. Parse a whole session to Parquet

When you will query the same session repeatedly, parse it once:

```bash
nsetick parse data/raw/CASH_Orders_25012022.DAT.gz --out data/parquet --where "series == 'EQ'"
nsetick parse data/raw/CASH_Trades_25012022.DAT.gz --out data/parquet --where "series == 'EQ'"
```

or from Python, `nsetick.parse(path, out="data/parquet", where="series == 'EQ'")`. The output is
partitioned by symbol:

```text
data/parquet/segment=cm/kind=orders/date=2022-01-25/symbol=RELIANCE/part-000.parquet
```

Query it with anything that reads Parquet. With DuckDB:

```sql
SELECT symbol, COUNT(*) AS trades, SUM(trade_quantity) AS volume
FROM read_parquet('data/parquet/segment=cm/kind=trades/**/*.parquet', hive_partitioning = true)
GROUP BY symbol ORDER BY volume DESC LIMIT 10;
```

A full session takes roughly 30–40 minutes to parse; most of that is decompression, which is
single-threaded per file. Parse several sessions in parallel to use a large machine.

## 4. Reconstruct the order book

```python
report = nsetick.build_books(
    "data/parquet/segment=cm/kind=orders/date=2022-01-25",  # parsed orders, preferred
    out="data/books",
    interval_secs=1.0,     # one snapshot per second
    levels=20,             # price levels per side
    symbols=["RELIANCE", "TCS"],
)
```

Replaying parsed Parquet is much faster than replaying the raw file, because each symbol is
already separated and can be replayed in parallel. A raw `.DAT.gz` also works as `input`.

Each output row is the book at one instant: best bid and ask, price, visible quantity and
hidden quantity at each level, the composition of the touch, and counts of what happened since
the previous snapshot. See [Order Book Reconstruction](Order-Book-Reconstruction) for every
column.

```python
import pandas as pd
books = pd.read_parquet("data/books", filters=[("symbol", "==", "RELIANCE")])
books[["snapshot_time", "best_bid", "best_ask", "bid_qty_1", "ask_qty_1", "bid_hidden_1"]].head()
```

## 5. Make the run reproducible

For a study, describe the runs in a JSON file and keep it with your code:

```json
{
  "defaults": { "out": "data/parquet", "where": "series == 'EQ'", "note": "expiry study" },
  "jobs": [
    { "input": "data/raw/CASH_Orders_25012022.DAT.gz" },
    { "input": "data/raw/CASH_Trades_25012022.DAT.gz" }
  ]
}
```

```bash
nsetick run study.json --dry-run   # show what would run
nsetick run study.json
```

Every run also writes a manifest (`_manifest.*.json`) recording the source file, layout
version, filter, columns and row counts, so any Parquet directory can be traced back to what
produced it.

## Next

- [Python API](Python-API) for every function and option
- [Filter Language](Filter-Language) for what `where=` accepts
- [Data Layouts and Quirks](Data-Layouts-and-Quirks) before you trust any column you have not
  looked at
