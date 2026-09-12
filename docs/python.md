# Using nsetick from Python

## Install

From a checkout:

```bash
pip install maturin
maturin develop --release        # into the active environment
# or build a wheel to install elsewhere
maturin build --release
pip install target/wheels/nsetick-0.1.0-cp39-abi3-win_amd64.whl
```

The wheel is `abi3`, so one build works on Python 3.9 and every later version.

## Two ways in

### 1. Stream Arrow batches, no Parquet at all

Use this when an analysis wants filtered rows and nothing needs to persist. Records are
decoded a chunk at a time, so memory stays flat regardless of session size, and the GIL is
released while decoding.

```python
import nsetick

for batch in nsetick.iter_batches(
    "data/raw/CASH_Orders_27012022.DAT.gz",
    where="symbol == 'RELIANCE' and activity_type == 1",
    select=["symbol", "txn_time", "buy_sell", "limit_price", "volume_original"],
):
    df = batch.to_pandas()          # a real pyarrow.RecordBatch
    ...
```

Convenience wrappers when the result is known to fit:

```python
df  = nsetick.to_pandas(path, where="symbol == 'M&M'", select=[...], max_rows=100_000)
pl  = nsetick.to_polars(path, where="series == 'EQ'", max_rows=1_000_000)
tbl = nsetick.read_table(path, where="...")   # pyarrow.Table
```

`read_table` holds everything in memory. A full unfiltered CM session is ~684 million rows, so
pass `where=` or `max_rows=`.

### 2. Write partitioned Parquet

Use this for the one-time conversion of a session that several analyses will then read.

```python
result = nsetick.parse(
    "data/raw/CASH_Orders_27012022.DAT.gz",
    out="data/parquet",
    where="series == 'EQ'",
    threads=6,
    note="expiry-day study, all EQ",
)
print(result["rows_emitted"], result["partitions"], result["elapsed_secs"])
```

Or drive a whole study from a JSON spec, which is the reproducible option:

```python
for r in nsetick.run_spec("study.json"):
    print(r["input"], r["rows_emitted"])
```

## Speed

8,000,000 CM order records, filter `series == 'EQ'`, 5 columns, 8-core Windows machine,
best of two runs:

| mode | time | M rows/s | output |
|---|---|---|---|
| `nsetick.iter_batches` -> count | 2.36 s | 3.39 | none |
| `nsetick.read_table` -> Arrow | 2.48 s | 3.22 | none |
| `nsetick.iter_batches` -> pandas per batch | 2.50 s | 3.20 | none |
| `nsetick.parse()` -> Parquet | 3.89 s | 2.06 | 47 MB |
| `nsetick` CLI -> Parquet | 4.14 s | 1.93 | 47 MB |
| DuckDB `read_csv` -> Parquet | 8.18 s | 0.98 | 44 MB |

Two things worth reading off this.

**Calling from Python costs nothing.** `parse()` in-process matches the CLI (the small
difference is process startup), because it is the same Rust pipeline with the GIL released.

**Streaming is faster than writing Parquet**, by about 1.6x, because it skips Parquet
encoding and compression entirely. If an analysis only wants filtered rows in memory, going
through Parquet is pure overhead. Converting each batch to pandas adds about 6%, since Arrow
to pandas is close to a zero-copy view for these types.

How much is left on the table, same fixture:

| | time | M rows/s |
|---|---|---|
| stream, filter matches nothing (inflate floor) | 1.40 s | 5.70 |
| stream, one symbol, 3 columns | 1.50 s | 5.34 |
| stream, all EQ rows, all 17 columns | 4.11 s | 1.95 |

A selective filter runs at essentially the speed of gzip decompression, which is the hard
floor: deflate cannot be decompressed by more than one thread. Note that streaming is
single-threaded by design, so on wide unfiltered reads `parse()` with `threads=` will
overtake it; streaming wins whenever the filter is selective, which is the usual case.

## Introspection

```python
nsetick.layouts()
# ['cm_orders', 'cm_trades', 'cm_index', 'fao_orders', 'fao_trades', 'cd_orders', 'cd_trades']

d = nsetick.describe("cm_orders", date="2022-01-27")
d["record_length"]   # 87
d["fields"][6]       # {'name': 'symbol', 'offset': 38, 'len': 10, 'type': 'Str', 'pad': 'Left', ...}

nsetick.probe("CASH_Orders_27012022.DAT.gz")
# {'layout': 'cm_orders', 'observed_record_length': 87, 'spec_version': '1.0', 'verified': True}
```

Validate a filter before a long run, so a typo fails in milliseconds rather than after an hour:

```python
nsetick.check_filter("series == 'EQ' and symbol in ('M&M')", "cm_orders")   # True
nsetick.check_filter("symobl == 'TCS'", "cm_orders")
# ValueError: unknown field "symobl"; this layout has: activity_type, algo_indicator, ...
```

Check whether a run will fit in memory before starting it:

```python
m = nsetick.memory_estimate(partitions=2000)
m["partitions_that_fit"]   # e.g. 3179 on this machine right now
m["estimated_bytes"]       # ~7.1 GB for 2000 symbol partitions
```

## Values and units

* **Prices are raw integers** in their segment's units, not floats. CM and FAO are paise
  (scale 2); Currency Derivatives is scale 4. The divisor travels in the Arrow field metadata
  rather than being assumed:

  ```python
  t = nsetick.read_table(path, select=["limit_price"], where="symbol == 'M&M'", max_rows=1)
  t.schema.field("limit_price").metadata
  # {b'scale': b'2', b'units': b'10^-2', b'doc': b'In paise'}

  scale = int(t.schema.field("limit_price").metadata[b"scale"])
  rupees = t.column("limit_price").to_pandas() / 10**scale
  ```

  Keep the integers for anything price-comparison based; they are exact where floats are not.

* **Timestamps are timezone-naive IST wall clock.** NSE jiffies decode directly to local
  time; labelling them UTC would shift everything by 5h30m.

* **Symbols keep every character.** `M&M`, `COX&KINGS` and `BAJAJ-AUTO` arrive intact.

* **Volumes are shares** in CM and FAO, and **lots** in CD.

## Derived features: where to compute them

Short answer: **compute features in Python; push *filters* into nsetick.** That split is
where the speed is, and it is measured, not assumed. Same 8,000,000-record fixture.

### A row-wise derived column is essentially free in Python

`is_iceberg` is the canonical example: `volume_original > volume_disclosed and
volume_disclosed > 0`.

| | time | overhead |
|---|---|---|
| stream and decode, no derived column | 2.68 s | baseline |
| + `is_iceberg` via Arrow compute | 2.84 s | +6.0% |
| + `is_iceberg` via Polars | 2.83 s | +5.9% |

A vectorised boolean over an Arrow array runs at hundreds of millions of elements per second,
while decoding runs at a few million rows per second. The derived column is roughly 6% of the
work. Moving that into the parser could not win back more than those 6%, and you would be
reimplementing what Arrow and Polars already do well.

### Filtering on a derived value *is* worth pushing down

The difference is between *computing* a value and *using it to reject rows*. A predicate
evaluated on raw bytes rejects a record before any column is built for it:

| | time | rows |
|---|---|---|
| decode all EQ rows, filter icebergs in Python | 2.69 s | 1,416,691 |
| `where="... and volume_original > volume_disclosed and volume_disclosed > 0"` | **2.15 s** | 1,416,691 |

1.25x, with identical output. The gain is bounded by gzip decompression, which still has to
read every record either way; what it saves is building columns for the 82% of rows that are
discarded. The more selective the condition and the wider the projection, the more it saves.

```python
# Two fields of the same record can be compared directly.
icebergs = nsetick.read_table(
    path,
    where="series == 'EQ' and volume_original > volume_disclosed and volume_disclosed > 0",
    select=["symbol", "txn_time", "limit_price", "volume_original", "volume_disclosed"],
)
```

Both sides must be numeric and share a scale, so comparing a price against a share count is
rejected rather than silently comparing paise to quantities.

### Stateful features belong in Polars

OFI, running sums, rolling means and lagged differences need an ordered series per symbol.
On 800,575 rows already in memory:

| | time |
|---|---|
| nsetick read (scans 8M records, keeps 800,575) | 2.00 s |
| Polars: sort + signed volume + `cum_sum` + `rolling_mean` + `diff` | **0.21 s** |

The whole feature pipeline is 10% of the read. And because nsetick partitions by symbol and
each partition is already in time order, the sort is usually unnecessary.

```python
import polars as pl

df = pl.from_arrow(nsetick.read_table(path, where="symbol == 'RELIANCE'", select=[...]))
qty = pl.col("volume_original").cast(pl.Int64)          # u64 has no negation
df = df.with_columns(
    signed=pl.when(pl.col("buy_sell") == "B").then(qty).otherwise(-qty)
).with_columns(
    ofi=pl.col("signed").cum_sum(),
    roll=pl.col("limit_price").rolling_mean(window_size=100),
)
```

### The rule

nsetick decides **which rows and which columns**. Your code decides **what to compute from
them**. Anything that changes the row count belongs in `where=`; anything that adds a column
belongs downstream. Keeping domain logic out of the parser is also what lets one parser serve
unrelated analyses without accumulating each one's definitions.

## Reference decoder

`nsetick.reference` is a pure-Python decoder reading the same TOML specs. It is far slower
than the Rust core and exists to be obviously correct, for validating a layout against a
handful of records:

```python
import gzip
from nsetick import layout, reference
import datetime as dt

ver = layout.load("cm_orders").for_date(dt.date(2022, 1, 27))
with gzip.open(path, "rb") as f:
    rec = reference.decode_record(next(f).rstrip(b"\n"), ver)
```
