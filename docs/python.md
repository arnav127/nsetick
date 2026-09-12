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

## Migrating the existing projects

### BlockCrosser (`stage1_data/duckdb_parser.py`)

The whole module reduces to one call. Replace `parse_cash_file(...)` with:

```python
import nsetick

def parse_cash_file(date_str, category, symbols=None, all_universe=True, force_reparse=False):
    prefix = "CASH_Orders" if category == "cash_orders" else "CASH_Trades"
    src = next(RAW_DATA_DIR.glob(f"{prefix}_{date_str}*.DAT.gz"))

    where = "series == 'EQ'"
    if not all_universe and symbols:
        quoted = ", ".join(f"'{s}'" for s in symbols)
        where += f" and symbol in ({quoted})"

    return nsetick.parse(src, out=PARSED_DATA_DIR, where=where, note=f"stage1 {category}")
```

Two behaviour changes worth knowing:

* The output is partitioned by symbol (`symbol=RELIANCE/part-000.parquet`) rather than one
  flat file per date. Reading is unchanged if you use
  `read_parquet('root/**/*.parquet', hive_partitioning=1)`; each symbol's rows are already
  sorted by `txn_time`, so the CLOB replay no longer needs its own sort.
* `is_iceberg` is **not** computed by nsetick. It is business logic, and belongs in
  BlockCrosser. It is one expression over columns you already have:

  ```python
  df["is_iceberg"] = (df["volume_original"] > df["volume_disclosed"]) & (df["volume_disclosed"] > 0)
  ```

`LTRIM(TRIM(symbol), 'b ')` is no longer needed; padding is handled by the layout.

### ProjectCourse (`stage1_parse/duckdb_parser.py`)

```python
import nsetick
from config.settings import TARGET_SYMBOLS, PARSED_DATA_DIR, RAW_DATA_DIR

def run_parser_for_date(date_str):
    symbols = ", ".join(f"'{s}'" for s in TARGET_SYMBOLS)
    jobs = [
        ("CASH_Orders", f"series == 'EQ' and symbol in ({symbols})"),
        ("CASH_Trades", f"series == 'EQ' and symbol in ({symbols})"),
        ("FAO_Orders",  f"instrument == 'FUTSTK' and symbol in ({symbols})"),
        ("FAO_Trades",  f"instrument == 'FUTSTK' and symbol in ({symbols})"),
    ]
    for prefix, where in jobs:
        for src in sorted(RAW_DATA_DIR.glob(f"{prefix}_{date_str}*.DAT.gz")):
            nsetick.parse(src, out=PARSED_DATA_DIR, where=where)
```

Note `glob` rather than a single file: FAO arrives split into `_01 ... _nn` parts.

Three correctness fixes come for free:

* `REGEXP_EXTRACT(symbol, '[A-Z0-9-]+')` silently truncated `M&M` to `M` and `COX&KINGS` to
  `COX`. Only the ten-symbol universe hid this; widening it would have corrupted data.
* The 0-based schema offsets and 1-based SQL `SUBSTRING` positions were maintained separately
  and drifted. Offsets now exist once.
* `TRY_CAST` turned malformed records into rows of NULLs. nsetick stops instead, and always
  reports `rows_malformed`.

`TARGET_SYMBOLS_RAW` with its hand-written leading spaces can go; filters take bare symbols.

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
