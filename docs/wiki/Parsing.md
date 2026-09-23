# Parsing

## The command line

```text
nsetick parse [OPTIONS] --out <OUT> <INPUT>
```

```bash
# Everything, partitioned by symbol, layout and date from the file name
nsetick parse CASH_Orders_25012022.DAT.gz --out data/parquet

# Only some columns and rows
nsetick parse CASH_Orders_25012022.DAT.gz --out data/parquet \
  --select symbol,txn_time,limit_price,volume_original \
  --where "series == 'EQ' and symbol in ('RELIANCE','TCS','M&M')"
```

| Option | Default | Meaning |
|---|---|---|
| `-o, --out` | required | Output root directory. |
| `--layout`, `--date` | from file name | Override the inferred layout or session date. |
| `--select` | all fields | Comma-separated fields to emit. |
| `--where` | none | [Filter expression](Filter-Language). |
| `--partition-by` | `symbol` | Column to partition on, or `none` for one file per session. |
| `--compression` | `snappy` | `zstd`, `snappy` or `none`. |
| `-j, --threads` | all cores | Decode and write workers. |
| `--memory-limit-mb` | from free memory | Ceiling on the run's memory footprint. |
| `--row-group-rows` | 256000 | Rows per Parquet row group. |
| `--data-page-kb` | 64 | Bytes each column writer buffers per page; matters with thousands of partitions. |
| `--chunk-mb` | 8 | Decompressed megabytes per work unit. |
| `--max-records` | none | Stop after about this many records; for smoke tests. |
| `--lenient` | off | Count malformed records and continue, instead of stopping at the first. |
| `--no-verify` | off | Skip the `.trg` size check. |

Other commands:

```bash
nsetick layouts                              # layouts available
nsetick describe cm_orders --date 2022-01-25 # fields of the version in force on a date
nsetick inspect FILE.DAT.gz --n 5            # decode the first records, write nothing
nsetick book INPUT --out DIR                 # order books; see Order Book Reconstruction
nsetick run spec.json                        # run a JSON spec
```

## Output

```text
<out>/segment=cm/kind=orders/date=2022-01-25/symbol=RELIANCE/part-000.parquet
<out>/_manifest.cm_orders.2022-01-25.json
```

The directory names are Hive partitions, so `segment`, `kind`, `date` and `symbol` become
columns when the dataset is read with partition discovery (DuckDB
`hive_partitioning = true`, `pyarrow.dataset`, Spark, Polars `hive_partitioning=True`).

Partitioning by symbol costs nothing: NSE writes each symbol's records in time order, so each
partition comes out time-sorted without a sort step.

### Manifests

Each run writes a JSON manifest beside the data: the source file and its size, layout and
spec version, filter, projection, compression, the `note` you supplied, and row counts in and
out. A Parquet directory can always be traced back to the command that produced it, which is
worth having when a reviewer asks.

## Run specs

For a study, keep the runs in a JSON file under version control instead of in shell history:

```json
{
  "defaults": {
    "out": "data/parquet",
    "where": "series == 'EQ'",
    "threads": 6,
    "note": "2022 expiry-day study"
  },
  "jobs": [
    { "input": "data/raw/CASH_Orders_27012022.DAT.gz" },
    { "input": "data/raw/CASH_Trades_27012022.DAT.gz" },
    { "input": "data/raw/CASH_Orders_24022022.DAT.gz", "select": ["symbol", "txn_time"] }
  ]
}
```

- Job fields override `defaults`.
- Relative paths resolve against the spec file, so a spec travels with the data it describes.
- Unknown fields are an error, so a misspelt option fails instead of being ignored.

```bash
nsetick run study.json --dry-run   # resolve and print the jobs, run nothing
nsetick run study.json
```

From Python: `nsetick.run_spec("study.json")`.

## Choosing options for a full session

- **Filter early.** `--where "series == 'EQ'"` drops other series before any column is built.
- **Select only what you use.** Fewer columns is less to decode and write.
- **Snappy vs ZSTD.** Snappy writes faster; ZSTD is roughly a third smaller. For data you
  will keep, ZSTD.
- **Parallelise across sessions.** One file decompresses on one thread. On a many-core machine,
  run several sessions at once with `--threads` split between them.
- **Smoke test first.** `--max-records 5000000` checks a spec end to end in seconds.
