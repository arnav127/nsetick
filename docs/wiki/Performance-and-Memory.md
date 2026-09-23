# Performance and Memory

## Speed

Against the DuckDB `read_csv` + `SUBSTRING` approach commonly used for these files, on the
same 8,000,000-record extract, same filter (`series == 'EQ'`), same output (one Snappy file,
all 17 columns), 8-core Windows machine:

| | Time | Output |
|---|---|---|
| DuckDB | 22.3 s | 116 MB |
| nsetick, same output | **5.7 s** | 116 MB |
| nsetick, ZSTD, partitioned by symbol | 8.6 s | **72 MB** |

Both produced 7,967,699 rows with identical symbols and identical column checksums.

Full sessions measured on an 8-core laptop, 25 January 2022:

| Task | Time |
|---|---|
| Parse CASH_Orders (704M records, 1,129 symbols) | 32–40 min |
| Parse CASH_Trades (29M records) | about 1 min |
| Book replay from parsed Parquet (497 symbols, 559M events, 1 s snapshots) | about 8 min |

Parsing is bounded by decompression, which runs on one thread per file. On a many-core
machine, parse several sessions at once and divide `--threads` between them.

Streaming into Python is about 1.6x faster than writing Parquet and reading it back, because it
skips Parquet encoding. A selective filter runs at the speed of decompression.

## Memory

Partitioning by symbol keeps one Parquet writer open per symbol for the whole run: NSE
interleaves symbols through the file, and a closed Parquet file cannot be reopened to append.
That, not buffering, decides the footprint:

| Configuration | Peak memory |
|---|---|
| One file, 1 thread | 269 MB |
| One file, 6 threads | 532 MB |
| 620 symbol partitions, 1 thread | 1.56 GB |
| 620 symbol partitions, 6 threads | 2.02 GB |

Roughly **2 MB per open partition**. A full ~2,000-symbol session plans for about 5 GB.

nsetick derives a ceiling from the memory available when it starts and checks it as each
partition opens. A run that cannot fit **stops with an explanation and suggested remedies**
rather than exhausting the machine. Set the ceiling explicitly with `--memory-limit-mb` or
`memory_limit_mb=`, which you should do on a shared or scheduled machine where the free memory
nsetick sees is not all yours. Every run reports what it used:

```text
memory            ~1.5 GB peak of 9.7 GB limit (619 partitions open)
```

To reduce memory: filter to fewer symbols, use `--partition-by none`, use fewer threads, or
lower `--data-page-kb`.

## Disk

Per full Capital Market session, all symbols:

| Output | Snappy | ZSTD |
|---|---|---|
| Parsed orders and trades | about 8 GB | about 5.2 GB |
| Book snapshots, 5 s interval, 20 levels | about 1 GB | about 0.65 GB |

Snapshot volume scales with 1/interval and with the number of levels.
