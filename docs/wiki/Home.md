# nsetick

**Fast, correct parsing of NSE historical order and trade data, and reconstruction of the
limit order book from it.**

NSE distributes its historical tick data as gzipped fixed-width text. One session of Capital
Market orders is about 8 GB compressed, 56 GB decompressed and 700 million records, and the
byte layout differs between segments and has changed over time. `nsetick` turns those files
into Parquet or streams them straight into Python as Arrow, and rebuilds the order book by
replaying every order event under NSE's matching rules.

It is a command-line tool and a Python package, built on a Rust core.

## What it does

| | |
|---|---|
| **Parse** | `.DAT.gz` to partitioned Parquet, 3–4x faster than DuckDB fixed-width parsing, with a filter language that runs before any column is built |
| **Stream** | Arrow batches into pandas, Polars or DuckDB without writing to disk, at constant memory |
| **Reconstruct books** | Periodic L2 snapshots from order-level events, honouring disclosed quantity, market, IOC and stop-loss orders, and self-trade prevention |
| **Verify** | Every byte offset lives in one versioned spec, checked against each file's record length; the replay reproduces 94% of NSE's trades exactly, order for order |

## Start here

1. [Installation](Installation) — prebuilt binary or `pip install`, no Rust needed
2. [Quick Start](Quick-Start) — from a raw file to a DataFrame and an order book in ten minutes
3. [Python API](Python-API) — every function
4. [Parsing](Parsing) — the command line, output layout and reproducible run specs

## Reference

- [Filter Language](Filter-Language)
- [Advanced Run Spec](Advanced-Run-Spec) — one large spec using every option and filter feature
- [Order Book Reconstruction](Order-Book-Reconstruction) — snapshots and their columns
- [Matching Engine](Matching-Engine) — how the replay matches orders, step by step, with diagrams
- [Data Layouts and Quirks](Data-Layouts-and-Quirks) — what the raw files actually contain
- [Validation and Accuracy](Validation-and-Accuracy) — how correctness was checked, and the known limits
- [Performance and Memory](Performance-and-Memory)
- [Troubleshooting](Troubleshooting)
- [Releasing](Releasing) — for maintainers

## Supported data

| Segment | Orders | Trades | Index |
|---|---|---|---|
| Capital Market (CM) | verified | verified | verified |
| Futures and Options (FAO) | verified for 2022 | verified for 2022 | — |
| Currency Derivatives (CD) | spec-transcribed | spec-transcribed | — |

"Verified" means checked against real files; earlier FAO layout versions are transcribed from
the specification only. Order book reconstruction currently supports
Capital Market orders.

## A note on the data

NSE historical data is licensed. `nsetick` contains no data and none can be distributed with
it; obtain the files through your institution's NSE data subscription.

## Citing

If you use `nsetick` in published work, please cite the repository:
`Dixit, A. nsetick: parsing and order book reconstruction for NSE historical tick data.
https://github.com/arnav127/nsetick`, with the version you used (`nsetick --version`). Book
reconstruction changed materially in 0.2.0; see the
[changelog](https://github.com/arnav127/nsetick/blob/main/CHANGELOG.md).
