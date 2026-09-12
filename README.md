# nsetick

Fast, correct parsing of NSE historical order and trade data into Parquet.

NSE ships its historical tick data as gzipped fixed-width text. A single session of Capital
Market orders is 8.3 GB compressed, 56 GB decompressed, and 684 million records. `nsetick`
turns that into partitioned Parquet at around 280 MB/s, and is designed to be the one parser
shared across every project that touches this data.

## Why it exists

Byte offsets for these layouts are easy to get subtly wrong, and wrong in ways that produce
plausible-looking output rather than errors. `nsetick` puts every offset in one versioned
spec, validates it structurally, and refuses to guess when a file does not match.

## Install

```bash
cargo build --release
./target/release/nsetick --help
```

## Use

```bash
# Parse a session. Layout and date are inferred from the file name.
nsetick parse CASH_Orders_27012022.DAT.gz --out ./parquet

# Only the columns you need, only the rows you want.
nsetick parse CASH_Orders_27012022.DAT.gz --out ./parquet \
  --select symbol,txn_time,limit_price,volume_original \
  --where "series == 'EQ' and symbol in ('RELIANCE','TCS','M&M')"

# What layouts exist, and what is in one.
nsetick layouts
nsetick describe cm_orders --date 2022-01-27

# Decode the first few records without writing anything.
nsetick inspect CASH_Orders_27012022.DAT.gz --n 5
```

Read the result from anywhere:

```sql
SELECT * FROM read_parquet('parquet/**/*.parquet', hive_partitioning = 1)
WHERE symbol = 'RELIANCE';
```

## Filter language

```text
expr       := or_expr
or_expr    := and_expr ('or' and_expr)*
and_expr   := unary ('and' unary)*
unary      := 'not' unary | '(' expr ')' | comparison
comparison := field op literal | field ['not'] 'in' '(' literal, ... ')'
op         := '==' | '=' | '!=' | '<' | '<=' | '>' | '>='
```

Filters compile to byte comparisons at resolved offsets and run before any column is built,
so a rejected record costs almost nothing. Field names are checked at compile time: a typo is
an error before the file is opened, not an empty result afterwards.

Prices compare in raw integer units, so `limit_price > 250000` means above 2500.00 rupees in
the Capital Market segment. Time literals must be quoted and resolve through the session
date: `txn_time >= '09:15:00'`.

## Output layout

```text
<root>/segment=cm/kind=orders/date=2022-01-27/symbol=RELIANCE/part-000.parquet
<root>/_manifest.cm_orders.2022-01-27.json
```

Partitioning by symbol is free: NSE writes each symbol's records contiguously and in time
order, so the partitions come out time-sorted with no sort step. Every run writes a manifest
recording the source file, layout version, filter, projection and row counts, so a directory
of Parquet can be traced back to what produced it.

## Layouts

| layout | segment | record length |
|---|---|---|
| `cm_orders` | CM | 87 |
| `cm_trades` | CM | 100 |
| `cm_index` | CM | 38 (unverified) |
| `fao_orders` | FAO | 112, 111 before spec 1.7 |
| `fao_trades` | FAO | 124, 123 before 2020-09-07 |
| `cd_orders` | CD | 112, 111 before spec 1.7 (unverified) |
| `cd_trades` | CD | 123 (unverified) |

Layout versions are selected by session date and then cross-checked against the record length
observed in the file, which wins if the two disagree. Unverified layouts are transcribed from
the specification but not yet checked against a real file.

See [`spec/README.md`](spec/README.md) for the quirks of the real data: the literal `b`
padding byte, symbols containing `&`, the scale-4 prices in Currency Derivatives, and the
fact that Capital Market files are internally split by symbol range.

## Layout

```text
spec/layouts/*.toml     the single source of truth for every byte offset
crates/nsetick-core     layout registry, decoder, filter engine
crates/nsetick-io       gzip reader, partitioned Parquet writer, manifests
crates/nsetick-cli      the nsetick binary
python/nsetick          reads the same TOML specs; reference decoder
```

## License

MIT
