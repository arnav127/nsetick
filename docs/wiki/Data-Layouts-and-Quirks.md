# Data Layouts and Quirks

## Layouts

| Layout | File name | Record length |
|---|---|---|
| `cm_orders` | `CASH_Orders_DDMMYYYY.DAT.gz` | 87 |
| `cm_trades` | `CASH_Trades_DDMMYYYY.DAT.gz` | 100 |
| `cm_index` | `CASH_Index_DDMMYYYY.DAT.gz` | 38 |
| `fao_orders` | `FAO_Orders_DDMMYYYY_nn.DAT.gz` | 112 (111 before spec 1.7, Dec 2021) |
| `fao_trades` | `FAO_Trades_DDMMYYYY_nn.DAT.gz` | 124 (123 before 7 Sep 2020) |
| `cd_orders` | `CDS_Orders_DDMMYYYY.DAT.gz` | 112 (111 before spec 1.7) |
| `cd_trades` | `CDS_Trades_DDMMYYYY.DAT.gz` | 123 |

Every byte offset lives in one place: `spec/layouts/*.toml` in the repository, derived from
NSE's *Historical Data Orders and Trade Layout* v1.10. The Rust core and the Python package
both read these files; no offset is written anywhere else.

```bash
nsetick describe cm_orders --date 2022-01-25
```

prints every field with its offset, length, type and meaning.

**Versions are chosen by date and checked against the file.** When a layout changed, both
versions are kept with their date ranges. nsetick picks the version for the session date,
then compares its record length with the length actually observed in the file. If they
disagree, the observed length wins and a warning says so. A file matching no known version is
refused. This matters because parsing with the wrong version shifts every later field by a
byte, which produces wrong but plausible numbers rather than an error.

## Quirks of the real files

Each of these was found in real data, and each has caught somebody out.

**The padding byte is a literal `b`.** The specification writes a blank as `b` (symbol `ABC`
is shown as `bbbbbbbABC`), and the files contain the letter `b` (0x62), not spaces. Some files
also contain real spaces. nsetick strips both, and nothing else.

**Symbols contain `&`, `-`, `*` and `.`.** `M&M`, `J&KBANK`, `BAJAJ-AUTO`. Cleaning symbols
with a regular expression such as `[A-Z0-9-]+` truncates `M&M` to `M` without any error.

**Price scales differ by segment.** Capital Market and FAO prices have 2 decimal places; Currency
Derivatives prices have 4. Dividing every price by 100 overstates CD prices a hundredfold. Each
price column carries its `scale` in the Arrow field metadata.

**Quantities differ by segment.** CM and FAO quantities are shares; CD quantities are lots.

**Times are jiffies.** Timestamps count 1/65536-second intervals from 1980-01-01 and decode
directly to IST wall-clock time. nsetick returns them as timestamps without a timezone. Do not
localise them to UTC; that shifts everything by five and a half hours.

**Records run past the close.** Orders and trades continue to about 15:59, not 15:30. A
09:15–15:30 filter discards real records.

**The pre-open session is in the same file.** Orders from 09:00 onward include the pre-open
call auction, matched at a single price around 09:08. Exclude it from intraday return series,
or the first continuous observation is differenced against the auction price.

**CM files are concatenated gzip streams split by symbol range.** A single
`CASH_Orders_*.DAT.gz` is several gzip members, each covering a range of symbols for the whole
day. A standard single-member gzip reader stops at the end of the first and returns a fraction
of the session with no error. Consequences:

- The first records of a file cover only early-alphabet symbols; `head` of a file is not a
  sample of the market.
- Each symbol's records are contiguous and in time order, so output partitioned by symbol is
  already time-sorted.

**FAO files are split** into `_01`, `_02`, … parts. Contracts do not overlap across parts.

**Series codes go beyond the documented list.** `E1`, `BZ`, `RR`, `SM`, `GS` and others
appear. `series` is kept verbatim.

**Order numbers can be reused within a session.** A number belonging to one order can
reappear on an unrelated order later the same day. Key on `(symbol, order_number)` and, when
joining trades to orders, restrict to the order's own lifetime.

**A modify can increase an order's quantity.** Computing a fill rate against the quantity at
entry can then exceed 100%. Use the largest quantity the order ever carried as the
denominator.

**`.trg` files** beside each `.DAT.gz` (from December 2020) carry the expected size and an MD5.
nsetick checks the size by default; `--no-verify` skips it.

The full notes, with the measurements behind each item, are in
[`spec/README.md`](https://github.com/arnav127/nsetick/blob/main/spec/README.md).

## Codes

| Field | Codes |
|---|---|
| `activity_type` (orders) | 1 entry, 3 cancel, 4 modify |
| `buy_sell` | `B`, `S` |
| `algo_indicator` | 0 algorithmic, 1 non-algorithmic, 2 algorithmic via SOR, 3 non-algorithmic via SOR |
| `client_identity` | 1 custodian, 2 proprietary, 3 neither |
| `mkt_order_flag`, `stop_loss_flag`, `ioc_flag` | `Y`/`N`, decoded as booleans |
