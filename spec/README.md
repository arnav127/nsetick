# NSE layout specifications

The TOML files in `layouts/` are the **single source of truth** for every byte offset in
nsetick. The Rust core and the Python package both read them. No offset is written down
anywhere else.

Authority: *Historical Data Orders and Trade Layout*, version 1.10, 27 July 2022, NSE Data &
Analytics Ltd. Each file cites the section it was derived from.

Run `python spec/validate_layouts.py` to check the specs are structurally sound: fields
contiguous, no gaps or overlaps, lengths summing to the declared record length, no two
versions of a layout sharing a record length or an overlapping date range.

## Quirks of the real data

These are the things that bite. Each was confirmed against actual NSE files, not assumed.

### `b` is the blank padding byte

The layout document denotes a blank with the letter `b` — it says symbol `ABC` is written
`"bbbbbbbABC"`. The file generator emitted **the notation rather than the character it stands
for**, so the data contains literal `b` (0x62) bytes as fill:

```
POCASH100000000000209487014847291579S1bbbbbbbBCGBE0000000000000075...
                                      ^^^^^^^ literal 'b' bytes
```

Real 0x20 spaces also occur in some files, so nsetick strips both. It strips **nothing else**.

The same applies to the `segment` field, which the spec writes as `"FAOb"` and `"CDSb"` —
padding is a property of a field, not a quirk of the symbol column.

### Symbols contain characters outside `[A-Z0-9]`

`&`, `-`, `*` and `.` are all legal. In the first 3M records of a single session there are
~46,000 symbols containing `-` and 140 containing `&` (e.g. `COX&KINGS`, `M&M`, `J&KBANK`).
Cleaning a symbol with a regex such as `[A-Z0-9-]+` truncates `M&M` to `M` **silently**.
Strip the pad bytes from the left; keep everything else verbatim.

### Prices are scale 2 in CM/FAO but scale 4 in CD

Currency Derivatives encodes `12.3456` as `"00123456"`. A shared `paise_to_rupees()` helper
that divides by 100 overstates every CD price by 100x. The divisor lives on the field, as
`scale`, and `reference.scale_price()` uses it.

### Record layouts change over time

* **FAO Trades**: trade number was 16 bytes (123-byte record) until 2020-09-04 and is 17
  bytes (124-byte record) from 2020-09-07. Every field after the trade number shifts by one.
* **FAO Orders / CD Orders**: spec 1.7 (Dec 2021) appended `limit_price_ind`, taking the
  record from 111 to 112 bytes.

nsetick selects a version by date and then **cross-checks it against the observed record
length**, preferring the observed length and warning loudly on disagreement. The revision
dates in the PDF are revision dates, not necessarily feed changeover dates, so the boundaries
are approximate by nature; the record length is ground truth.

### Volumes mean different things per segment

CM and FAO volumes are in **shares**. CD volumes are in **lots (contracts)**. The spec is
explicit that FAO quantities "do not represent No of Contracts".

### Series values exceed the documented list

The document lists ~40 series codes. A single 2022 session also contains `E1`, `BZ`, `RR`,
`SM`, `GS` and others. `series` is stored verbatim and never validated against a whitelist.

### FAO files are split

FAO orders and trades arrive as `FAO_Orders_DDMMYYYY_01.DAT.gz` … `_nn.DAT.gz`. Contracts do
not overlap across the parts, so the parts can be processed independently and concatenated.

### CM files are internally split by symbol range

The layout document says only FAO files are split into streams. In practice a single
`CASH_Orders_DDMMYYYY.DAT.gz` is a **concatenation of gzip members**, each covering a range of
symbols and each internally ordered by time across the whole session.

Measured on `CASH_Orders_27012022.DAT.gz` (684,079,700 records, 56 GB decompressed):

* the first 30M records cover 09:00:00 to 10:00:17 but only 625 distinct symbols, spanning
  `1018GS2026` through `CYIENT`;
* every individual symbol nevertheless spans the full session, 09:00 to 15:59.

Two consequences:

1. A plain `GzDecoder` stops at the end of the first member and silently returns a fraction
   of the session with no error. nsetick uses `MultiGzDecoder`.
2. Each symbol's records are contiguous and in time order, so partitioning the output by
   symbol produces time-sorted partitions with no sort step. Verified: zero out-of-order rows
   across all seven symbols in a full-session run.

Note also that records continue past the 15:30 close to about 15:59, so a naive
"market hours" filter of 09:15-15:30 discards real data.

### `.trg` trigger files

Each `.DAT.gz` has a sibling `.DAT.gz.trg` containing an MD5 and a byte count, available for
files from 2020-12-01. nsetick can verify against these rather than merely skipping them.

### Unverified layouts

`verified = false` on a layout version means it is transcribed from the PDF but has not yet
been checked against a real file, because no such file was available locally. Currently: all
CD layouts and the pre-changeover FAO and CD variants.

`fao_orders@1.7` and `fao_trades@1.2` have now been confirmed against real April 2022 files.
The checks that settle it, on `FAO_Trades_28042022_01.DAT.gz`:

* `trade_number` runs unbroken from `20220428000000001` to `20220428002013270` across
  2,013,270 records, which only holds if the 17-byte field and every following offset are
  right and no record was skipped or double counted;
* every trade quantity is divisible by 50, the NIFTY lot size, with zero exceptions, which
  confirms both the quantity offset and the spec's claim that FAO volumes are shares rather
  than contracts;
* strikes land in 14350.00 to 19550.00 rupees and expiry decodes to 2022-04-28, the Thursday
  expiry, from the `ddMMMyyyy` field.

On the orders side `limit_price_ind` decodes as a clean Y/N and `segment` arrives as `FAOb`,
confirming the spec-1.7 112-byte record and the trailing-pad handling.

`cm_index` carried a **spec contradiction**: field lengths summing to 38 bytes against a
"Total Length" cell of 24. Real files settle it at 38, and the decoded values are right
(Nifty 50 at 17,437.80 for the 2022-04-01 open), so the document's total is an error.
