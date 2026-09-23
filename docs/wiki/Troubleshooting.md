# Troubleshooting

**"observed record length N matches no known version … Refusing to guess"**
The file's records are not the length any version of the chosen layout specifies. Usually the
layout is wrong: check the file still has its original NSE name (layout and date come from it),
or that you did not pass an orders layout for a trades file. If the file is genuine and the
length is new, the exchange changed the layout; please open an issue with the output of
`nsetick inspect FILE --n 3`.

**"warning: … date … selects spec X but records are NB; using spec Y"**
Not an error. The layout changed around that date and the file's record length decided which
version applies, as intended.

**"unknown field …" in a filter or `select`**
Field names are checked before any data is read. The message lists the valid names; see also
`nsetick describe <layout>`. Remember trades and orders have different fields: `limit_price` is
an orders field, `trade_price` a trades field.

**The run stops with a memory message**
nsetick stopped rather than run the machine out of memory. Follow the suggested
remedies: filter to fewer symbols, `--partition-by none`, fewer `--threads`, or raise
`--memory-limit-mb` if you know more memory is available. See
[Performance and Memory](Performance-and-Memory).

**A filter returns no rows**
- Prices are integer paise: `limit_price > 2500` means above ₹25.00, not ₹2,500.
- Symbols are exact and case-sensitive: `'BAJAJ-AUTO'`, `'M&M'`.
- With `max_records` / `--max-records`, only the start of the file is read, and the start
  covers only early-alphabet symbols. `TCS` is not in the first few million records.

**Times look five and a half hours off**
They were localised to UTC somewhere. nsetick timestamps are IST wall clock with no timezone;
treat them as naive local times.

**Only part of a session came back (when using another tool)**
CM files are concatenated gzip members. Tools that read only the first member return a fraction
of the data silently. nsetick reads every member.

**`build_books` says a column is missing**
The parsed input was written with a narrow `--select`. Book replay needs `symbol, txn_time,
order_number, activity_type, buy_sell, limit_price, volume_disclosed, volume_original,
algo_indicator, client_identity`, and uses `ioc_flag, mkt_order_flag, stop_loss_flag,
trigger_price` when present. Parse orders with all fields (the default).

**Order books differ from an earlier run**
Check the version. 0.2.0 changed the matching rules; see the
[changelog](https://github.com/arnav127/nsetick/blob/main/CHANGELOG.md).

**`is_crossed` is `Y`**
The best bid is at or above the best ask, which a correct replay should never produce. Please
open an issue with the symbol, date and snapshot time.

**macOS refuses to run the downloaded binary**
`xattr -d com.apple.quarantine ./nsetick`

**`pip` finds no matching wheel**
Wheels cover Python 3.9 and newer on Linux (x86_64, aarch64; glibc 2.17+), macOS (Intel,
Apple silicon) and Windows (x86_64). On anything else, install from source; see
[Installation](Installation#from-source).

## Reporting a problem

Open an issue at <https://github.com/arnav127/nsetick/issues> with the `nsetick` version, the
command or code, the full error, and — if the problem is in the data — the output of
`nsetick inspect FILE --n 5`. Please do not attach NSE data files; they are licensed.
