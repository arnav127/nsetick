# Filter Language

The `--where` / `where=` argument selects records. Filters compile to byte comparisons at fixed
offsets and run before any column is decoded, so a rejected record costs almost nothing, and a
selective filter runs at the speed of decompression.

## Grammar

```text
expr       := or_expr
or_expr    := and_expr ('or' and_expr)*
and_expr   := unary ('and' unary)*
unary      := 'not' unary | '(' expr ')' | comparison
comparison := field op literal
            | field op field
            | field ['not'] 'in' '(' literal, ... ')'
op         := '==' | '=' | '!=' | '<' | '<=' | '>' | '>='
```

Keywords (`and`, `or`, `not`, `in`) are case-insensitive. Strings use single quotes.

## Examples

```text
series == 'EQ'
symbol in ('RELIANCE', 'TCS', 'M&M')
series == 'EQ' and activity_type == 1
buy_sell == 'B' and limit_price >= 250000
txn_time >= '15:00:00' and txn_time < '15:30:00'
not (mkt_order_flag == true)
volume_original > volume_disclosed and volume_disclosed > 0
```

The last selects disclosed-quantity ("iceberg") orders.

## Rules worth knowing

**Field names are checked before the file is opened.** A typo is an error immediately, not an
empty result an hour later. `nsetick.check_filter(expr, layout)` checks without any data.

**Prices compare in raw integer units.** In the Capital Market and FAO segments prices are in
paise, so `limit_price > 250000` means above ₹2,500.00. Currency Derivatives prices have four
decimal places.

**Times are quoted and resolve through the session date:** `txn_time >= '09:15:00'`.

**Two fields of the same record can be compared,** which pushes a derived condition into the
scan. Both sides must have the same scale, so a price cannot be compared against a share count
by mistake.

**Strings compare against the field with its padding removed.** A literal longer than the
field is rejected at compile time, since it could never match.

**Booleans** (`mkt_order_flag`, `ioc_flag`, `stop_loss_flag`) compare against `true` / `false`.

## Useful fields

Capital Market orders (`cm_orders`):

| Field | Values |
|---|---|
| `activity_type` | 1 entry, 3 cancel, 4 modify |
| `buy_sell` | `'B'`, `'S'` |
| `series` | `'EQ'`, `'BE'`, `'SM'`, … |
| `volume_original`, `volume_disclosed` | total and displayed quantity |
| `limit_price`, `trigger_price` | paise |
| `mkt_order_flag`, `ioc_flag`, `stop_loss_flag` | booleans |
| `algo_indicator` | 0 algorithmic, 1 non-algorithmic, 2 algorithmic via smart order routing, 3 non-algorithmic via smart order routing |
| `client_identity` | 1 custodian, 2 proprietary, 3 neither custodian nor proprietary |

Run `nsetick describe <layout> --date <date>` for the full list of any layout.
