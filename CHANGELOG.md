# Changelog

## 0.2.0

Order book reconstruction now matches the exchange's own trade file. **Books built with
0.1.0 differ from books built with this release**, in depth, spreads and fills; rebuild any
snapshots a study depends on.

The replay was checked by comparing the volume it matches with the volume NSE's trade file
records for the same security and session. Against 0.1.0 it matched 91-97% of actual
continuous-session volume for most securities and over 120% for some. It now matches
100.3-102.3% for most, and 108.5% in the worst case examined (YESBANK, 25 January 2022).

### Fixed

- **Market orders are matched.** They carry a limit price of zero in the feed and were
  rejected as unusable, dropping 3.4% of all entries and leaving the liquidity they consumed
  in the book.
- **IOC and market remainders no longer rest.** The unfilled part is discarded on entry; the
  cancel the feed writes for it microseconds later is recognised.
- **Stop-loss orders wait for their trigger.** They are held off the book until the last
  trade reaches the trigger price, instead of resting at their limit on arrival.
- **Self-trade prevention is honoured.** NSE cancels a resting order when an incoming order
  from the same client would match it. The feed carries no client identifier, but it does
  record that cancel a few hundred microseconds later; the replay now reads 1 ms ahead and
  withdraws a resting order the exchange is about to cancel.
- The data-gated Python tests assumed an orders file and failed on a trades file.

### Added

- `BookStats` counters: `market_orders`, `unrested_quantity`, `remainder_cancels`,
  `stops_held`, `stops_triggered`, `self_trade_preventions`.
- `OrderBook::announce`, `pending_stops`, `last_trade_price`; `stream::SymbolReplay`, the
  per-symbol replay driver both input paths now share.
- `examples/dump_fills`, which writes every fill the replay generates, for joining against
  the trade file.
- LICENSE, continuous integration, and release builds of the CLI and Python wheels.

### Compatibility

The order-type flags (`ioc_flag`, `mkt_order_flag`, `stop_loss_flag`, `trigger_price`) are
optional replay inputs. Parquet written by 0.1.0 without them still replays, with the 0.1.0
behaviour for those order types.

## 0.1.0

Initial release: versioned layout specs, the Arrow decoder with filter pushdown, partitioned
Parquet writing, book reconstruction, the CLI and Python bindings.
