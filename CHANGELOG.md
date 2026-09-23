# Changelog

## 0.2.0

Order book reconstruction now reproduces the exchange's own trades. **Books built with 0.1.0
differ from books built with this release**, in depth, spreads and fills; rebuild any
snapshots a study depends on.

The replay is now checked trade by trade: every trade it generates names a buy order and a
sell order, exactly as NSE's trade file does. Over 24 sessions of 2022 and 10 large
securities (33.3 million trades), it reproduces **94.0%** of the exchange's trades exactly
(same two orders, price and quantity), and **99.2%** of orders execute exactly the quantity
the exchange executed for them. Total volume is within 0.2%. 0.1.0 matched 91–97% of volume
for most securities and over 120% for some, and was not checked order by order.

### Fixed

- **Market orders are matched.** They carry a limit price of zero in the feed and were
  rejected as unusable, dropping 3.4% of all entries and leaving the liquidity they consumed
  in the book.
- **IOC and market remainders no longer rest.** The unfilled part is discarded on entry; the
  cancel the feed writes for it microseconds later is recognised.
- **Stop-loss orders wait for the exchange's trigger.** They are held off the book until the
  feed's second entry record for the order, which is the exchange reporting the trigger. A
  modify clearing the stop flag converts the order; a modify setting it takes a live order
  off the book again.
- **Self-trade prevention is honoured.** NSE cancels a resting order when an incoming order
  from the same client reaches it. The feed records that cancel one clock tick (1/65536 s)
  per step of the incoming order's sweep. The replay reads 100 ms ahead and withdraws a
  resting order whose cancel falls in the slot of the step that reached it, if the two orders
  share a participant category.
- **A same-price quantity reduction keeps queue priority.** Other modifies remain
  cancel-then-enter.
- **One trade per pair.** Consecutive fills of the same two orders at one price (an incoming
  order working through an iceberg's tranches) are one trade, as in the exchange's file.
- `Fill.from_hidden` was never set; it now marks fills from a tranche revealed after entry.
- The data-gated Python tests assumed an orders file and failed on a trades file.
- Python 3.9 and 3.10 failed to import the layout module (`tomllib` is 3.11+); they now use
  `tomli`.

### Added

- `nsetick.replay_fills()`: every trade the replay generates, as an Arrow table with the trade
  file's column names, for joining against it.
- `tools/verify_replay.py`: compares the replay with the parsed trade file, per session and
  security (exact trades, order pairs, per-order quantity, volume).
- The wiki, including a step-by-step description of the matching engine.
- `BookStats` counters: `market_orders`, `unrested_quantity`, `remainder_cancels`,
  `stops_held`, `stops_triggered`, `amended_in_place`, `self_trade_preventions`.
- `OrderBook::announce`, `pending_stops`, `last_trade_price`; `Fill::aggressor`, `buy_order`,
  `sell_order`; `stream::SymbolReplay`, the per-symbol replay driver both input paths share.
- `examples/dump_fills`, which writes every fill as CSV.
- LICENSE, continuous integration, and release builds of the CLI and Python wheels.

### Compatibility

The order-type flags (`ioc_flag`, `mkt_order_flag`, `stop_loss_flag`, `trigger_price`) are
optional replay inputs. Parquet written by 0.1.0 without them still replays, with the 0.1.0
behaviour for those order types.

## 0.1.0

Initial release: versioned layout specs, the Arrow decoder with filter pushdown, partitioned
Parquet writing, book reconstruction, the CLI and Python bindings.
