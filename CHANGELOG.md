# Changelog

## 0.2.0

Order book reconstruction now reproduces the exchange's own trade records exactly. **Books
built with 0.1.0 differ from books built with this release** in depth, spreads and fills;
rebuild any snapshots a study depends on.

The replay is checked trade by trade against NSE's trade file, which names the buy and sell
order of every execution. Over 24 sessions of 2022 and 10 large securities, it reproduces
**every one of the 33,379,259 trade records of the day**, pre-open auction included: the same
two orders, price, quantity and record boundaries. 0.1.0 matched
91-97% of volume for most securities and over 120% for some, and was not checked order by
order.

### Fixed

- **Market orders are matched.** They carry a limit price of zero and were rejected as
  unusable, dropping 3.4% of all entries.
- **IOC and market remainders no longer rest**; the feed's cancel for them is recognised.
- **Stop-loss orders wait for the exchange's trigger record** (a second entry record for the
  order), and modifies setting or clearing the stop flag are followed.
- **Self-trade prevention.** When an order reaches one of the same client's, the exchange
  cancels one of them, resting or incoming. The replay reads 100 ms ahead and recognises the
  cancel by its exact slot in the match (one jiffy per message, to half a jiffy). A
  prevention cancel ends the current trade record.
- **Disclosed-quantity orders** are tranched as the exchange does: every trade the order takes
  part in counts against its current tranche, as resting order or aggressor; a trade that uses
  the tranche up leaves a fresh full one; a new tranche goes to the back of the queue.
- **Modifies** keep the order's place unless the price changes or the displayed quantity
  grows, and can carry an iceberg's current tranche over (rules in the wiki's Matching Engine
  page).
- **One trade record per pair**: consecutive fills of the same two orders at one price are one
  record, as in the exchange's file.
- `Fill.from_hidden` was never set; it now marks fills from a tranche revealed after entry.
- The data-gated Python tests assumed an orders file and failed on a trades file.
- Python 3.9 and 3.10 failed to import the layout module; they now use `tomli`.

### Added

- **The pre-open call auction**: orders from 09:00 are collected and matched at the
  equilibrium price by NSE's priority, exact in all 240 security-sessions checked. A tie
  between equally good prices is settled from the feed (the exchange's conversion of unfilled
  market orders reveals its price) or, failing that, by `previous_close`.
- **The post-close session**: from 15:40 every order trades at the closing price (the
  15:00-15:30 VWAP truncated to paise and rounded to the tick) in time order.
- `previous_close` for `build_books` and `replay_fills`, and `--previous-close` for
  `nsetick book`.
- `nsetick.replay_fills()`: every trade the replay generates, as an Arrow table with the trade
  file's column names.
- `tools/verify_replay.py`: compares the replay with the parsed trade file per session and
  security. `tools/label_trades.py` and `examples/oracle_diff`: find the first event where the
  replay and the exchange part, with the queue as it stood.
- `OrderBook::queue`, `prices`, `closing_price`, `close_pre_open`, `set_previous_close`,
  `force_next_match` (drive a match from known trades), `announce`, `last_trade_price`;
  `Fill::aggressor`, `buy_order`, `sell_order`; `stream::SymbolReplay`.
- `BookStats` counters: `market_orders`, `unrested_quantity`, `remainder_cancels`,
  `stops_held`, `stops_triggered`, `amended_in_place`, `self_trade_preventions`.
- The wiki, including a step-by-step description of the matching engine and an advanced
  run-spec example (`examples/advanced.json`).
- LICENSE, continuous integration, and release builds of the CLI and Python wheels.

### Compatibility

The order-type flags (`ioc_flag`, `mkt_order_flag`, `stop_loss_flag`, `trigger_price`) are
optional replay inputs. Parquet written by 0.1.0 without them still replays, with the 0.1.0
behaviour for those order types. Book timestamps are the feed's IST wall clock; the session
rules (auction before 09:15, post-close from 15:35) read the time of day from them.

## 0.1.0

Initial release: versioned layout specs, the Arrow decoder with filter pushdown, partitioned
Parquet writing, book reconstruction, the CLI and Python bindings.
