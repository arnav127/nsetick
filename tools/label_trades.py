"""Label each of the exchange's trades with the order event that caused it.

The input for `examples/oracle_diff`, which replays a security event by event and stops at
every point where the matching rules trade differently from the exchange.

The aggressor is the side whose latest entry or modify at or before the trade is the more
recent; the trade belongs to that event. Trades before 09:15 are the pre-open call auction and
are written with kind 'A'. Output columns, no header:
kind, event_ts, aggressor, resting, price, quantity, trade_number, trade_ts, buy, sell.

    python tools/label_trades.py PARSED_ROOT DDMMYYYY SYMBOL labelled.csv

PARSED_ROOT holds `cash_orders/date=.../symbol=...` and `cash_trades/...`. Requires duckdb.
"""
import sys

import duckdb

root, date, sym, out = sys.argv[1:5]
c = duckdb.connect()
c.execute("SET enable_progress_bar=false")
c.execute(f"""CREATE TABLE ev AS SELECT order_number o, epoch_us(txn_time) ts FROM
    read_parquet('{root}/cash_orders/date={date}/symbol={sym}/*.parquet') WHERE activity_type IN (1, 4)""")
c.execute(f"""CREATE TABLE t AS SELECT trade_number n, epoch_us(txn_time) ts, buy_order_number b,
    sell_order_number s, trade_price p, trade_quantity q, CAST(txn_time AS TIME) < TIME '09:15:00' auction
    FROM read_parquet('{root}/cash_trades/date={date}/symbol={sym}/*.parquet')""")
c.execute("""CREATE TABLE tb AS SELECT t.*, e.ts b_ts FROM t ASOF LEFT JOIN ev e ON e.o = t.b AND e.ts <= t.ts""")
c.execute("""CREATE TABLE tbs AS SELECT tb.*, e.ts s_ts FROM tb ASOF LEFT JOIN ev e ON e.o = tb.s AND e.ts <= tb.ts""")
c.execute(f"""COPY (
    SELECT CASE WHEN auction THEN 'A' WHEN b_ts = s_ts OR b_ts IS NULL OR s_ts IS NULL THEN '?' ELSE 'C' END kind,
           CASE WHEN b_ts > s_ts THEN b_ts ELSE s_ts END event_ts,
           CASE WHEN b_ts > s_ts THEN b ELSE s END aggressor,
           CASE WHEN b_ts > s_ts THEN s ELSE b END resting,
           p, q, n, ts, b, s
    FROM tbs ORDER BY n) TO '{out}' (HEADER false)""")
print(c.execute("""SELECT CASE WHEN auction THEN 'A' WHEN b_ts = s_ts OR b_ts IS NULL OR s_ts IS NULL THEN '?' ELSE 'C' END k,
    COUNT(*) FROM tbs GROUP BY 1 ORDER BY 1""").fetchall())
