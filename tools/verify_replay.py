"""Check the order book replay against the exchange's trade file.

For each session and security, replays the parsed orders with ``nsetick.replay_fills`` and
compares the result with the parsed trade file, trade by trade:

  exact trades   same buy order, sell order, price and quantity
  order pairs    same buy and sell order, quantity and price summed over the pair
  order volume   each order executes the same total quantity
  volume         total quantity matched, as a share of what the exchange printed

Continuous session only (from 09:15); the pre-open call auction matches by a different rule.

    python tools/verify_replay.py PARSED_ROOT [--symbols TCS INFY] [--dates 2022-01-25 ...]

PARSED_ROOT is the directory ``nsetick parse`` wrote to, holding ``cash_orders`` and
``cash_trades`` (either ``kind=orders`` Hive layout or ``cash_orders/date=...`` layout).
Requires ``duckdb``.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import nsetick


def sessions(root: Path) -> list[tuple[str, Path, Path]]:
    """(date, orders dir, trades dir) for every session with both."""
    layouts = [
        (root / "cash_orders", root / "cash_trades"),
        (root / "segment=cm" / "kind=orders", root / "segment=cm" / "kind=trades"),
    ]
    out = []
    for orders_root, trades_root in layouts:
        for d in sorted(orders_root.glob("date=*")):
            t = trades_root / d.name
            if t.is_dir():
                out.append((d.name.split("=", 1)[1], d, t))
    return out


def verify(orders: Path, trades: Path, symbols: list[str] | None) -> list[tuple]:
    fills = nsetick.replay_fills(str(orders), symbols=symbols)
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    con.register("replay", fills)
    sym_filter = ""
    if symbols:
        quoted = ", ".join("'" + s.replace("'", "''") + "'" for s in symbols)
        sym_filter = f"AND symbol IN ({quoted})"
    con.execute(f"""
        CREATE TABLE a AS SELECT symbol, buy_order_number b, sell_order_number s,
               trade_price p, trade_quantity q
        FROM read_parquet('{trades.as_posix()}/*/*.parquet', hive_partitioning = true)
        WHERE CAST(txn_time AS TIME) >= TIME '09:15:00' {sym_filter}""")
    con.execute("""
        CREATE TABLE r AS SELECT symbol, buy_order_number b, sell_order_number s,
               trade_price p, trade_quantity q
        FROM replay WHERE CAST(txn_time AS TIME) >= TIME '09:15:00'""")
    return con.execute("""
        WITH
        ak AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol, b, s, p, q) k FROM a),
        rk AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol, b, s, p, q) k FROM r),
        exact AS (SELECT ak.symbol, COUNT(*) n, COUNT(rk.k) hit
                  FROM ak LEFT JOIN rk USING (symbol, b, s, p, q, k) GROUP BY 1),
        ap AS (SELECT symbol, b, s, SUM(q) q, SUM(p * q) pq FROM a GROUP BY 1, 2, 3),
        rp AS (SELECT symbol, b, s, SUM(q) q, SUM(p * q) pq FROM r GROUP BY 1, 2, 3),
        pairs AS (SELECT ap.symbol, COUNT(*) n, COUNT(*) FILTER (WHERE ap.q = rp.q AND ap.pq = rp.pq) hit
                  FROM ap LEFT JOIN rp USING (symbol, b, s) GROUP BY 1),
        ao AS (SELECT symbol, o, SUM(q) q FROM (SELECT symbol, b o, q FROM a UNION ALL
                                                SELECT symbol, s, q FROM a) GROUP BY 1, 2),
        ro AS (SELECT symbol, o, SUM(q) q FROM (SELECT symbol, b o, q FROM r UNION ALL
                                                SELECT symbol, s, q FROM r) GROUP BY 1, 2),
        ords AS (SELECT ao.symbol, COUNT(*) n, COUNT(*) FILTER (WHERE ao.q = ro.q) hit
                 FROM ao LEFT JOIN ro USING (symbol, o) GROUP BY 1),
        vol AS (SELECT symbol, SUM(q) q FROM a GROUP BY 1),
        rvol AS (SELECT symbol, SUM(q) q FROM r GROUP BY 1)
        SELECT exact.symbol, exact.n, exact.hit, pairs.n, pairs.hit, ords.n, ords.hit,
               vol.q, COALESCE(rvol.q, 0)
        FROM exact JOIN pairs USING (symbol) JOIN ords USING (symbol) JOIN vol USING (symbol)
        LEFT JOIN rvol USING (symbol) ORDER BY 1""").fetchall()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("root", type=Path)
    ap.add_argument("--symbols", nargs="*")
    ap.add_argument("--dates", nargs="*", help="YYYY-MM-DD or DDMMYYYY, as in the directory names")
    args = ap.parse_args()

    found = sessions(args.root)
    if args.dates:
        found = [x for x in found if x[0] in args.dates]
    if not found:
        print(f"no sessions with both orders and trades under {args.root}", file=sys.stderr)
        return 1

    print(f"{'session':10s} {'symbol':11s} {'trades':>9s} {'exact':>7s} {'pairs':>7s} {'orders':>7s} {'volume':>8s}")
    tot = [0] * 8
    for date, orders, trades in found:
        for row in verify(orders, trades, args.symbols):
            sym, n, hit, pn, phit, on, ohit, vq, rq = row
            print(f"{date:10s} {sym:11s} {n:9,d} {100*hit/n:6.1f}% {100*phit/pn:6.1f}% "
                  f"{100*ohit/on:6.1f}% {100*rq/vq:7.1f}%")
            for i, v in enumerate((n, hit, pn, phit, on, ohit, vq, rq)):
                tot[i] += v
    n, hit, pn, phit, on, ohit, vq, rq = tot
    print(f"\n{'ALL':10s} {'':11s} {n:9,d} {100*hit/n:6.1f}% {100*phit/pn:6.1f}% "
          f"{100*ohit/on:6.1f}% {100*rq/vq:7.1f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
