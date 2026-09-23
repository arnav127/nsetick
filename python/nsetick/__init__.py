"""nsetick: fast, correct parsing of NSE historical order and trade data.

Two ways to use this, depending on what you need.

**Write Parquet once, query it forever.** Best for a one-time conversion of a session that
several analyses will then read:

    import nsetick

    nsetick.parse(
        "data/raw/CASH_Orders_27012022.DAT.gz",
        out="data/parquet",
        where="series == 'EQ' and symbol in ('RELIANCE', 'TCS')",
        select=["symbol", "txn_time", "limit_price", "volume_original"],
    )

**Stream Arrow batches straight into the process.** Best when an analysis wants filtered rows
and nothing needs to persist. Nothing is written to disk and memory stays bounded no matter
how large the session is:

    for batch in nsetick.iter_batches(path, where="symbol == 'RELIANCE'"):
        df = batch.to_pandas()
        ...

Byte offsets come from ``spec/layouts/*.toml``, which the Rust core embeds at compile time,
so the Python and Rust views of a layout cannot disagree.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable, Iterator, Sequence

from ._native import (
    BatchReader,
    check_filter,
    describe,
    layouts,
    memory_estimate,
    probe,
    run_spec,
)
from ._native import build_books as _native_build_books
from ._native import replay_fills as _native_replay_fills
from ._native import parse as _native_parse
from ._native import version as _native_version

if TYPE_CHECKING:  # pragma: no cover
    import pyarrow as pa

__version__ = _native_version()

__all__ = [
    "BatchReader",
    "check_filter",
    "describe",
    "build_books",
    "iter_batches",
    "layouts",
    "memory_estimate",
    "parse",
    "probe",
    "read_table",
    "replay_fills",
    "run_spec",
    "to_pandas",
    "to_polars",
    "__version__",
]


def parse(
    input: str,
    out: str,
    *,
    layout: str | None = None,
    date: str | None = None,
    select: Sequence[str] | None = None,
    where: str | None = None,
    partition_by: str | None = "symbol",
    compression: str = "snappy",
    threads: int | None = None,
    strict: bool = True,
    verify_trigger: bool = True,
    memory_limit_mb: int | None = None,
    max_records: int | None = None,
    row_group_rows: int | None = None,
    note: str | None = None,
) -> dict:
    """Parse one NSE file into partitioned Parquet, returning a summary dict.

    ``layout`` and ``date`` are inferred from the file name when omitted. ``where`` is
    validated against the layout before any data is read.

    The keyword is spelled ``where`` here, matching :func:`iter_batches`; the native function
    underneath must call it ``where_`` because ``where`` is a Rust keyword.
    """
    return _native_parse(
        input,
        out,
        layout=layout,
        date=date,
        select=list(select) if select is not None else None,
        where_=where,
        partition_by=partition_by,
        compression=compression,
        threads=threads,
        strict=strict,
        verify_trigger=verify_trigger,
        memory_limit_mb=memory_limit_mb,
        max_records=max_records,
        row_group_rows=row_group_rows,
        note=note,
    )


def build_books(
    input: str,
    out: str,
    *,
    date: str | None = None,
    where: str | None = None,
    interval_secs: float = 1.0,
    levels: int = 20,
    threads: int | None = None,
    compression: str = "snappy",
    max_records: int | None = None,
    symbols: Sequence[str] | None = None,
) -> dict:
    """Reconstruct limit order books and write periodic L2 snapshots as Parquet.

    ``input`` may be a raw ``.DAT.gz`` or a directory of already-parsed orders (the
    ``date=...`` directory holding ``symbol=*`` partitions). Prefer the parsed directory when
    it exists: it avoids a second pass over the compressed file and parallelises per symbol,
    which measured 28x faster on a full session for identical output.

    Replays NSE order events into a per-symbol book - honouring disclosed-quantity
    replenishment and the queue-priority loss that comes with it - and captures the book
    every ``interval_secs``. Output is partitioned by symbol, with ``levels`` price levels per
    side plus the hidden quantity resting at each.

    Every symbol's book is independent, so the replay fans out across ``threads`` and covers
    the whole session in a single pass over the file.
    """
    return _native_build_books(
        input,
        out,
        date=date,
        where_=where,
        interval_secs=interval_secs,
        levels=levels,
        threads=threads,
        compression=compression,
        max_records=max_records,
        symbols=list(symbols) if symbols is not None else None,
    )


def replay_fills(input: str, *, symbols: Sequence[str] | None = None) -> "pa.Table":
    """Every trade the order book replay generates, as a ``pyarrow.Table``.

    ``input`` is a directory of parsed Capital Market orders (the ``date=...`` directory with
    ``symbol=*`` partitions) or a single parquet file. Columns follow the exchange's trade file
    - ``buy_order_number``, ``sell_order_number``, ``trade_price``, ``trade_quantity`` - so the
    result can be joined to the parsed trades on the two order numbers to check which trades
    the replay reproduces. ``txn_time`` is the time of the incoming order's event; the exchange
    stamps its own trades about 15 microseconds per trade later, so join on order numbers.
    """
    import pyarrow as pa

    batch = _native_replay_fills(input, list(symbols) if symbols is not None else None)
    return pa.Table.from_batches([batch])


def iter_batches(
    input: str,
    *,
    layout: str | None = None,
    date: str | None = None,
    select: Sequence[str] | None = None,
    where: str | None = None,
    strict: bool = True,
    chunk_mb: int = 8,
) -> BatchReader:
    """Stream ``pyarrow.RecordBatch`` objects from an NSE file.

    Decodes one chunk at a time, so a 56 GB session streams through in ``chunk_mb`` pieces
    rather than being materialised. The GIL is released during decoding.

    ``layout`` and ``date`` are inferred from the file name when omitted. ``where`` is
    validated against the layout before any data is read, so a typo in a field name raises
    immediately instead of yielding nothing.

    Batches whose rows are entirely filtered out are skipped, so an empty result is an empty
    iterator rather than a stream of empty batches.
    """
    return BatchReader(
        input,
        layout=layout,
        date=date,
        select=list(select) if select is not None else None,
        where_=where,
        strict=strict,
        chunk_mb=chunk_mb,
    )


def read_table(
    input: str,
    *,
    layout: str | None = None,
    date: str | None = None,
    select: Sequence[str] | None = None,
    where: str | None = None,
    strict: bool = True,
    chunk_mb: int = 8,
    max_rows: int | None = None,
) -> "pa.Table":
    """Read matching records into a single ``pyarrow.Table``.

    Convenient, but it holds the whole result in memory. A full unfiltered CM session is
    roughly 684 million rows, so pass ``where=`` to narrow it, or use :func:`iter_batches`
    when the result will not fit. ``max_rows`` caps the result as a safety net.
    """
    import pyarrow as pa

    reader = iter_batches(
        input,
        layout=layout,
        date=date,
        select=select,
        where=where,
        strict=strict,
        chunk_mb=chunk_mb,
    )
    batches: list[Any] = []
    rows = 0
    for batch in reader:
        batches.append(batch)
        rows += batch.num_rows
        if max_rows is not None and rows >= max_rows:
            break

    if not batches:
        return pa.Table.from_batches([], schema=reader.schema)
    table = pa.Table.from_batches(batches)
    if max_rows is not None and table.num_rows > max_rows:
        table = table.slice(0, max_rows)
    return table


def to_pandas(input: str, **kwargs: Any):
    """Read matching records into a pandas DataFrame. See :func:`read_table`."""
    return read_table(input, **kwargs).to_pandas()


def to_polars(input: str, **kwargs: Any):
    """Read matching records into a Polars DataFrame. See :func:`read_table`."""
    import polars as pl

    return pl.from_arrow(read_table(input, **kwargs))


def _iter_all(readers: Iterable[BatchReader]) -> Iterator[Any]:
    for r in readers:
        yield from r
