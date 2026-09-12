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
    parse,
    probe,
    run_spec,
)
from ._native import version as _native_version

if TYPE_CHECKING:  # pragma: no cover
    import pyarrow as pa

__version__ = _native_version()

__all__ = [
    "BatchReader",
    "check_filter",
    "describe",
    "iter_batches",
    "layouts",
    "memory_estimate",
    "parse",
    "probe",
    "read_table",
    "run_spec",
    "to_pandas",
    "to_polars",
    "__version__",
]


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
