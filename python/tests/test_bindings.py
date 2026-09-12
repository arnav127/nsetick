"""Tests for the Python bindings.

Tests needing a real NSE file are skipped unless NSETICK_TEST_FILE points at one, so the
suite still runs on a machine without the data.

    set NSETICK_TEST_FILE=/path/to/CASH_Orders_27012022.DAT.gz
    pytest python/tests
"""

from __future__ import annotations

import os

import pytest

import nsetick

TEST_FILE = os.environ.get("NSETICK_TEST_FILE")
needs_data = pytest.mark.skipif(not TEST_FILE, reason="set NSETICK_TEST_FILE to a .DAT.gz")

# Records to slice out of the real session for the streaming tests. Large enough to span many
# symbols and several chunks, small enough that the whole suite stays quick: reading all
# 684M records of a real session takes minutes per test.
FIXTURE_RECORDS = 300_000


@pytest.fixture(scope="session")
def sample(tmp_path_factory):
    """A small .DAT.gz cut from the head of the real session file."""
    import gzip
    import zlib

    reclen = nsetick.probe(TEST_FILE)["observed_record_length"] + 1
    want = FIXTURE_RECORDS * reclen
    # Keep the original file name so layout and session date stay inferable.
    out = tmp_path_factory.mktemp("nsetick") / os.path.basename(TEST_FILE)

    d = zlib.decompressobj(31)
    written = 0
    with open(TEST_FILE, "rb") as src, gzip.open(out, "wb", compresslevel=1) as dst:
        while written < want:
            chunk = src.read(4 << 20)
            if not chunk:
                break
            block = d.decompress(chunk)
            if written + len(block) > want:
                block = block[: want - written]
            dst.write(block)
            written += len(block)
    return str(out)


@pytest.fixture(scope="session")
def ampersand_symbol(sample):
    """A symbol containing '&' that actually occurs in the sample.

    Which symbols appear in the head of a session is not arbitrary: NSE writes the file as
    symbol-range blocks, so the first records cover only early-alphabet names and M&M is not
    among them. Discover one rather than assuming.
    """
    seen = set()
    for batch in nsetick.iter_batches(sample, select=["symbol"]):
        seen.update(batch.column("symbol").to_pylist())
    hits = sorted(s for s in seen if "&" in s)
    if not hits:
        pytest.skip("no symbol containing '&' in the sample")
    return hits[0]


def test_version_is_exposed():
    assert nsetick.__version__.count(".") == 2


def test_every_layout_is_listed():
    got = set(nsetick.layouts())
    assert got == {
        "cm_orders",
        "cm_trades",
        "cm_index",
        "fao_orders",
        "fao_trades",
        "cd_orders",
        "cd_trades",
    }


def test_describe_reports_offsets_from_the_shared_spec():
    d = nsetick.describe("cm_orders", date="2022-01-27")
    assert d["record_length"] == 87
    assert d["verified"] is True
    fields = {f["name"]: f for f in d["fields"]}
    # The symbol field is the one everything else hinges on.
    assert fields["symbol"]["offset"] == 38
    assert fields["symbol"]["len"] == 10
    assert fields["symbol"]["pad"] == "Left"
    # Fields tile the record exactly.
    assert sum(f["len"] for f in d["fields"]) == d["record_length"]


def test_fao_trades_layout_switches_on_the_2020_boundary():
    before = nsetick.describe("fao_trades", date="2019-05-01")
    after = nsetick.describe("fao_trades", date="2022-05-01")
    assert before["record_length"] == 123
    assert after["record_length"] == 124
    # The one-byte trade number change shifts everything after it.
    off = lambda d, n: next(f["offset"] for f in d["fields"] if f["name"] == n)  # noqa: E731
    assert off(before, "symbol") == 36
    assert off(after, "symbol") == 37


def test_cd_prices_are_scale_four_and_cm_scale_two():
    cd = nsetick.describe("cd_orders", date="2022-05-01")
    cm = nsetick.describe("cm_orders", date="2022-05-01")
    scale = lambda d, n: next(f["scale"] for f in d["fields"] if f["name"] == n)  # noqa: E731
    assert scale(cd, "limit_price") == 4
    assert scale(cm, "limit_price") == 2


def test_a_good_filter_compiles():
    assert nsetick.check_filter("series == 'EQ' and symbol in ('M&M','TCS')", "cm_orders")
    assert nsetick.check_filter("not (mkt_order_flag == true)", "cm_orders")


def test_a_typo_in_a_field_name_raises_immediately():
    with pytest.raises(ValueError) as e:
        nsetick.check_filter("symobl == 'TCS'", "cm_orders")
    assert "unknown field" in str(e.value)


def test_memory_estimate_is_self_consistent():
    m = nsetick.memory_estimate(1000)
    assert m["available_bytes"] <= m["total_bytes"]
    assert m["estimated_bytes"] >= 1000 * m["bytes_per_open_partition"]
    assert m["partitions_that_fit"] > 0


def test_parse_and_iter_batches_agree_on_the_filter_keyword():
    # parse() previously exposed the native `where_` spelling while iter_batches took
    # `where`, so documented calls to parse(where=...) raised TypeError.
    import inspect

    assert "where" in inspect.signature(nsetick.parse).parameters
    assert "where" in inspect.signature(nsetick.iter_batches).parameters


def test_bundled_specs_are_findable_from_an_installed_package():
    # The TOMLs ship in the wheel; if the lookup path is wrong they resolve to nothing and
    # the pure-Python reference decoder breaks, while the native API keeps working and hides
    # the problem.
    from nsetick import layout as pylayout

    assert pylayout.SPEC_DIR.is_dir(), f"spec dir not found at {pylayout.SPEC_DIR}"
    assert set(pylayout.available()) == set(nsetick.layouts())


def test_unknown_layout_is_rejected():
    with pytest.raises(ValueError):
        nsetick.describe("not_a_layout")


@needs_data
def test_probe_identifies_the_file():
    p = nsetick.probe(TEST_FILE)
    assert p["observed_record_length"] == 87
    assert p["layout"] == "cm_orders"


@needs_data
def test_streaming_yields_arrow_batches_with_the_projected_schema(sample):
    reader = nsetick.iter_batches(
        sample,
        where="symbol == 'BAJAJ-AUTO'",
        select=["symbol", "txn_time", "limit_price"],
    )
    # Schema follows layout order, not the order fields were requested in.
    assert [f.name for f in reader.schema] == ["txn_time", "symbol", "limit_price"]

    batch = next(iter(reader))
    assert batch.num_rows > 0
    assert set(batch.column("symbol").to_pylist()) == {"BAJAJ-AUTO"}

    s = reader.stats
    assert s["rows_read"] > s["rows_emitted"] > 0
    assert s["rows_malformed"] == 0


@needs_data
def test_a_filter_matching_nothing_yields_an_empty_iterator_not_empty_batches(sample):
    # 8 characters: a needle wider than the 10-byte symbol field is rejected at compile time.
    reader = nsetick.iter_batches(sample, where="symbol == 'ZZZZZZZZ'")
    # Must terminate rather than emitting a stream of empty batches.
    assert list(reader) == []
    assert reader.stats["rows_read"] > 0
    assert reader.stats["rows_emitted"] == 0


@needs_data
def test_symbols_containing_ampersands_survive(sample, ampersand_symbol):
    # A regex clean of [A-Z0-9-]+ would truncate this at the ampersand.
    t = nsetick.read_table(
        sample,
        where=f"symbol == '{ampersand_symbol}'",
        select=["symbol"],
        max_rows=50,
    )
    assert t.num_rows > 0
    assert set(t.column("symbol").to_pylist()) == {ampersand_symbol}


@needs_data
def test_price_scale_travels_in_field_metadata(sample):
    t = nsetick.read_table(sample, select=["limit_price"], max_rows=1)
    meta = t.schema.field("limit_price").metadata
    assert meta[b"scale"] == b"2"


@needs_data
def test_timestamps_are_naive_ist_wall_clock(sample):
    t = nsetick.read_table(sample, select=["txn_time"], max_rows=1)
    # Naive: tagging these UTC would shift every value by 5h30m.
    assert t.schema.field("txn_time").type.tz is None
    first = t.column("txn_time").to_pylist()[0]
    assert first.hour == 9, f"session should start at 09:xx IST, got {first}"


@needs_data
def test_max_rows_caps_the_result(sample):
    t = nsetick.read_table(sample, where="series == 'EQ'", max_rows=1234)
    assert t.num_rows == 1234


@needs_data
def test_selecting_an_unknown_field_raises(sample):
    with pytest.raises(ValueError) as e:
        nsetick.iter_batches(sample, select=["not_a_field"])
    assert "unknown field" in str(e.value)
