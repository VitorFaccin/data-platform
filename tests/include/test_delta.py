"""include.delta: the two write shapes the platform allows, and partition reads.

Every test runs against a real Delta table in a temporary directory.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl
import pytest

from include import delta, runtime

JAN = dt.date(2026, 1, 1)
FEB = dt.date(2026, 2, 1)
SCHEMA = {"key": pl.String(), "value": pl.Int64(), "month": pl.Date()}


@pytest.fixture
def lake(tmp_path: Path) -> runtime.Lakehouse:
    """Build a local-mode lakehouse inside a temporary directory.

    Args:
        tmp_path (Path): Per-test directory from pytest.

    Returns:
        runtime.Lakehouse: Tables and landing under ``tmp_path``.
    """
    return runtime.Lakehouse(runtime.Mode.LOCAL, str(tmp_path), str(tmp_path / "landing"))


def _rows(month: dt.date, *pairs: tuple[str, int]) -> pl.DataFrame:
    """Build a small frame for one month.

    Args:
        month (dt.date): Partition value of every row.
        *pairs (tuple[str, int]): ``(key, value)`` per row.

    Returns:
        pl.DataFrame: Rows matching ``SCHEMA``.
    """
    return pl.DataFrame(
        [{"key": key, "value": value, "month": month} for key, value in pairs], schema=SCHEMA
    )


def _read(lake: runtime.Lakehouse, uri: str) -> list[tuple[str, int, dt.date]]:
    """Read a whole table as sorted tuples.

    Args:
        lake (runtime.Lakehouse): Where the table lives.
        uri (str): Table location.

    Returns:
        list[tuple[str, int, dt.date]]: Every row, sorted.
    """
    return sorted(pl.read_delta(uri, storage_options=lake.storage_options).rows())


def test_table_exists_only_after_the_first_write(lake: runtime.Lakehouse) -> None:
    uri = lake.table_uri("bronze", "d", "t")
    assert not delta.table_exists(lake, uri)
    delta.overwrite_partition(lake, uri, _rows(JAN, ("a", 1)), "month", JAN)
    assert delta.table_exists(lake, uri)


def test_overwrite_replaces_only_its_partition(lake: runtime.Lakehouse) -> None:
    uri = lake.table_uri("bronze", "d", "t")
    delta.overwrite_partition(lake, uri, _rows(JAN, ("a", 1), ("b", 2)), "month", JAN)
    delta.overwrite_partition(lake, uri, _rows(FEB, ("a", 10)), "month", FEB)

    delta.overwrite_partition(lake, uri, _rows(JAN, ("a", 5)), "month", JAN)

    assert _read(lake, uri) == [("a", 5, JAN), ("a", 10, FEB)]


def test_overwriting_with_no_rows_empties_the_partition(lake: runtime.Lakehouse) -> None:
    uri = lake.table_uri("silver", "d", "discards")
    delta.overwrite_partition(lake, uri, _rows(JAN, ("a", 1)), "month", JAN)
    delta.overwrite_partition(lake, uri, _rows(FEB, ("b", 2)), "month", FEB)

    delta.overwrite_partition(lake, uri, pl.DataFrame(schema=SCHEMA), "month", JAN)

    assert _read(lake, uri) == [("b", 2, FEB)]


def test_upsert_updates_existing_keys_and_inserts_new_ones(lake: runtime.Lakehouse) -> None:
    uri = lake.table_uri("control", "d", "t")
    delta.upsert(lake, uri, _rows(JAN, ("a", 1)), keys=("key",), updates=("value",))
    delta.upsert(lake, uri, _rows(JAN, ("a", 2), ("b", 3)), keys=("key",), updates=("value",))
    delta.upsert(lake, uri, _rows(JAN, ("a", 2), ("b", 3)), keys=("key",), updates=("value",))
    assert _read(lake, uri) == [("a", 2, JAN), ("b", 3, JAN)]


def test_update_existing_never_inserts(lake: runtime.Lakehouse) -> None:
    uri = lake.table_uri("control", "d", "t")
    delta.upsert(lake, uri, _rows(JAN, ("a", 1)), keys=("key",), updates=("value",))
    delta.update_existing(lake, uri, _rows(JAN, ("a", 9), ("new", 7)), ("key",), ("value",))
    assert _read(lake, uri) == [("a", 9, JAN)]


def test_partition_reads_and_listing(lake: runtime.Lakehouse) -> None:
    uri = lake.table_uri("bronze", "d", "t")
    assert delta.partition_values(lake, uri, "month") == []
    delta.overwrite_partition(lake, uri, _rows(FEB, ("x", 1)), "month", FEB)
    delta.overwrite_partition(lake, uri, _rows(JAN, ("y", 2), ("z", 3)), "month", JAN)
    assert delta.partition_values(lake, uri, "month") == [JAN, FEB]
    assert delta.read_partition(lake, uri, "month", JAN).height == 2
