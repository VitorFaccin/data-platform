"""Delta Lake operations every layer shares: partition replace, upsert, partition reads.

Business-blind: callers pass the table location (from ``include.runtime.Lakehouse``),
the partition column and the keys; nothing here knows a table name. The two write
shapes are the only ones the platform allows (docs/ARCHITECTURE.md, "Idempotency by
construction"): a partition overwrite scoped by a predicate, or a MERGE on a key.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence

import polars as pl
from deltalake import DeltaTable

from include.runtime import Lakehouse


def table_exists(lake: Lakehouse, uri: str) -> bool:
    """Tell whether a Delta table already exists at ``uri``.

    Args:
        lake (Lakehouse): Storage locations, for the credentials delta-rs needs.
        uri (str): Candidate table location.

    Returns:
        bool: True when a Delta transaction log exists there.
    """
    return DeltaTable.is_deltatable(uri, storage_options=lake.storage_options)


def overwrite_partition(
    lake: Lakehouse, uri: str, frame: pl.DataFrame, partition_column: str, value: dt.date
) -> None:
    """Replace exactly one partition of a table, creating the table on first write.

    The predicate is the whole point: without it ``mode="overwrite"`` replaces the
    ENTIRE table, and re-running one partition silently deletes all the others. Delta
    also rejects the write if any row falls outside the predicate. An empty ``frame``
    empties the partition — how a month that no longer has rows gets cleared.

    Args:
        lake (Lakehouse): Storage locations for the current mode.
        uri (str): Table location.
        frame (pl.DataFrame): Every row the partition must hold after the write.
        partition_column (str): Date column the table is partitioned by.
        value (dt.date): The partition being replaced.
    """
    options: dict[str, object] = {"partition_by": [partition_column]}
    if table_exists(lake, uri):
        options["predicate"] = f"{partition_column} = '{value.isoformat()}'"
    frame.write_delta(
        uri, mode="overwrite", storage_options=lake.storage_options, delta_write_options=options
    )


def _merge_options(keys: Sequence[str]) -> dict[str, str]:
    """Build the MERGE options matching source to target on ``keys``.

    Args:
        keys (Sequence[str]): Columns that identify a row.

    Returns:
        dict[str, str]: Predicate and aliases for ``write_delta(mode="merge")``.
    """
    return {
        "predicate": " AND ".join(f"t.{key} = s.{key}" for key in keys),
        "source_alias": "s",
        "target_alias": "t",
    }


def upsert(
    lake: Lakehouse, uri: str, frame: pl.DataFrame, keys: Sequence[str], updates: Sequence[str]
) -> None:
    """MERGE rows on ``keys``: update ``updates`` when the key exists, insert otherwise.

    Running it twice with the same rows leaves the table as one run does.

    Args:
        lake (Lakehouse): Storage locations for the current mode.
        uri (str): Table location; created from ``frame`` if absent.
        frame (pl.DataFrame): Rows to merge, with every column of the table.
        keys (Sequence[str]): Columns that identify a row.
        updates (Sequence[str]): Columns refreshed on an existing key.
    """
    if not table_exists(lake, uri):
        frame.write_delta(uri, mode="error", storage_options=lake.storage_options)
        return
    (
        frame.write_delta(
            uri,
            mode="merge",
            storage_options=lake.storage_options,
            delta_merge_options=_merge_options(keys),
        )
        .when_matched_update(updates={column: f"s.{column}" for column in updates})
        .when_not_matched_insert_all()
        .execute()
    )


def update_existing(
    lake: Lakehouse, uri: str, frame: pl.DataFrame, keys: Sequence[str], updates: Sequence[str]
) -> None:
    """MERGE that only refreshes rows whose key already exists; never inserts.

    Args:
        lake (Lakehouse): Storage locations for the current mode.
        uri (str): Table location; must exist.
        frame (pl.DataFrame): The key columns plus the columns to refresh.
        keys (Sequence[str]): Columns that identify a row.
        updates (Sequence[str]): Columns refreshed on an existing key.
    """
    (
        frame.write_delta(
            uri,
            mode="merge",
            storage_options=lake.storage_options,
            delta_merge_options=_merge_options(keys),
        )
        .when_matched_update(updates={column: f"s.{column}" for column in updates})
        .execute()
    )


def read_partition(
    lake: Lakehouse, uri: str, partition_column: str, value: dt.date
) -> pl.DataFrame:
    """Read one partition of a table.

    Args:
        lake (Lakehouse): Storage locations for the current mode.
        uri (str): Table location.
        partition_column (str): Date column the table is partitioned by.
        value (dt.date): The partition to read.

    Returns:
        pl.DataFrame: Its rows (empty when the partition holds none).
    """
    return (
        pl.scan_delta(uri, storage_options=lake.storage_options)
        .filter(pl.col(partition_column) == value)
        .collect()
    )


def partition_values(lake: Lakehouse, uri: str, partition_column: str) -> list[dt.date]:
    """List the partitions a table currently holds.

    Args:
        lake (Lakehouse): Storage locations for the current mode.
        uri (str): Table location.
        partition_column (str): Date column the table is partitioned by.

    Returns:
        list[dt.date]: Distinct partition values, oldest first; empty if no table.
    """
    if not table_exists(lake, uri):
        return []
    values = (
        pl.scan_delta(uri, storage_options=lake.storage_options)
        .select(pl.col(partition_column).unique().sort())
        .collect()
    )
    return values.get_column(partition_column).to_list()
