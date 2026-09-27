"""Every side effect of the silver_cvm_fund_daily DAG: read bronze, write silver and discards.

Table locations follow the platform convention — an Asset's name is its table's path
under the tables root — so bronze is found through the Asset that announced it.
"""

from __future__ import annotations

import datetime as dt

import polars as pl

from cvm.assets import BRONZE_FUND_DAILY, SILVER_FUND_DAILY
from cvm.bronze_fund_daily.core import schema as bronze
from cvm.silver_fund_daily.core import schema
from include import delta
from include.runtime import Lakehouse


def _table_uri(lake: Lakehouse, asset_name: str) -> str:
    """Return the table an Asset names (``<layer>/<domain>/<table>``).

    Args:
        lake (Lakehouse): Storage locations for the current mode.
        asset_name (str): The Asset's name.

    Returns:
        str: The table location.
    """
    layer, domain, table = asset_name.split("/")
    return lake.table_uri(layer, domain, table)


def bronze_uri(lake: Lakehouse) -> str:
    """Return the bronze table silver reads.

    Args:
        lake (Lakehouse): Storage locations for the current mode.

    Returns:
        str: ``<tables>/bronze/cvm/fund_daily``.
    """
    return _table_uri(lake, BRONZE_FUND_DAILY.name)


def silver_uri(lake: Lakehouse) -> str:
    """Return the silver table this DAG writes.

    Args:
        lake (Lakehouse): Storage locations for the current mode.

    Returns:
        str: ``<tables>/silver/cvm/fund_daily``.
    """
    return _table_uri(lake, SILVER_FUND_DAILY.name)


def discarded_uri(lake: Lakehouse) -> str:
    """Return the discard log: every bronze row not kept, with the reason.

    Args:
        lake (Lakehouse): Storage locations for the current mode.

    Returns:
        str: ``<tables>/silver/cvm/fund_daily_discarded``.
    """
    return f"{silver_uri(lake)}_discarded"


def bronze_months(lake: Lakehouse) -> list[dt.date]:
    """List the months bronze holds — the scope of a full rebuild.

    Args:
        lake (Lakehouse): Storage locations for the current mode.

    Returns:
        list[dt.date]: First day of each month, oldest first.
    """
    return delta.partition_values(lake, bronze_uri(lake), bronze.BRONZE_PARTITION)


def read_bronze_month(lake: Lakehouse, month: dt.date) -> pl.DataFrame:
    """Read one month of bronze.

    Args:
        lake (Lakehouse): Storage locations for the current mode.
        month (dt.date): First day of the month.

    Returns:
        pl.DataFrame: That month's bronze rows.
    """
    return delta.read_partition(lake, bronze_uri(lake), bronze.BRONZE_PARTITION, month)


def write_month(
    lake: Lakehouse, month: dt.date, kept: pl.DataFrame, discarded: pl.DataFrame
) -> None:
    """Replace one month in silver and in the discard log.

    Both partitions are always rewritten — an empty discard frame clears discards left by
    an earlier version of the month. Discards go first: a crash in between leaves silver
    at its previous version and the re-run rewrites both.

    Args:
        lake (Lakehouse): Storage locations for the current mode.
        month (dt.date): First day of the month.
        kept (pl.DataFrame): Silver rows of the month.
        discarded (pl.DataFrame): Discarded rows of the month.
    """
    delta.overwrite_partition(lake, discarded_uri(lake), discarded, schema.SILVER_PARTITION, month)
    delta.overwrite_partition(lake, silver_uri(lake), kept, schema.SILVER_PARTITION, month)
