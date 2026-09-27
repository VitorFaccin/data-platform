"""Rules that turn one month of bronze into silver: conform, resolve duplicates, reconcile.

Pure: DataFrames in, DataFrames out. Duplicates on the grain are resolved in three
levels, measured on the real files before being written down:

1. Same figures (type label may differ) → one row survives, the post-CVM 175 type first.
2. Different figures, exactly one class report (post-175) and the rest legacy fund
   reports (``FI``) → the class report survives. A fund mid-adaptation reports in both
   formats; after CVM 175 the class is the reporting entity. This covered all 21
   conflicts seen in the first 15 months.
3. Anything else → ``UnresolvableConflictError``. Never seen; if it appears it is a new
   kind of defect, and a person decides the rule — the parser's philosophy, applied to
   content instead of layout.
"""

from __future__ import annotations

import datetime as dt
import re
from typing import Any

import polars as pl

from cvm.silver_fund_daily.core import schema

_MONTH = re.compile(r"^\d{4}-\d{2}$")
_CNPJ_LENGTH = 14


class UnresolvableConflictError(ValueError):
    """Rows share the grain with different figures and no rule says which one is right."""


class ReconciliationError(ValueError):
    """The month's rows do not add up: silver and discards must account for all of bronze."""


def parse_months(values: list[str]) -> list[dt.date]:
    """Parse ``YYYY-MM`` strings from a DAG param into distinct first days, oldest first.

    Args:
        values (list[str]): Months as typed in the trigger form.

    Returns:
        list[dt.date]: First day of each month.

    Raises:
        ValueError: On any string that is not ``YYYY-MM``.
    """
    bad = [value for value in values if not _MONTH.match(value)]
    if bad:
        raise ValueError(f"months must be YYYY-MM, got {bad}")
    return sorted({dt.date.fromisoformat(f"{value}-01") for value in values})


def conform(bronze: pl.DataFrame) -> pl.DataFrame:
    """Rename to the platform's vocabulary, conform the CNPJ and derive ``net_flow``.

    The CNPJ keeps only its digits (``00.017.024/0001-53`` → ``00017024000153``), the
    spelling the fund registry uses.

    Args:
        bronze (pl.DataFrame): One month of bronze rows.

    Returns:
        pl.DataFrame: Rows matching ``schema.SILVER_SCHEMA``, duplicates still present.

    Raises:
        ValueError: When a CNPJ does not have exactly 14 digits.
    """
    renamed = bronze.rename(schema.BRONZE_TO_SILVER).with_columns(
        pl.col("fund_class_cnpj").str.replace_all(r"\D", ""),
    )
    malformed = renamed.filter(pl.col("fund_class_cnpj").str.len_chars() != _CNPJ_LENGTH)
    if malformed.height:
        sample = malformed.get_column("fund_class_cnpj").head(3).to_list()
        raise ValueError(f"{malformed.height} CNPJs without 14 digits, e.g. {sample}")
    return renamed.with_columns(
        (pl.col("subscriptions") - pl.col("redemptions")).alias("net_flow")
    ).select(pl.col(name).cast(dtype) for name, dtype in schema.SILVER_SCHEMA.items())


def _type_rank(row: dict[str, Any]) -> tuple[int, str]:
    """Order rows so the post-CVM 175 class report comes first, legacy reports next.

    Args:
        row (dict[str, Any]): One silver row.

    Returns:
        tuple[int, str]: Sort key — rank of the type family, then the type itself.
    """
    fund_type = row["fund_type"]
    if fund_type in schema.CLASS_REPORT_TYPES:
        return 0, fund_type
    if fund_type in schema.LEGACY_REPORT_TYPES:
        return 1, fund_type
    return 2, fund_type


def _figures(row: dict[str, Any]) -> tuple[Any, ...]:
    """Return the measured values of a row, the part that must agree between duplicates.

    Args:
        row (dict[str, Any]): One silver row.

    Returns:
        tuple[Any, ...]: Values of ``schema.FIGURES``, in order.
    """
    return tuple(row[column] for column in schema.FIGURES)


def _resolve_group(
    rows: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[tuple[dict[str, Any], schema.DiscardReason]]]:
    """Pick the surviving row among rows sharing the grain (levels 1-3 of the module doc).

    Args:
        rows (list[dict[str, Any]]): Two or more rows with the same key.

    Returns:
        tuple: The surviving row, and each discarded row with its reason.

    Raises:
        UnresolvableConflictError: When no rule decides between different figures.
    """
    ranked = sorted(rows, key=_type_rank)
    if len({_figures(row) for row in ranked}) == 1:
        return ranked[0], [(row, schema.DiscardReason.EXACT_DUPLICATE) for row in ranked[1:]]

    variants: dict[tuple[Any, ...], dict[str, Any]] = {}
    discarded: list[tuple[dict[str, Any], schema.DiscardReason]] = []
    for row in ranked:
        identity = (row["fund_type"], *_figures(row))
        if identity in variants:
            discarded.append((row, schema.DiscardReason.EXACT_DUPLICATE))
        else:
            variants[identity] = row
    distinct = list(variants.values())
    class_reports = [r for r in distinct if r["fund_type"] in schema.CLASS_REPORT_TYPES]
    legacy_reports = [r for r in distinct if r["fund_type"] in schema.LEGACY_REPORT_TYPES]
    if len(class_reports) == 1 and len(class_reports) + len(legacy_reports) == len(distinct):
        superseded = schema.DiscardReason.SUPERSEDED_BY_CLASS_REPORT
        return class_reports[0], discarded + [(row, superseded) for row in legacy_reports]

    key = {column: rows[0][column] for column in schema.SILVER_KEY}
    raise UnresolvableConflictError(
        f"{key}: {len(distinct)} reports with different figures and no rule to choose — "
        f"types {[row['fund_type'] for row in distinct]}"
    )


def resolve_duplicates(frame: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Enforce the grain: keep one row per key, log every other row with its reason.

    Rows with a unique key pass through untouched; only the (few) duplicated keys are
    resolved row by row.

    Args:
        frame (pl.DataFrame): Conformed rows of one month.

    Returns:
        tuple[pl.DataFrame, pl.DataFrame]: Kept rows (``SILVER_SCHEMA``) and discarded
            rows (``DISCARDED_SCHEMA``).

    Raises:
        UnresolvableConflictError: From ``_resolve_group``.
    """
    duplicated = pl.struct(list(schema.SILVER_KEY)).is_duplicated()
    unique = frame.filter(~duplicated)
    kept: list[dict[str, Any]] = []
    discarded: list[dict[str, Any]] = []
    for _, group in frame.filter(duplicated).group_by(list(schema.SILVER_KEY), maintain_order=True):
        survivor, dropped = _resolve_group(group.to_dicts())
        kept.append(survivor)
        discarded.extend({**row, "discard_reason": str(reason)} for row, reason in dropped)
    return (
        pl.concat([unique, pl.DataFrame(kept, schema=schema.SILVER_SCHEMA, orient="row")]),
        pl.DataFrame(discarded, schema=schema.DISCARDED_SCHEMA, orient="row"),
    )


def reconcile(
    month: dt.date, bronze_rows: int, kept: pl.DataFrame, discarded: pl.DataFrame
) -> None:
    """Gate the month before anything is written: counts add up, grain holds, keys exist.

    Args:
        month (dt.date): The month being published.
        bronze_rows (int): Rows read from bronze for the month.
        kept (pl.DataFrame): Rows about to be written to silver.
        discarded (pl.DataFrame): Rows about to be written to the discard log.

    Raises:
        ReconciliationError: On any broken invariant, naming it.
    """
    if kept.height + discarded.height != bronze_rows:
        raise ReconciliationError(
            f"{month:%Y-%m}: silver {kept.height} + discarded {discarded.height} "
            f"!= bronze {bronze_rows}"
        )
    if kept.select(pl.struct(list(schema.SILVER_KEY)).is_duplicated().any()).item():
        raise ReconciliationError(f"{month:%Y-%m}: duplicate grain left in silver")
    nulls = sum(
        kept.get_column(column).null_count() for column in ("fund_class_cnpj", "report_date")
    )
    if nulls:
        raise ReconciliationError(f"{month:%Y-%m}: {nulls} null key values in silver")
    other_months = kept.filter(pl.col(schema.SILVER_PARTITION) != month).height
    if other_months:
        raise ReconciliationError(f"{month:%Y-%m}: {other_months} rows of another month")
