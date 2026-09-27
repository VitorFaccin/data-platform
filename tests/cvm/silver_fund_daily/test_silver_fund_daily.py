"""silver_cvm_fund_daily: conforming, the three duplicate levels, the gate, the rewrite.

Duplicate cases mirror what the real files contain (docs/CVM_DATA.md): repeated lines,
a fund reported as ``FI`` and as ``CLASSES - FIF`` with the same or slightly different
figures. Bronze input is produced by bronze's own parser from its fixtures.
"""

from __future__ import annotations

import datetime as dt
import io
import zipfile
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from cvm.bronze_fund_daily import adapters as bronze_adapters
from cvm.bronze_fund_daily.core import domain as bronze_domain
from cvm.silver_fund_daily import adapters
from cvm.silver_fund_daily.core import domain, schema
from include import runtime

BRONZE_FIXTURES = Path(__file__).parents[1] / "bronze_fund_daily" / "fixtures"
SEP_2026 = dt.date(2026, 9, 1)
DAY = dt.date(2026, 9, 1)
SHA = "f" * 64


def _bronze_month(
    month: dt.date = SEP_2026, fixture: str = "inf_diario_v2_sample.csv"
) -> pl.DataFrame:
    """Parse a bronze fixture exactly as the bronze DAG would.

    Args:
        month (dt.date): Month the fixture represents.
        fixture (str): File under the bronze fixtures folder.

    Returns:
        pl.DataFrame: Bronze rows.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            f"inf_diario_fi_{month:%Y%m}.csv", (BRONZE_FIXTURES / fixture).read_bytes()
        )
    return bronze_domain.parse_fund_daily(buffer.getvalue(), month, SHA)


def _row(fund_type: str = "CLASSES - FIF", shareholders: int = 10, **overrides: Any) -> dict:
    """Build one conformed silver row with sensible defaults.

    Args:
        fund_type (str): Report type.
        shareholders (int): The figure most tests vary.
        **overrides (Any): Any other column.

    Returns:
        dict: A row matching ``schema.SILVER_SCHEMA``.
    """
    row = {
        "fund_class_cnpj": "11111111000111",
        "subclass_id": None,
        "report_date": DAY,
        "fund_type": fund_type,
        "total_assets": Decimal("100.00"),
        "quota_value": Decimal("1.000000000000"),
        "net_assets": Decimal("100.00"),
        "subscriptions": Decimal("0.00"),
        "redemptions": Decimal("0.00"),
        "net_flow": Decimal("0.00"),
        "shareholders": shareholders,
        "reference_month": SEP_2026,
        "source_sha256": SHA,
    }
    return {**row, **overrides}


def _frame(*rows: dict) -> pl.DataFrame:
    """Turn rows into a conformed frame.

    Args:
        *rows (dict): Rows from ``_row``.

    Returns:
        pl.DataFrame: Frame matching ``schema.SILVER_SCHEMA``.
    """
    return pl.DataFrame(list(rows), schema=schema.SILVER_SCHEMA, orient="row")


def test_conform_renames_digits_the_cnpj_and_derives_net_flow() -> None:
    silver = domain.conform(_bronze_month())
    assert silver.schema == pl.Schema(schema.SILVER_SCHEMA)
    assert set(silver.get_column("fund_class_cnpj")) >= {"11111111000111", "22222222000122"}
    flows = silver.filter(pl.col("subscriptions") > 0).row(0, named=True)
    assert flows["net_flow"] == flows["subscriptions"] - flows["redemptions"]


def test_conform_rejects_a_cnpj_without_14_digits() -> None:
    bronze = _bronze_month().with_columns(pl.lit("12.345/0001").alias("cnpj_fundo_classe"))
    with pytest.raises(ValueError, match="14 digits"):
        domain.conform(bronze)


def test_parse_months_validates_and_deduplicates() -> None:
    assert domain.parse_months(["2026-09", "2025-10", "2026-09"]) == [
        dt.date(2025, 10, 1),
        SEP_2026,
    ]
    with pytest.raises(ValueError, match="YYYY-MM"):
        domain.parse_months(["2026-9"])


def test_unique_keys_pass_through_untouched() -> None:
    frame = _frame(_row(), _row(fund_class_cnpj="22222222000122"))
    kept, discarded = domain.resolve_duplicates(frame)
    assert kept.height == 2
    assert discarded.is_empty()


def test_a_repeated_line_collapses_to_one() -> None:
    kept, discarded = domain.resolve_duplicates(_frame(_row(), _row()))
    assert kept.height == 1
    assert discarded.get_column("discard_reason").to_list() == ["exact_duplicate"]


def test_same_figures_under_two_types_keep_the_class_report() -> None:
    kept, discarded = domain.resolve_duplicates(_frame(_row("FI"), _row("CLASSES - FIF")))
    assert kept.get_column("fund_type").to_list() == ["CLASSES - FIF"]
    assert discarded.get_column("fund_type").to_list() == ["FI"]
    assert discarded.get_column("discard_reason").to_list() == ["exact_duplicate"]


def test_different_figures_keep_the_class_report_over_the_legacy_one() -> None:
    """The real 2025-12 case: same day, 12,290 vs 12,306 shareholders."""
    frame = _frame(_row("FI", shareholders=12306), _row("CLASSES - FIF", shareholders=12290))
    kept, discarded = domain.resolve_duplicates(frame)
    assert kept.row(0, named=True)["shareholders"] == 12290
    assert discarded.row(0, named=True)["discard_reason"] == "superseded_by_class_report"


def test_a_repeated_class_report_plus_a_legacy_one_is_still_resolved() -> None:
    frame = _frame(_row("CLASSES - FIF", 5), _row("CLASSES - FIF", 5), _row("FI", 6))
    kept, discarded = domain.resolve_duplicates(frame)
    assert kept.row(0, named=True)["shareholders"] == 5
    assert sorted(discarded.get_column("discard_reason").to_list()) == [
        "exact_duplicate",
        "superseded_by_class_report",
    ]


def test_an_unresolvable_conflict_fails_loudly() -> None:
    frame = _frame(_row("CLASSES - FIF", 5), _row("CLASSES - FIF", 6))
    with pytest.raises(domain.UnresolvableConflictError, match="no rule to choose"):
        domain.resolve_duplicates(frame)


def test_subclasses_of_one_class_are_not_duplicates() -> None:
    frame = _frame(_row(), _row(subclass_id="SUB0000000001"))
    kept, discarded = domain.resolve_duplicates(frame)
    assert kept.height == 2
    assert discarded.is_empty()


def test_reconcile_accepts_a_month_that_adds_up() -> None:
    kept, discarded = domain.resolve_duplicates(
        _frame(_row(), _row(), _row(fund_class_cnpj="2" * 14))
    )
    domain.reconcile(SEP_2026, 3, kept, discarded)


@pytest.mark.parametrize(
    ("bronze_rows", "rows", "message"),
    [
        (3, [_row()], "!= bronze 3"),
        (2, [_row(), _row()], "duplicate grain"),
        (1, [_row(fund_class_cnpj=None)], "null key"),
        (1, [_row(reference_month=dt.date(2026, 8, 1))], "another month"),
    ],
)
def test_reconcile_names_the_broken_invariant(bronze_rows: int, rows: list, message: str) -> None:
    empty = pl.DataFrame(schema=schema.DISCARDED_SCHEMA)
    with pytest.raises(domain.ReconciliationError, match=message):
        domain.reconcile(SEP_2026, bronze_rows, _frame(*rows), empty)


@pytest.fixture
def lake(tmp_path: Path) -> runtime.Lakehouse:
    """Build a local-mode lakehouse inside a temporary directory.

    Args:
        tmp_path (Path): Per-test directory from pytest.

    Returns:
        runtime.Lakehouse: Tables and landing under ``tmp_path``.
    """
    return runtime.Lakehouse(runtime.Mode.LOCAL, str(tmp_path), str(tmp_path / "landing"))


def _rebuild(lake: runtime.Lakehouse, month: dt.date) -> tuple[int, int]:
    """Run the transform task's body for one month.

    Args:
        lake (runtime.Lakehouse): Where the tables live.
        month (dt.date): Month to rebuild.

    Returns:
        tuple[int, int]: Rows written to silver and to the discard log.
    """
    bronze = adapters.read_bronze_month(lake, month)
    kept, discarded = domain.resolve_duplicates(domain.conform(bronze))
    domain.reconcile(month, bronze.height, kept, discarded)
    adapters.write_month(lake, month, kept, discarded)
    return kept.height, discarded.height


def test_bronze_to_silver_end_to_end_and_idempotent(lake: runtime.Lakehouse) -> None:
    bronze = _bronze_month()
    duplicated = pl.concat([bronze, bronze.head(1)])
    bronze_adapters.write_bronze(lake, duplicated, SEP_2026)
    assert adapters.bronze_months(lake) == [SEP_2026]

    assert _rebuild(lake, SEP_2026) == (bronze.height, 1)
    first = pl.read_delta(adapters.silver_uri(lake)).sort(list(schema.SILVER_KEY))
    assert _rebuild(lake, SEP_2026) == (bronze.height, 1)
    assert pl.read_delta(adapters.silver_uri(lake)).sort(list(schema.SILVER_KEY)).equals(first)


def test_a_clean_republication_clears_old_discards(lake: runtime.Lakehouse) -> None:
    bronze = _bronze_month()
    bronze_adapters.write_bronze(lake, pl.concat([bronze, bronze.head(1)]), SEP_2026)
    _rebuild(lake, SEP_2026)
    bronze_adapters.write_bronze(lake, bronze, SEP_2026)

    assert _rebuild(lake, SEP_2026) == (bronze.height, 0)
    assert pl.read_delta(adapters.discarded_uri(lake)).is_empty()
