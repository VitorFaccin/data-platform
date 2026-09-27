"""Contracts for the silver daily fund table and its discard log.

- **Input is bronze's own contract.** Silver reads bronze through
  ``cvm.bronze_fund_daily.core.schema``: a change there breaks this module at import,
  loudly, instead of producing a silently wrong table.
- **Conformed vocabulary.** English column names, and the CNPJ as 14 digits — the
  registry's spelling — so silver joins the fund registry without reformatting.
- **Grain enforced.** One row per ``(fund_class_cnpj, subclass_id, report_date)``.
- **Nothing disappears.** Every bronze row not kept goes to the discard log with its
  reason, so ``silver + discarded = bronze`` for every month.
"""

from __future__ import annotations

import enum

import polars as pl

from cvm.bronze_fund_daily.core import schema as bronze

BRONZE_TO_SILVER: dict[str, str] = {
    "tp_fundo_classe": "fund_type",
    "cnpj_fundo_classe": "fund_class_cnpj",
    "id_subclasse": "subclass_id",
    "dt_comptc": "report_date",
    "vl_total": "total_assets",
    "vl_quota": "quota_value",
    "vl_patrim_liq": "net_assets",
    "captc_dia": "subscriptions",
    "resg_dia": "redemptions",
    "nr_cotst": "shareholders",
}

SILVER_SCHEMA: dict[str, pl.DataType] = {
    "fund_class_cnpj": pl.String(),
    "subclass_id": pl.String(),
    "report_date": pl.Date(),
    "fund_type": pl.String(),
    "total_assets": bronze.MONEY,
    "quota_value": bronze.QUOTA,
    "net_assets": bronze.MONEY,
    "subscriptions": bronze.MONEY,
    "redemptions": bronze.MONEY,
    "net_flow": bronze.MONEY,
    "shareholders": pl.Int64(),
    "reference_month": pl.Date(),
    "source_sha256": pl.String(),
}
SILVER_KEY: tuple[str, ...] = ("fund_class_cnpj", "subclass_id", "report_date")
SILVER_PARTITION = "reference_month"
FIGURES: tuple[str, ...] = (
    "total_assets",
    "quota_value",
    "net_assets",
    "subscriptions",
    "redemptions",
    "shareholders",
)

DISCARDED_SCHEMA: dict[str, pl.DataType] = {**SILVER_SCHEMA, "discard_reason": pl.String()}

CLASS_REPORT_TYPES = frozenset({"CLASSES - FIF", "CLASSE FIF/FAPI"})
LEGACY_REPORT_TYPES = frozenset({"FI", "FAPI"})


class DiscardReason(enum.StrEnum):
    """Why a bronze row did not become a silver row."""

    EXACT_DUPLICATE = "exact_duplicate"
    SUPERSEDED_BY_CLASS_REPORT = "superseded_by_class_report"
