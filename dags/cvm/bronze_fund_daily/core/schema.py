"""Contracts for the CVM daily fund bronze table and its ingestion manifest.

Pure declarations: no I/O, no Airflow; Polars is imported for its dtypes only.

- **Layouts.** CVM has published two headers. The switch happened in the 2023-12 file
  (CVM Resolution 175: funds became classes with optional subclasses). Any other header
  is rejected, never guessed.
- **One bronze for both layouts.** v1 columns are relabelled with their v2 names and
  ``id_subclasse`` is null for v1 rows. Values are untouched; the semantic difference
  (fund CNPJ before 2023-12, class CNPJ after) is reconciled in silver.
- **Decimal, not float.** Money has 2 places and the quota 12, as published; a float
  cannot represent 0.10 exactly and sums of fund balances drift in the cents.
- **Grain not enforced in bronze.** One row per class (or subclass) per day is the
  intent, but the source repeats rows routinely (7 of the first 15 months ingested).
  Bronze keeps what was published, the manifest counts duplicates, silver resolves them.
- **Manifest.** One row per distinct content of a month: an identical republication
  refreshes the row (etag, last_seen_at); different bytes add a row.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import polars as pl

LAYOUT_V1_HEADER: tuple[str, ...] = (
    "TP_FUNDO",
    "CNPJ_FUNDO",
    "DT_COMPTC",
    "VL_TOTAL",
    "VL_QUOTA",
    "VL_PATRIM_LIQ",
    "CAPTC_DIA",
    "RESG_DIA",
    "NR_COTST",
)
LAYOUT_V2_HEADER: tuple[str, ...] = (
    "TP_FUNDO_CLASSE",
    "CNPJ_FUNDO_CLASSE",
    "ID_SUBCLASSE",
    "DT_COMPTC",
    "VL_TOTAL",
    "VL_QUOTA",
    "VL_PATRIM_LIQ",
    "CAPTC_DIA",
    "RESG_DIA",
    "NR_COTST",
)
LAYOUTS: dict[int, tuple[str, ...]] = {1: LAYOUT_V1_HEADER, 2: LAYOUT_V2_HEADER}
V1_TO_V2_NAMES: dict[str, str] = {"TP_FUNDO": "TP_FUNDO_CLASSE", "CNPJ_FUNDO": "CNPJ_FUNDO_CLASSE"}

MONEY = pl.Decimal(precision=38, scale=2)
QUOTA = pl.Decimal(precision=38, scale=12)

BRONZE_SCHEMA: dict[str, pl.DataType] = {
    "tp_fundo_classe": pl.String(),
    "cnpj_fundo_classe": pl.String(),
    "id_subclasse": pl.String(),
    "dt_comptc": pl.Date(),
    "vl_total": MONEY,
    "vl_quota": QUOTA,
    "vl_patrim_liq": MONEY,
    "captc_dia": MONEY,
    "resg_dia": MONEY,
    "nr_cotst": pl.Int64(),
    "layout_version": pl.Int8(),
    "reference_month": pl.Date(),
    "source_sha256": pl.String(),
}
BRONZE_KEY: tuple[str, ...] = ("cnpj_fundo_classe", "id_subclasse", "dt_comptc")
BRONZE_PARTITION = "reference_month"

MANIFEST_SCHEMA: dict[str, pl.DataType] = {
    "reference_month": pl.Date(),
    "sha256": pl.String(),
    "etag": pl.String(),
    "last_modified": pl.Datetime(time_unit="us", time_zone="UTC"),
    "size_bytes": pl.Int64(),
    "row_count": pl.Int64(),
    "duplicate_key_rows": pl.Int64(),
    "layout_version": pl.Int8(),
    "landing_path": pl.String(),
    "first_ingested_at": pl.Datetime(time_unit="us", time_zone="UTC"),
    "last_seen_at": pl.Datetime(time_unit="us", time_zone="UTC"),
}
MANIFEST_KEY: tuple[str, ...] = ("reference_month", "sha256")


@dataclass(frozen=True)
class RemoteFile:
    """Metadata the source returns about a published file.

    Attributes:
        url (str): Where the file was requested from.
        etag (str): Source ETag, as returned (quotes included).
        last_modified (dt.datetime): Source modification time, UTC.
        size_bytes (int): Content-Length of the file.
    """

    url: str
    etag: str
    last_modified: dt.datetime
    size_bytes: int


@dataclass(frozen=True)
class ManifestEntry:
    """The version of a month currently in bronze, per the manifest.

    Attributes:
        reference_month (dt.date): First day of the month.
        sha256 (str): Fingerprint of the file whose rows are in bronze.
        etag (str): Source ETag last seen for that content.
    """

    reference_month: dt.date
    sha256: str
    etag: str
