"""Rules for ingesting CVM daily fund reports: which months to check, and how to parse.

Pure: no network, no disk, no Airflow. Bytes in, DataFrame out, so every rule is
provable with a hand-typed fixture in milliseconds.

The check window follows what CVM does: it rewrites the current and previous month
every night and the whole last-12-months window on Saturdays. Weekday runs check two
months; the Sunday run checks thirteen and absorbs Saturday's rewrite, keeping heavy
republications out of the weekday runs whose fresh data someone is waiting for.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import io
import zipfile

import polars as pl

from cvm.bronze_fund_daily.core import schema

SOURCE_URL = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{yyyymm}.zip"
FIRST_MONTHLY_FILE = dt.date(2021, 1, 1)
DAILY_WINDOW_MONTHS = 2
FULL_WINDOW_MONTHS = 13
FULL_WINDOW_WEEKDAY = 6

_MONEY = r"^-?\d+(\.\d{1,2})?$"
_QUOTA = r"^-?\d+(\.\d{1,12})?$"
_VALUE_FORMATS: dict[str, str] = {
    "DT_COMPTC": r"^\d{4}-\d{2}-\d{2}$",
    "VL_TOTAL": _MONEY,
    "VL_QUOTA": _QUOTA,
    "VL_PATRIM_LIQ": _MONEY,
    "CAPTC_DIA": _MONEY,
    "RESG_DIA": _MONEY,
    "NR_COTST": r"^\d+$",
}
_NULLABLE = {"ID_SUBCLASSE"}
_SAMPLE_SIZE = 3


class UnexpectedLayoutError(ValueError):
    """The published file does not match any layout this parser knows.

    Raised instead of casting permissively: a broken month must fail loudly here, not
    land looking healthy and surface weeks later in a report.
    """


def month_start(day: dt.date) -> dt.date:
    """Return the first day of the month containing ``day``.

    Args:
        day (dt.date): Any date.

    Returns:
        dt.date: First day of that month.
    """
    return day.replace(day=1)


def shift_months(month: dt.date, delta: int) -> dt.date:
    """Return the first day of the month ``delta`` months away from ``month``.

    Args:
        month (dt.date): Starting month (any day in it).
        delta (int): Months to move; negative moves back.

    Returns:
        dt.date: First day of the target month.
    """
    index = month.year * 12 + (month.month - 1) + delta
    return dt.date(index // 12, index % 12 + 1, 1)


def parse_month(value: str) -> dt.date:
    """Parse a ``YYYY-MM`` string into the first day of that month.

    Args:
        value (str): Month as typed in a DAG param, e.g. ``2025-10``.

    Returns:
        dt.date: First day of the month.

    Raises:
        ValueError: If the string is not exactly ``YYYY-MM``.
    """
    try:
        parsed = dt.datetime.strptime(value, "%Y-%m").date()
    except ValueError:
        raise ValueError(f"month must be YYYY-MM, got {value!r}") from None
    if parsed.strftime("%Y-%m") != value:
        raise ValueError(f"month must be YYYY-MM, got {value!r}")
    return parsed


def months_to_check(today: dt.date, full_window: bool = False) -> list[dt.date]:
    """Return the months a scheduled run checks: 2 on weekdays, 13 on Sundays.

    Args:
        today (dt.date): The run's date in America/Sao_Paulo.
        full_window (bool): Force the 13-month window regardless of the weekday.

    Returns:
        list[dt.date]: First days of the months, oldest first, never before 2021-01.
    """
    full = full_window or today.weekday() == FULL_WINDOW_WEEKDAY
    size = FULL_WINDOW_MONTHS if full else DAILY_WINDOW_MONTHS
    current = month_start(today)
    months = [shift_months(current, -back) for back in range(size - 1, -1, -1)]
    return [month for month in months if month >= FIRST_MONTHLY_FILE]


def validate_requested_months(values: list[str], today: dt.date) -> list[dt.date]:
    """Validate months passed explicitly (manual re-runs, backfills).

    Monthly files start in 2021-01; earlier years exist only as yearly files under
    ``HIST/``, which need their own parser and are out of this DAG's scope.

    Args:
        values (list[str]): Months as ``YYYY-MM`` strings, duplicates allowed.
        today (dt.date): The run's date in America/Sao_Paulo.

    Returns:
        list[dt.date]: Distinct first days of the months, oldest first.

    Raises:
        ValueError: On a malformed month, a month before 2021-01, or a future month.
    """
    months = sorted({parse_month(value) for value in values})
    too_early = [m for m in months if m < FIRST_MONTHLY_FILE]
    if too_early:
        raise ValueError(
            f"monthly files start in {FIRST_MONTHLY_FILE:%Y-%m}; {too_early[0]:%Y-%m} lives "
            "in the yearly HIST/ files, which this DAG does not read"
        )
    future = [m for m in months if m > month_start(today)]
    if future:
        raise ValueError(f"{future[0]:%Y-%m} is in the future")
    return months


def source_url(month: dt.date) -> str:
    """Return the download URL of the monthly file.

    Args:
        month (dt.date): First day of the month.

    Returns:
        str: Public CVM URL of that month's zip.
    """
    return SOURCE_URL.format(yyyymm=f"{month:%Y%m}")


def plan_targets(
    months: list[dt.date], today: dt.date, force: bool = False
) -> list[dict[str, str | bool]]:
    """Build one JSON-serialisable mapped-task input per month.

    ``may_be_missing`` is True only for the current month: early on the 1st its file
    may not exist yet, so a 404 there is a skip; on a past month it is a failure.

    Args:
        months (list[dt.date]): Months to ingest, first day of each.
        today (dt.date): The run's date in America/Sao_Paulo.
        force (bool): Re-ingest even when ETag and content are unchanged.

    Returns:
        list[dict[str, str | bool]]: Inputs with month, may_be_missing and force keys.
    """
    current = month_start(today)
    return [
        {"month": m.isoformat(), "may_be_missing": m == current, "force": force} for m in months
    ]


def sha256_hex(data: bytes) -> str:
    """Fingerprint a file's content: identical bytes always give the identical hash.

    Args:
        data (bytes): File content.

    Returns:
        str: SHA-256 as 64 hexadecimal characters.
    """
    return hashlib.sha256(data).hexdigest()


def etag_unchanged(remote: schema.RemoteFile, current: schema.ManifestEntry | None) -> bool:
    """Tell whether the file was left untouched since the version in bronze.

    CVM's ETag is the file's modification time plus its size (nginx default): a match
    proves no rewrite happened, at the cost of a HEAD instead of a download.

    Args:
        remote (schema.RemoteFile): Metadata from the HEAD request.
        current (schema.ManifestEntry | None): Version in bronze, None if never ingested.

    Returns:
        bool: True when the ETag matches the version in bronze.
    """
    return current is not None and current.etag == remote.etag


def content_unchanged(sha256: str, current: schema.ManifestEntry | None) -> bool:
    """Tell whether rewritten bytes are identical to the version in bronze.

    Compared with the version CURRENTLY in bronze, not every version ever seen: if a
    month goes A → B → A, bronze holds B and the return to A must rewrite it.

    Args:
        sha256 (str): Fingerprint of the freshly downloaded file.
        current (schema.ManifestEntry | None): Version in bronze, None if never ingested.

    Returns:
        bool: True when the content matches the version in bronze.
    """
    return current is not None and current.sha256 == sha256


def count_duplicate_keys(frame: pl.DataFrame) -> int:
    """Count rows sharing the bronze grain key with at least one other row.

    Counted, not rejected: the source publishes duplicates routinely, and the count in
    the manifest makes them visible without blocking the month.

    Args:
        frame (pl.DataFrame): Parsed bronze rows of one month.

    Returns:
        int: Number of rows involved in a duplicate key.
    """
    return int(frame.select(pl.struct(list(schema.BRONZE_KEY)).is_duplicated().sum()).item())


def manifest_entry(
    month: dt.date,
    sha256: str,
    remote: schema.RemoteFile,
    row_count: int,
    duplicate_key_rows: int,
    layout_version: int,
    landing_uri: str,
    now: dt.datetime,
) -> dict[str, object]:
    """Build the manifest row recording one ingested version of a month.

    Args:
        month (dt.date): First day of the month.
        sha256 (str): Fingerprint of the ingested file.
        remote (schema.RemoteFile): Metadata from the GET that produced the bytes.
        row_count (int): Rows written to bronze.
        duplicate_key_rows (int): Rows sharing the grain key with another row.
        layout_version (int): Layout detected by the parser (1 or 2).
        landing_uri (str): Where the raw file was kept (local path or ``gs://`` URI).
        now (dt.datetime): Ingestion instant, UTC.

    Returns:
        dict[str, object]: A row matching ``schema.MANIFEST_SCHEMA``.
    """
    return {
        "reference_month": month,
        "sha256": sha256,
        "etag": remote.etag,
        "last_modified": remote.last_modified,
        "size_bytes": remote.size_bytes,
        "row_count": row_count,
        "duplicate_key_rows": duplicate_key_rows,
        "layout_version": layout_version,
        "landing_uri": landing_uri,
        "first_ingested_at": now,
        "last_seen_at": now,
    }


def _fail_on_bad_values(frame: pl.DataFrame, month: dt.date) -> None:
    """Reject blanks where the source never leaves them, and values off their format.

    The format check runs BEFORE any cast because Polars rounds silently when casting
    text to Decimal ("1.239" becomes 1.24): extra precision is a layout change.

    Args:
        frame (pl.DataFrame): The file as read, every column still text.
        month (dt.date): Month being parsed, for the error message.

    Raises:
        UnexpectedLayoutError: On the first offending column, with sample values.
    """
    for column in frame.columns:
        values = frame.get_column(column)
        if column not in _NULLABLE and values.null_count():
            raise UnexpectedLayoutError(
                f"{month:%Y-%m}: {values.null_count()} empty values in {column}, which is "
                "never empty in the known layouts"
            )
        pattern = _VALUE_FORMATS.get(column)
        if pattern is None:
            continue
        bad = values.filter(values.is_not_null() & ~values.str.contains(pattern))
        if bad.len():
            raise UnexpectedLayoutError(
                f"{month:%Y-%m}: {bad.len()} values in {column} do not match {pattern}, "
                f"e.g. {bad.head(_SAMPLE_SIZE).to_list()}"
            )


def parse_fund_daily(zip_bytes: bytes, month: dt.date, sha256: str) -> pl.DataFrame:
    """Parse one monthly zip into bronze rows, asserting the layout at every step.

    Everything is read as text and cast explicitly: no type inference, no codec
    guessing. The content is ASCII, so a byte that is not valid UTF-8 means the file
    changed nature and fails the month.

    Args:
        zip_bytes (bytes): The file exactly as downloaded.
        month (dt.date): Month the file was requested for.
        sha256 (str): Fingerprint of ``zip_bytes``, recorded on every row for lineage.

    Returns:
        pl.DataFrame: Rows matching ``schema.BRONZE_SCHEMA`` exactly.

    Raises:
        UnexpectedLayoutError: On any deviation from the known layouts.
    """
    expected_member = f"inf_diario_fi_{month:%Y%m}.csv"
    try:
        archive = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except zipfile.BadZipFile:
        raise UnexpectedLayoutError(f"{month:%Y-%m}: not a zip file") from None
    members = archive.namelist()
    if members != [expected_member]:
        raise UnexpectedLayoutError(
            f"{month:%Y-%m}: expected exactly [{expected_member!r}] in the zip, got {members}"
        )
    raw = archive.read(expected_member)

    try:
        first_line = raw.decode("utf-8").split("\n", 1)[0]
    except UnicodeDecodeError as error:
        raise UnexpectedLayoutError(f"{month:%Y-%m}: not valid UTF-8 ({error})") from None

    header = tuple(first_line.rstrip("\r").split(";"))
    versions = [version for version, known in schema.LAYOUTS.items() if known == header]
    if not versions:
        raise UnexpectedLayoutError(f"{month:%Y-%m}: unknown header {header}")
    layout_version = versions[0]

    frame = pl.read_csv(raw, separator=";", infer_schema=False)
    if frame.is_empty():
        raise UnexpectedLayoutError(f"{month:%Y-%m}: header present but no rows")
    _fail_on_bad_values(frame, month)

    if layout_version == 1:
        frame = frame.rename(schema.V1_TO_V2_NAMES).with_columns(
            pl.lit(None, dtype=pl.String).alias("ID_SUBCLASSE")
        )
    frame = frame.rename(str.lower)

    typed = frame.with_columns(
        pl.col("dt_comptc").str.to_date("%Y-%m-%d"),
        pl.col("vl_total", "vl_patrim_liq", "captc_dia", "resg_dia").cast(schema.MONEY),
        pl.col("vl_quota").cast(schema.QUOTA),
        pl.col("nr_cotst").cast(pl.Int64),
        pl.lit(layout_version, dtype=pl.Int8).alias("layout_version"),
        pl.lit(month, dtype=pl.Date).alias("reference_month"),
        pl.lit(sha256, dtype=pl.String).alias("source_sha256"),
    )

    outside = typed.filter(pl.col("dt_comptc").dt.truncate("1mo") != month)
    if outside.height:
        raise UnexpectedLayoutError(
            f"{month:%Y-%m}: {outside.height} rows dated outside the month, e.g. "
            f"{outside.get_column('dt_comptc').head(_SAMPLE_SIZE).to_list()}"
        )
    return typed.select(pl.col(name).cast(dtype) for name, dtype in schema.BRONZE_SCHEMA.items())
