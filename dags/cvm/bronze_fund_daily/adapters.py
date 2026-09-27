"""Every side effect of the bronze_cvm_fund_daily DAG: HTTP, landing and Delta tables.

Locations come from ``include.runtime.Lakehouse``, so the same functions write to the
local volume or to GCS depending on ``DATA_PLATFORM_MODE``.
"""

from __future__ import annotations

import datetime as dt
import email.utils
import urllib.error
import urllib.request
from http.client import HTTPMessage
from typing import Any

import polars as pl

from cvm.bronze_fund_daily.core import schema
from include import delta
from include.runtime import Lakehouse

DOMAIN = "cvm"
DATASET = "fund_daily"
_USER_AGENT = "data-platform/bronze-cvm-fund-daily (+https://github.com/VitorFaccin/data-platform)"
_TIMEOUT_SECONDS = 120
_REFRESHED_ON_REPUBLICATION = ("etag", "last_modified", "size_bytes", "last_seen_at")


def landing_uri(lake: Lakehouse, month: dt.date, sha256: str) -> str:
    """Return where one version of a month's file is kept, named by its fingerprint.

    Args:
        lake (Lakehouse): Storage locations for the current mode.
        month (dt.date): First day of the month.
        sha256 (str): Fingerprint of the file content.

    Returns:
        str: ``<landing>/cvm/fund_daily/reference_month=YYYY-MM/<sha256>.zip``.
    """
    return lake.landing_uri(DOMAIN, DATASET, f"reference_month={month:%Y-%m}", f"{sha256}.zip")


def manifest_uri(lake: Lakehouse) -> str:
    """Return the Delta table recording every version of every month that reached bronze.

    It lives with the tables, not in landing: the landing bucket archives old objects,
    which a Delta log must never be.

    Args:
        lake (Lakehouse): Storage locations for the current mode.

    Returns:
        str: ``<tables>/control/cvm/fund_daily_manifest``.
    """
    return lake.table_uri("control", DOMAIN, f"{DATASET}_manifest")


def bronze_uri(lake: Lakehouse) -> str:
    """Return the bronze Delta table, partitioned by ``reference_month``.

    Args:
        lake (Lakehouse): Storage locations for the current mode.

    Returns:
        str: ``<tables>/bronze/cvm/fund_daily``.
    """
    return lake.table_uri("bronze", DOMAIN, DATASET)


def _remote_file(url: str, headers: HTTPMessage) -> schema.RemoteFile:
    """Build the file metadata from an HTTP response's headers.

    Args:
        url (str): Requested URL.
        headers (HTTPMessage): Response headers (HEAD or GET).

    Returns:
        schema.RemoteFile: ETag, modification time and size of the file.
    """
    return schema.RemoteFile(
        url=url,
        etag=headers["ETag"],
        last_modified=email.utils.parsedate_to_datetime(headers["Last-Modified"]),
        size_bytes=int(headers["Content-Length"]),
    )


def head(url: str) -> schema.RemoteFile | None:
    """Ask the source about a file without downloading it.

    The User-Agent names this project, so a public-data publisher that sees a problem
    can tell who is calling.

    Args:
        url (str): File URL.

    Returns:
        schema.RemoteFile | None: File metadata, or None when the source answers 404.
    """
    request = urllib.request.Request(url, method="HEAD", headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUT_SECONDS) as response:
            return _remote_file(url, response.headers)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise


def download(url: str) -> tuple[bytes, schema.RemoteFile]:
    """Download a file together with the metadata of the SAME response.

    The ETag comes from the GET, not the earlier HEAD: if CVM rewrote the file in
    between, the recorded ETag must describe the bytes actually ingested.

    Args:
        url (str): File URL.

    Returns:
        tuple[bytes, schema.RemoteFile]: File content and its metadata.

    Raises:
        OSError: When fewer bytes arrive than Content-Length announced.
    """
    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(request, timeout=_TIMEOUT_SECONDS) as response:
        data = response.read()
        remote = _remote_file(url, response.headers)
    if len(data) != remote.size_bytes:
        raise OSError(f"{url}: got {len(data)} bytes, Content-Length said {remote.size_bytes}")
    return data, remote


def current_version(lake: Lakehouse, month: dt.date) -> schema.ManifestEntry | None:
    """Return the version of ``month`` currently in bronze: the latest one seen.

    Args:
        lake (Lakehouse): Storage locations for the current mode.
        month (dt.date): First day of the month.

    Returns:
        schema.ManifestEntry | None: That version, or None if never ingested.
    """
    uri = manifest_uri(lake)
    if not delta.table_exists(lake, uri):
        return None
    rows = (
        pl.scan_delta(uri, storage_options=lake.storage_options)
        .filter(pl.col("reference_month") == month)
        .sort("last_seen_at", descending=True)
        .head(1)
        .collect()
    )
    if rows.is_empty():
        return None
    row = rows.row(0, named=True)
    return schema.ManifestEntry(
        reference_month=row["reference_month"], sha256=row["sha256"], etag=row["etag"]
    )


def write_bronze(lake: Lakehouse, frame: pl.DataFrame, month: dt.date) -> None:
    """Replace exactly one month's partition of bronze.

    Args:
        lake (Lakehouse): Storage locations for the current mode.
        frame (pl.DataFrame): Parsed rows of one month.
        month (dt.date): First day of the month the rows belong to.
    """
    delta.overwrite_partition(lake, bronze_uri(lake), frame, schema.BRONZE_PARTITION, month)


def record_version(lake: Lakehouse, entry: dict[str, Any]) -> None:
    """Upsert one ingested version into the manifest (MERGE on the manifest key).

    Running it again with the same entry changes nothing but ``last_seen_at``.

    Args:
        lake (Lakehouse): Storage locations for the current mode.
        entry (dict[str, Any]): Row built by ``domain.manifest_entry``.
    """
    frame = pl.DataFrame([entry], schema=schema.MANIFEST_SCHEMA)
    delta.upsert(lake, manifest_uri(lake), frame, schema.MANIFEST_KEY, _REFRESHED_ON_REPUBLICATION)


def refresh_version(
    lake: Lakehouse, month: dt.date, sha256: str, remote: schema.RemoteFile, now: dt.datetime
) -> None:
    """Record that a month was republished with identical bytes (new ETag, same content).

    Without this, the next run would meet an unknown ETag and download the file again,
    every day, until the content actually changed.

    Args:
        lake (Lakehouse): Storage locations for the current mode.
        month (dt.date): First day of the month.
        sha256 (str): Fingerprint of the (unchanged) content.
        remote (schema.RemoteFile): Metadata of the republished file.
        now (dt.datetime): Instant of the check, UTC.
    """
    columns = (*schema.MANIFEST_KEY, *_REFRESHED_ON_REPUBLICATION)
    frame = pl.DataFrame(
        [
            {
                "reference_month": month,
                "sha256": sha256,
                "etag": remote.etag,
                "last_modified": remote.last_modified,
                "size_bytes": remote.size_bytes,
                "last_seen_at": now,
            }
        ],
        schema={column: schema.MANIFEST_SCHEMA[column] for column in columns},
    )
    delta.update_existing(
        lake, manifest_uri(lake), frame, schema.MANIFEST_KEY, _REFRESHED_ON_REPUBLICATION
    )
