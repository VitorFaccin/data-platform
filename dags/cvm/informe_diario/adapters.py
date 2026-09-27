"""Every side effect of the informe diário DAG: HTTP, landing files and Delta tables."""

from __future__ import annotations

import datetime as dt
import email.utils
import os
import urllib.error
import urllib.request
from http.client import HTTPMessage
from pathlib import Path
from typing import Any

import polars as pl

from cvm.informe_diario.core import schema

_USER_AGENT = "data-platform/cvm-informe-diario (+https://github.com/VitorFaccin/data-platform)"
_TIMEOUT_SECONDS = 120
_MERGE_OPTIONS = {
    "predicate": " AND ".join(f"t.{key} = s.{key}" for key in schema.MANIFEST_KEY),
    "source_alias": "s",
    "target_alias": "t",
}
_REFRESHED_ON_REPUBLICATION = ("etag", "last_modified", "size_bytes", "last_seen_at")


def warehouse_root() -> Path:
    """Return the root of the local lakehouse (the ``warehouse`` volume in compose).

    Returns:
        Path: Directory holding landing files and Delta tables.

    Raises:
        NotImplementedError: For the gcp sink, not supported by this DAG yet — failing
            loudly beats writing somewhere nobody expects.
    """
    sink = os.environ.get("DATA_PLATFORM_SINK", "local")
    if sink != "local":
        raise NotImplementedError(f"DATA_PLATFORM_SINK={sink!r}: only 'local' is supported yet")
    return Path(os.environ.get("DATA_PLATFORM_WAREHOUSE", "/opt/airflow/warehouse"))


def landing_path(root: Path, month: dt.date, sha256: str) -> Path:
    """Return where one version of a month's file is kept, named by its fingerprint.

    Args:
        root (Path): Warehouse root.
        month (dt.date): First day of the month.
        sha256 (str): Fingerprint of the file content.

    Returns:
        Path: ``landing/cvm/informe_diario/reference_month=YYYY-MM/<sha256>.zip``.
    """
    return root / "landing/cvm/informe_diario" / f"reference_month={month:%Y-%m}" / f"{sha256}.zip"


def manifest_path(root: Path) -> Path:
    """Return the Delta table recording every version of every month that reached bronze.

    Args:
        root (Path): Warehouse root.

    Returns:
        Path: Location of the manifest Delta table.
    """
    return root / "landing/cvm/informe_diario/_manifest"


def bronze_path(root: Path) -> Path:
    """Return the bronze Delta table, partitioned by ``reference_month``.

    Args:
        root (Path): Warehouse root.

    Returns:
        Path: Location of the bronze Delta table.
    """
    return root / "bronze/cvm/informe_diario"


def _is_delta_table(path: Path) -> bool:
    """Tell whether a Delta table already exists at ``path``.

    Args:
        path (Path): Candidate table location.

    Returns:
        bool: True when the directory holds a Delta transaction log.
    """
    return (path / "_delta_log").is_dir()


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


def save_landing(path: Path, data: bytes) -> None:
    """Keep the raw file once; a known fingerprint is never rewritten.

    Written under a temporary name and renamed, so a crash mid-write never leaves a
    truncated file under a valid fingerprint.

    Args:
        path (Path): Target from ``landing_path``.
        data (bytes): File content.
    """
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(".partial")
    partial.write_bytes(data)
    partial.replace(path)


def current_version(root: Path, month: dt.date) -> schema.ManifestEntry | None:
    """Return the version of ``month`` currently in bronze: the latest one seen.

    Args:
        root (Path): Warehouse root.
        month (dt.date): First day of the month.

    Returns:
        schema.ManifestEntry | None: That version, or None if never ingested.
    """
    path = manifest_path(root)
    if not _is_delta_table(path):
        return None
    rows = (
        pl.scan_delta(str(path))
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


def write_bronze(root: Path, frame: pl.DataFrame, month: dt.date) -> None:
    """Replace exactly one month's partition of bronze.

    The predicate is the whole point: without it ``mode="overwrite"`` replaces the
    ENTIRE table, and re-running one month silently deletes all the others. Delta also
    rejects the write if any row falls outside the predicate.

    Args:
        root (Path): Warehouse root.
        frame (pl.DataFrame): Parsed rows of one month.
        month (dt.date): First day of the month the rows belong to.
    """
    path = bronze_path(root)
    options: dict[str, Any] = {"partition_by": [schema.BRONZE_PARTITION]}
    if _is_delta_table(path):
        options["predicate"] = f"{schema.BRONZE_PARTITION} = '{month.isoformat()}'"
    frame.write_delta(str(path), mode="overwrite", delta_write_options=options)


def record_version(root: Path, entry: dict[str, Any]) -> None:
    """Upsert one ingested version into the manifest (MERGE on the manifest key).

    Running it again with the same entry changes nothing but ``last_seen_at``.

    Args:
        root (Path): Warehouse root.
        entry (dict[str, Any]): Row built by ``domain.manifest_entry``.
    """
    path = manifest_path(root)
    frame = pl.DataFrame([entry], schema=schema.MANIFEST_SCHEMA)
    if not _is_delta_table(path):
        frame.write_delta(str(path), mode="error")
        return
    (
        frame.write_delta(str(path), mode="merge", delta_merge_options=_MERGE_OPTIONS)
        .when_matched_update(updates={c: f"s.{c}" for c in _REFRESHED_ON_REPUBLICATION})
        .when_not_matched_insert_all()
        .execute()
    )


def refresh_version(
    root: Path, month: dt.date, sha256: str, remote: schema.RemoteFile, now: dt.datetime
) -> None:
    """Record that a month was republished with identical bytes (new ETag, same content).

    Without this, the next run would meet an unknown ETag and download the file again,
    every day, until the content actually changed.

    Args:
        root (Path): Warehouse root.
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
    (
        frame.write_delta(
            str(manifest_path(root)), mode="merge", delta_merge_options=_MERGE_OPTIONS
        )
        .when_matched_update(updates={c: f"s.{c}" for c in _REFRESHED_ON_REPUBLICATION})
        .execute()
    )
