"""The platform's runtime mode: where data lands and where secrets come from.

One switch, ``DATA_PLATFORM_MODE``, and this module is the only code that reads it:

- ``local`` (default): landing files and Delta tables in the ``warehouse`` Docker
  volume; secrets from environment variables (``.env``).
- ``cloud``: landing files in the landing bucket, Delta tables in the lakehouse bucket
  (GCS); secrets from Secret Manager.

DAGs never branch on the mode themselves: they ask for URIs and secrets here, so the
same task code runs in both. Cloud credentials come from Application Default
Credentials (``GOOGLE_APPLICATION_CREDENTIALS``); no code path handles a key.
"""

from __future__ import annotations

import enum
import os
from dataclasses import dataclass
from pathlib import Path

from google.api_core import exceptions as google_exceptions
from google.cloud import secretmanager, storage

MODE_ENV_VAR = "DATA_PLATFORM_MODE"
_DEFAULT_LOCAL_ROOT = "/opt/airflow/warehouse"
_GCS_SCHEME = "gs://"


class Mode(enum.StrEnum):
    """Where the platform reads and writes."""

    LOCAL = "local"
    CLOUD = "cloud"


class ConfigurationError(RuntimeError):
    """The environment does not describe a usable runtime (unknown mode, missing bucket)."""


def current_mode() -> Mode:
    """Return the mode selected by ``DATA_PLATFORM_MODE``, ``local`` when unset.

    Returns:
        Mode: The selected mode.

    Raises:
        ConfigurationError: On any value other than ``local`` or ``cloud``.
    """
    value = os.environ.get(MODE_ENV_VAR) or Mode.LOCAL
    try:
        return Mode(value)
    except ValueError:
        raise ConfigurationError(
            f"{MODE_ENV_VAR}={value!r}; expected one of: {', '.join(Mode)}"
        ) from None


def _required(name: str) -> str:
    """Return an environment variable that cloud mode cannot run without.

    Args:
        name (str): Variable name.

    Returns:
        str: Its value.

    Raises:
        ConfigurationError: When it is unset or empty.
    """
    value = os.environ.get(name)
    if not value:
        raise ConfigurationError(f"{name} must be set when {MODE_ENV_VAR}={Mode.CLOUD}")
    return value


def _split_gcs_uri(uri: str) -> tuple[str, str]:
    """Split ``gs://bucket/key`` into bucket and key.

    Args:
        uri (str): A GCS URI.

    Returns:
        tuple[str, str]: Bucket name and object key.
    """
    bucket, _, key = uri.removeprefix(_GCS_SCHEME).partition("/")
    return bucket, key


@dataclass(frozen=True)
class Lakehouse:
    """Where landing files and Delta tables live in the current mode.

    Attributes:
        mode (Mode): The mode these locations belong to.
        tables_root (str): Root of the Delta tables: a directory, or ``gs://<bucket>``.
        landing_root (str): Root of the raw files: a directory, or ``gs://<bucket>``.
    """

    mode: Mode
    tables_root: str
    landing_root: str

    def table_uri(self, layer: str, domain: str, table: str) -> str:
        """Return the location of a Delta table.

        Args:
            layer (str): ``bronze``, ``silver``, ``gold`` or ``control``.
            domain (str): Source domain, e.g. ``cvm``.
            table (str): Table name, e.g. ``fund_daily``.

        Returns:
            str: ``<tables_root>/<layer>/<domain>/<table>``.
        """
        return f"{self.tables_root}/{layer}/{domain}/{table}"

    def landing_uri(self, domain: str, dataset: str, *parts: str) -> str:
        """Return the location of a raw file in landing.

        Args:
            domain (str): Source domain, e.g. ``cvm``.
            dataset (str): Dataset name, e.g. ``fund_daily``.
            *parts (str): Remaining path segments, file name last.

        Returns:
            str: ``<landing_root>/<domain>/<dataset>/<parts...>``.
        """
        return "/".join([self.landing_root, domain, dataset, *parts])

    @property
    def storage_options(self) -> dict[str, str]:
        """Return the options delta-rs needs to reach the tables.

        Returns:
            dict[str, str]: Empty locally; the credentials file path in cloud mode.
        """
        credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if self.mode is Mode.CLOUD and credentials:
            return {"google_application_credentials": credentials}
        return {}

    def write_once(self, uri: str, data: bytes) -> bool:
        """Store a file only if nothing exists at ``uri`` yet.

        Locally the file is written under a temporary name and renamed, so a crash never
        leaves a truncated file at a valid name. On GCS the upload carries the
        precondition "object does not exist" (``if_generation_match=0``), which the
        service enforces atomically.

        Args:
            uri (str): Target location from ``landing_uri``.
            data (bytes): File content.

        Returns:
            bool: True when written, False when the file already existed.
        """
        if self.mode is Mode.CLOUD:
            bucket, key = _split_gcs_uri(uri)
            blob = storage.Client(project=os.environ.get("GCP_PROJECT_ID")).bucket(bucket).blob(key)
            try:
                blob.upload_from_string(data, if_generation_match=0)
            except google_exceptions.PreconditionFailed:
                return False
            return True
        path = Path(uri)
        if path.exists():
            return False
        path.parent.mkdir(parents=True, exist_ok=True)
        partial = path.with_name(path.name + ".partial")
        partial.write_bytes(data)
        partial.replace(path)
        return True


def lakehouse() -> Lakehouse:
    """Return the storage locations for the current mode.

    Returns:
        Lakehouse: Local directories, or the two GCS buckets in cloud mode.

    Raises:
        ConfigurationError: In cloud mode, when a bucket variable is missing.
    """
    mode = current_mode()
    if mode is Mode.CLOUD:
        return Lakehouse(
            mode=mode,
            tables_root=f"{_GCS_SCHEME}{_required('GCS_LAKEHOUSE_BUCKET')}",
            landing_root=f"{_GCS_SCHEME}{_required('GCS_LANDING_BUCKET')}",
        )
    root = os.environ.get("DATA_PLATFORM_WAREHOUSE", _DEFAULT_LOCAL_ROOT)
    return Lakehouse(mode=mode, tables_root=root, landing_root=f"{root}/landing")


def get_secret(name: str) -> str | None:
    """Return a secret by its logical name, from the source the mode dictates.

    Locally the secret is the environment variable ``NAME`` (upper-cased); in cloud mode
    it is the latest version of the Secret Manager secret ``name`` with underscores as
    hyphens, in ``GCP_PROJECT_ID``. ``google_chat_webhook_url`` is therefore
    ``GOOGLE_CHAT_WEBHOOK_URL`` in ``.env`` and ``google-chat-webhook-url`` in Secret
    Manager.

    Args:
        name (str): Logical secret name in snake_case.

    Returns:
        str | None: The secret, or None when it does not exist (the caller decides
            whether that is fatal).

    Raises:
        ConfigurationError: In cloud mode, when ``GCP_PROJECT_ID`` is missing.
    """
    if current_mode() is Mode.LOCAL:
        return os.environ.get(name.upper()) or None
    client = secretmanager.SecretManagerServiceClient()
    version = client.secret_version_path(
        _required("GCP_PROJECT_ID"), name.replace("_", "-"), "latest"
    )
    try:
        return client.access_secret_version(name=version).payload.data.decode("utf-8")
    except google_exceptions.NotFound:
        return None
