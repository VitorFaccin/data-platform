"""include.runtime: the local/cloud switch, storage locations, write-once and secrets.

The cloud side is exercised against stand-ins for the Google clients: what is proven
here is the contract with them (bucket, key, precondition, secret path), not GCP itself.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from google.api_core import exceptions as google_exceptions

from include import runtime

CLOUD_ENV = {
    "DATA_PLATFORM_MODE": "cloud",
    "GCP_PROJECT_ID": "my-project",
    "GCS_LANDING_BUCKET": "my-landing",
    "GCS_LAKEHOUSE_BUCKET": "my-lakehouse",
}


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start every test with none of the runtime variables set.

    Args:
        monkeypatch (pytest.MonkeyPatch): Pytest's environment patcher.
    """
    for name in (*CLOUD_ENV, "DATA_PLATFORM_WAREHOUSE", "GOOGLE_APPLICATION_CREDENTIALS"):
        monkeypatch.delenv(name, raising=False)


def _cloud(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> None:
    """Switch the environment to a complete cloud configuration.

    Args:
        monkeypatch (pytest.MonkeyPatch): Pytest's environment patcher.
        **overrides (str): Variables to replace; an empty string unsets one.
    """
    for name, value in {**CLOUD_ENV, **overrides}.items():
        if value:
            monkeypatch.setenv(name, value)
        else:
            monkeypatch.delenv(name, raising=False)


def test_mode_defaults_to_local() -> None:
    assert runtime.current_mode() is runtime.Mode.LOCAL


def test_unknown_mode_fails_loudly(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATA_PLATFORM_MODE", "gcp")
    with pytest.raises(runtime.ConfigurationError, match="expected one of: local, cloud"):
        runtime.current_mode()


def test_local_lakehouse_lives_under_the_warehouse_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATA_PLATFORM_WAREHOUSE", "/data")
    lake = runtime.lakehouse()
    assert lake.table_uri("bronze", "cvm", "fund_daily") == "/data/bronze/cvm/fund_daily"
    assert lake.landing_uri("cvm", "fund_daily", "a.zip") == "/data/landing/cvm/fund_daily/a.zip"
    assert lake.storage_options == {}


def test_cloud_lakehouse_uses_the_two_buckets(monkeypatch: pytest.MonkeyPatch) -> None:
    _cloud(monkeypatch)
    lake = runtime.lakehouse()
    assert (
        lake.table_uri("bronze", "cvm", "fund_daily") == "gs://my-lakehouse/bronze/cvm/fund_daily"
    )
    assert lake.landing_uri("cvm", "fund_daily", "a.zip") == "gs://my-landing/cvm/fund_daily/a.zip"


def test_cloud_without_a_bucket_names_what_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    _cloud(monkeypatch, GCS_LAKEHOUSE_BUCKET="")
    with pytest.raises(runtime.ConfigurationError, match="GCS_LAKEHOUSE_BUCKET"):
        runtime.lakehouse()


def test_cloud_hands_the_credentials_file_to_delta(monkeypatch: pytest.MonkeyPatch) -> None:
    _cloud(monkeypatch)
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/secrets/adc.json")
    assert runtime.lakehouse().storage_options == {
        "google_application_credentials": "/secrets/adc.json"
    }


def test_local_write_once_keeps_the_first_version(tmp_path: Path) -> None:
    lake = runtime.Lakehouse(runtime.Mode.LOCAL, str(tmp_path), str(tmp_path / "landing"))
    uri = lake.landing_uri("cvm", "fund_daily", "a.zip")
    assert lake.write_once(uri, b"first")
    assert not lake.write_once(uri, b"second")
    assert Path(uri).read_bytes() == b"first"
    assert list(Path(uri).parent.iterdir()) == [Path(uri)]


class _FakeBlob:
    """Stand-in for a GCS blob that enforces ``if_generation_match=0`` like the service."""

    def __init__(self, store: dict[str, bytes], key: str) -> None:
        """Bind the blob to a shared object store.

        Args:
            store (dict[str, bytes]): Objects already "in the bucket", by bucket/key.
            key (str): This blob's bucket/key.
        """
        self._store, self._key = store, key

    def upload_from_string(self, data: bytes, if_generation_match: int) -> None:
        """Store the object unless the precondition fails.

        Args:
            data (bytes): Object content.
            if_generation_match (int): 0 means "only if the object does not exist".

        Raises:
            google_exceptions.PreconditionFailed: When the object already exists.
        """
        assert if_generation_match == 0
        if self._key in self._store:
            raise google_exceptions.PreconditionFailed("object exists")
        self._store[self._key] = data


def test_cloud_write_once_relies_on_the_gcs_precondition(monkeypatch: pytest.MonkeyPatch) -> None:
    _cloud(monkeypatch)
    store: dict[str, bytes] = {}

    def _client(project: str) -> SimpleNamespace:
        assert project == "my-project"
        return SimpleNamespace(
            bucket=lambda name: SimpleNamespace(blob=lambda key: _FakeBlob(store, f"{name}/{key}"))
        )

    monkeypatch.setattr(runtime.storage, "Client", _client)
    lake = runtime.lakehouse()
    uri = lake.landing_uri("cvm", "fund_daily", "a.zip")
    assert lake.write_once(uri, b"first")
    assert not lake.write_once(uri, b"second")
    assert store == {"my-landing/cvm/fund_daily/a.zip": b"first"}


def test_local_secret_is_the_upper_cased_environment_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GOOGLE_CHAT_WEBHOOK_URL", "https://hook")
    assert runtime.get_secret("google_chat_webhook_url") == "https://hook"


def test_missing_or_empty_local_secret_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    assert runtime.get_secret("google_chat_webhook_url") is None
    monkeypatch.setenv("GOOGLE_CHAT_WEBHOOK_URL", "")
    assert runtime.get_secret("google_chat_webhook_url") is None


class _FakeSecretManager:
    """Stand-in for SecretManagerServiceClient holding a single secret."""

    secrets: dict[str, str] = {}

    def secret_version_path(self, project: str, secret: str, version: str) -> str:
        """Build the resource name the way the real client does.

        Args:
            project (str): Project id.
            secret (str): Secret id.
            version (str): Version alias.

        Returns:
            str: ``projects/<p>/secrets/<s>/versions/<v>``.
        """
        return f"projects/{project}/secrets/{secret}/versions/{version}"

    def access_secret_version(self, name: str) -> SimpleNamespace:
        """Return the payload of a version, or fail like the service.

        Args:
            name (str): Full version resource name.

        Returns:
            SimpleNamespace: Object shaped like the real response (``payload.data``).

        Raises:
            google_exceptions.NotFound: When the secret does not exist.
        """
        if name not in self.secrets:
            raise google_exceptions.NotFound(name)
        return SimpleNamespace(payload=SimpleNamespace(data=self.secrets[name].encode()))


def test_cloud_secret_comes_from_secret_manager(monkeypatch: pytest.MonkeyPatch) -> None:
    _cloud(monkeypatch)
    _FakeSecretManager.secrets = {
        "projects/my-project/secrets/google-chat-webhook-url/versions/latest": "https://hook"
    }
    monkeypatch.setattr(runtime.secretmanager, "SecretManagerServiceClient", _FakeSecretManager)
    monkeypatch.setenv("GOOGLE_CHAT_WEBHOOK_URL", "https://ignored-in-cloud-mode")
    assert runtime.get_secret("google_chat_webhook_url") == "https://hook"


def test_absent_cloud_secret_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    _cloud(monkeypatch)
    _FakeSecretManager.secrets = {}
    monkeypatch.setattr(runtime.secretmanager, "SecretManagerServiceClient", _FakeSecretManager)
    assert runtime.get_secret("google_chat_webhook_url") is None
