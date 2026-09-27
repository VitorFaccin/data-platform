"""The failure alert: what it says, where it threads, and that it can never raise."""

from __future__ import annotations

import json
import urllib.parse
from types import SimpleNamespace

import pytest

from alerting import google_chat

WEBHOOK = "https://chat.googleapis.com/v1/spaces/AAA/messages?key=k&token=t"


def _context(map_index: int = -1, exception: Exception | None = None) -> dict:
    """Build a minimal failure-callback context with a fake task instance.

    Args:
        map_index (int): Map index of the failed instance; -1 when not mapped.
        exception (Exception | None): The error the task raised.

    Returns:
        dict: Context with ``ti`` and ``exception``, as the callback receives it.
    """
    ti = SimpleNamespace(
        dag_id="bronze_cvm_fund_daily",
        task_id="ingest_month",
        run_id="manual__2026-09-27T12:00:00+00:00",
        try_number=3,
        map_index=map_index,
        log_url="http://localhost:8080/dags/bronze_cvm_fund_daily/runs/x/tasks/ingest_month",
    )
    return {"ti": ti, "exception": exception}


def test_message_names_the_task_the_run_and_the_error() -> None:
    text = google_chat.build_failure_message(_context(exception=ValueError("bad layout")))["text"]
    assert "`bronze_cvm_fund_daily` › `ingest_month`" in text
    assert "attempt 3" in text
    assert "ValueError: bad layout" in text
    assert "|Open the log>" in text


def test_mapped_task_shows_its_map_index() -> None:
    text = google_chat.build_failure_message(_context(map_index=7))["text"]
    assert "[map 7]" in text


def test_long_errors_are_truncated() -> None:
    text = google_chat.build_failure_message(_context(exception=RuntimeError("x" * 5_000)))["text"]
    assert len(text) < 1_500
    assert "…" in text


def test_thread_parameters_keep_the_webhook_credentials() -> None:
    url = google_chat.threaded_webhook_url(WEBHOOK, thread_key="dag/run 1")
    query = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
    assert query["key"] == "k"
    assert query["token"] == "t"
    assert query["threadKey"] == "dag/run 1"
    assert query["messageReplyOption"] == "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD"


def test_without_a_webhook_nothing_is_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(google_chat.WEBHOOK_ENV_VAR, raising=False)

    def _must_not_be_called(*_: object, **__: object) -> None:
        raise AssertionError("urlopen called without a webhook configured")

    monkeypatch.setattr(google_chat.urllib.request, "urlopen", _must_not_be_called)
    google_chat.GoogleChatNotifier().notify(_context())


def test_posts_the_message_to_the_threaded_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(google_chat.WEBHOOK_ENV_VAR, WEBHOOK)
    sent = {}

    class _Response:
        def __enter__(self) -> _Response:
            return self

        def __exit__(self, *_: object) -> None:
            return None

    def _fake_urlopen(request: object, timeout: int) -> _Response:
        sent["url"] = request.full_url
        sent["body"] = json.loads(request.data)
        return _Response()

    monkeypatch.setattr(google_chat.urllib.request, "urlopen", _fake_urlopen)
    google_chat.GoogleChatNotifier().notify(_context())
    assert "threadKey=bronze_cvm_fund_daily" in sent["url"]
    assert "Task failed" in sent["body"]["text"]


def test_a_failing_webhook_never_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """An alerting outage must not replace the task's real exception."""
    monkeypatch.setenv(google_chat.WEBHOOK_ENV_VAR, WEBHOOK)

    def _boom(*_: object, **__: object) -> None:
        raise OSError("network down")

    monkeypatch.setattr(google_chat.urllib.request, "urlopen", _boom)
    google_chat.GoogleChatNotifier().notify(_context())
