"""Task-failure alerts to a Google Chat space, through an incoming webhook.

Wired into every DAG as ``default_args={"on_failure_callback": GoogleChatNotifier()}``;
the integrity gate fails the build if a task is missing it. Airflow fires the callback
only after the LAST retry fails, so a transient error that a retry absorbs never pages
anyone.

Two failure modes are handled on purpose:

- **No webhook available** (unset in ``.env``, absent from Secret Manager, or the
  secret cannot be read): the alert degrades to a log warning. A missing secret must not
  turn every failure into a second, confusing error.
- **The webhook call itself fails**: logged and swallowed. BaseNotifier re-raises
  whatever ``notify`` raises, and an alerting outage must never replace the task's real
  exception in the logs.

The webhook URL is a secret, read through ``include.runtime.get_secret``: from ``.env``
in local mode, from Secret Manager (``google-chat-webhook-url``) in cloud mode.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import Any

from airflow.sdk import BaseNotifier

from include import runtime

WEBHOOK_SECRET = "google_chat_webhook_url"
_MAX_ERROR_CHARS = 1_000
_TIMEOUT_SECONDS = 10


def build_failure_message(context: dict[str, Any]) -> dict[str, str]:
    """Render the chat message for one failed task instance.

    The error is truncated: enough to read it in the chat, with the full traceback one
    click away in the linked log.

    Args:
        context (dict[str, Any]): The Airflow context handed to ``on_failure_callback``.

    Returns:
        dict[str, str]: A Google Chat message payload (``{"text": ...}``).
    """
    ti = context["ti"]
    error = context.get("exception")
    error_text = f"{type(error).__name__}: {error}" if error else "no exception captured"
    if len(error_text) > _MAX_ERROR_CHARS:
        error_text = error_text[:_MAX_ERROR_CHARS] + " …"

    mapped = f" [map {ti.map_index}]" if ti.map_index is not None and ti.map_index >= 0 else ""
    lines = [
        f"🔴 *Task failed* — `{ti.dag_id}` › `{ti.task_id}`{mapped}",
        f"Run `{ti.run_id}` · attempt {ti.try_number}",
        f"```{error_text}```",
        f"<{ti.log_url}|Open the log>",
    ]
    return {"text": "\n".join(lines)}


def threaded_webhook_url(webhook_url: str, thread_key: str) -> str:
    """Add the thread parameters so every failure of one DAG run lands in one thread.

    A backfill that fails twelve mapped tasks becomes one thread with twelve replies,
    not twelve top-level messages burying everything else in the space.

    Args:
        webhook_url (str): The incoming-webhook URL as issued by Google Chat.
        thread_key (str): Any stable string identifying the conversation (the DAG run).

    Returns:
        str: The same URL with ``threadKey`` and ``messageReplyOption`` added.
    """
    parts = urllib.parse.urlsplit(webhook_url)
    query = urllib.parse.parse_qsl(parts.query)
    query += [
        ("threadKey", thread_key),
        ("messageReplyOption", "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD"),
    ]
    return urllib.parse.urlunsplit(parts._replace(query=urllib.parse.urlencode(query)))


class GoogleChatNotifier(BaseNotifier):
    """Post a task failure to the Google Chat space behind the webhook secret."""

    def notify(self, context: dict[str, Any]) -> None:
        """Send the alert and never raise (see the module docstring).

        A failed call is logged without the URL, which embeds the webhook key and token.

        Args:
            context (dict[str, Any]): The Airflow context handed to ``on_failure_callback``.
        """
        try:
            webhook_url = runtime.get_secret(WEBHOOK_SECRET)
        except Exception:
            self.log.exception("Could not read the %s secret — alert not sent.", WEBHOOK_SECRET)
            return
        if not webhook_url:
            self.log.warning("Secret %s is not set — failure alert not sent.", WEBHOOK_SECRET)
            return

        ti = context["ti"]
        request = urllib.request.Request(
            threaded_webhook_url(webhook_url, thread_key=f"{ti.dag_id}/{ti.run_id}"),
            data=json.dumps(build_failure_message(context)).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=UTF-8"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=_TIMEOUT_SECONDS):
                pass
        except Exception:
            self.log.exception("Google Chat alert failed for %s.%s", ti.dag_id, ti.task_id)
