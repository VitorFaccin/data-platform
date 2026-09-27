"""CVM informe diário → bronze, daily at 05:00 America/Sao_Paulo.

``plan_months`` decides which months the run checks; ``ingest_month`` is mapped over
them, one instance per month. An instance skips when the file is unchanged (ETag) or
republished with identical bytes (sha256); otherwise it keeps the raw file in landing,
parses it against the known layouts and replaces that month's bronze partition.
Full contract, schedule rationale and traps: README.md in this folder.
"""

from __future__ import annotations

import datetime as dt

import pendulum
from airflow.sdk import CronTriggerTimetable, Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from alerting.google_chat import GoogleChatNotifier
from cvm.informe_diario import adapters
from cvm.informe_diario.core import domain

TIMEZONE = pendulum.timezone("America/Sao_Paulo")


@task
def plan_months() -> list[dict[str, str | bool]]:
    """Decide which months this run checks: explicit params, else the scheduled window.

    Returns:
        list[dict[str, str | bool]]: One mapped-task input per month, oldest first.
    """
    context = get_current_context()
    today = context["dag_run"].run_after.astimezone(TIMEZONE).date()
    params = context["params"]
    if params["months"]:
        months = domain.validate_requested_months(params["months"], today)
    else:
        months = domain.months_to_check(today, full_window=params["full_window"])
    return domain.plan_targets(months, today, force=params["force"])


@task(
    max_active_tis_per_dagrun=1,
    retries=3,
    retry_delay=dt.timedelta(minutes=2),
    retry_exponential_backoff=True,
    map_index_template="{{ month_label }}",
)
def ingest_month(target: dict[str, str | bool]) -> None:
    """Ingest one month into bronze, skipping when the source did not change.

    Instances run one at a time because all of them commit to the same two Delta
    tables. Order is chosen for crash safety: landing, then bronze, then manifest — a
    crash before the manifest makes the next run redo the month, never skip it.

    Args:
        target (dict[str, str | bool]): Month (ISO date), may_be_missing and force flags.

    Raises:
        AirflowSkipException: When the month is unchanged or not published yet.
        FileNotFoundError: When a past month answers 404.
    """
    month = dt.date.fromisoformat(str(target["month"]))
    get_current_context()["month_label"] = f"{month:%Y-%m}"
    root = adapters.warehouse_root()
    url = domain.source_url(month)

    remote = adapters.head(url)
    if remote is None:
        if target["may_be_missing"]:
            raise AirflowSkipException(f"{month:%Y-%m} not published yet (404)")
        raise FileNotFoundError(f"{url} answered 404 for a past month")

    current = adapters.current_version(root, month)
    if not target["force"] and domain.etag_unchanged(remote, current):
        raise AirflowSkipException(f"{month:%Y-%m} unchanged (ETag {remote.etag})")

    data, remote = adapters.download(url)
    sha256 = domain.sha256_hex(data)
    now = dt.datetime.now(dt.UTC)
    if not target["force"] and domain.content_unchanged(sha256, current):
        adapters.refresh_version(root, month, sha256, remote, now)
        raise AirflowSkipException(f"{month:%Y-%m} republished with identical content")

    landing = adapters.landing_path(root, month, sha256)
    adapters.save_landing(landing, data)
    frame = domain.parse_informe(data, month, sha256)
    adapters.write_bronze(root, frame, month)
    adapters.record_version(
        root,
        domain.manifest_entry(
            month=month,
            sha256=sha256,
            remote=remote,
            row_count=frame.height,
            duplicate_key_rows=domain.count_duplicate_keys(frame),
            layout_version=int(frame.get_column("layout_version")[0]),
            landing_path=str(landing.relative_to(root)),
            now=now,
        ),
    )


@dag(
    dag_id="cvm_informe_diario",
    schedule=CronTriggerTimetable("0 5 * * *", timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 9, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args={"on_failure_callback": GoogleChatNotifier()},
    params={
        "months": Param(
            None,
            type=["null", "array"],
            items={"type": "string", "pattern": r"^\d{4}-\d{2}$"},
            description="Explicit months (YYYY-MM) to check. Empty = the scheduled window.",
        ),
        "full_window": Param(
            False,
            type="boolean",
            description="Check the 13-month window (the Sunday behaviour) on any day.",
        ),
        "force": Param(
            False,
            type="boolean",
            description="Re-ingest even when ETag and content are unchanged (e.g. after "
            "a parser fix).",
        ),
    },
    doc_md=__doc__,
    tags=["cvm", "bronze"],
)
def informe_diario() -> None:
    """Wire the tasks: plan the months, then ingest each one."""
    ingest_month.expand(target=plan_months())


informe_diario()
