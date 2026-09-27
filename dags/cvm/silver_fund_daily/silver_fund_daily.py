"""CVM daily fund reports, bronze → silver. Runs when bronze announces changed months.

``plan_months`` reads which months changed from the bronze Asset events (or the
``months`` param on a manual run); ``transform_month`` is mapped over them: conform,
resolve duplicates, reconcile, then replace the month in silver and in the discard log.
``publish`` emits the silver Asset with the months written.
Full contract and the duplicate rules: README.md in this folder.
"""

from __future__ import annotations

import datetime as dt
import logging

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

from alerting.google_chat import GoogleChatNotifier
from cvm.assets import BRONZE_FUND_DAILY, CHANGED_MONTHS_KEY, SILVER_FUND_DAILY
from cvm.silver_fund_daily import adapters
from cvm.silver_fund_daily.core import domain
from include import runtime

log = logging.getLogger(__name__)


def _months_from_events() -> list[str]:
    """Collect the months carried by every bronze Asset event that triggered this run.

    Several bronze runs can land before silver starts; their months are united.

    Returns:
        list[str]: Distinct months (ISO dates) named by the events, possibly empty.
    """
    events = get_current_context()["triggering_asset_events"][BRONZE_FUND_DAILY]
    return sorted({month for event in events for month in event.extra.get(CHANGED_MONTHS_KEY, [])})


@task
def plan_months() -> list[str]:
    """Decide which months to rebuild: params, else the triggering events, else all of bronze.

    Returns:
        list[str]: Months as ISO dates, oldest first.
    """
    requested = get_current_context()["params"]["months"]
    if requested:
        return [month.isoformat() for month in domain.parse_months(requested)]
    from_events = _months_from_events()
    if from_events:
        return from_events
    return [month.isoformat() for month in adapters.bronze_months(runtime.lakehouse())]


@task(
    max_active_tis_per_dagrun=1,
    retries=1,
    retry_delay=dt.timedelta(minutes=1),
    map_index_template="{{ month_label }}",
)
def transform_month(month_iso: str) -> str:
    """Rebuild one month of silver from bronze, gated before anything is written.

    Instances run one at a time because all of them commit to the same two tables.

    Args:
        month_iso (str): First day of the month, ISO format.

    Returns:
        str: The month written, collected by ``publish``.

    Raises:
        ValueError: When bronze holds no rows for the month.
    """
    month = dt.date.fromisoformat(month_iso)
    get_current_context()["month_label"] = f"{month:%Y-%m}"
    lake = runtime.lakehouse()
    bronze = adapters.read_bronze_month(lake, month)
    if bronze.is_empty():
        raise ValueError(f"bronze holds no rows for {month:%Y-%m}")
    kept, discarded = domain.resolve_duplicates(domain.conform(bronze))
    domain.reconcile(month, bronze.height, kept, discarded)
    adapters.write_month(lake, month, kept, discarded)
    log.info(
        "%s: bronze %d → silver %d + discarded %d",
        f"{month:%Y-%m}",
        bronze.height,
        kept.height,
        discarded.height,
    )
    return month_iso


@task(outlets=[SILVER_FUND_DAILY], trigger_rule="none_failed_min_one_success")
def publish(written: list[str]) -> None:
    """Emit the silver Asset with the months rewritten.

    Args:
        written (list[str]): Months returned by the transform_month instances.
    """
    months = sorted(month for month in written if month)
    get_current_context()["outlet_events"][SILVER_FUND_DAILY].extra = {CHANGED_MONTHS_KEY: months}


@dag(
    dag_id="silver_cvm_fund_daily",
    schedule=[BRONZE_FUND_DAILY],
    start_date=pendulum.datetime(2026, 9, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={"on_failure_callback": GoogleChatNotifier()},
    params={
        "months": Param(
            None,
            type=["null", "array"],
            items={"type": "string", "pattern": r"^\d{4}-\d{2}$"},
            description="Months (YYYY-MM) to rebuild on a manual run. Empty = every month "
            "in bronze. Scheduled runs take the months from the bronze Asset events.",
        ),
    },
    doc_md=__doc__,
    tags=["cvm", "silver"],
)
def silver_fund_daily() -> None:
    """Wire the tasks: plan the months, rebuild each one, announce what changed."""
    publish(transform_month.expand(month_iso=plan_months()))


silver_fund_daily()
