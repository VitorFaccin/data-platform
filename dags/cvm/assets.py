"""The cvm domain's Assets: the signals its DAGs exchange, declared once.

A producer lists one of these in a task's ``outlets``; a consumer lists it in its
``schedule``. Declaring each Asset here, not as a string in two DAG files, makes a
typo between producer and consumer impossible — the name is the contract.
"""

from __future__ import annotations

from airflow.sdk import Asset

BRONZE_FUND_DAILY = Asset("bronze/cvm/fund_daily")
SILVER_FUND_DAILY = Asset("silver/cvm/fund_daily")

CHANGED_MONTHS_KEY = "months"
