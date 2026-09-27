"""Shared test setup.

Puts dags/ and plugins/ on sys.path the same way Airflow does, so tests import the
DAG-local packages (`<domain>.<dag_name>...`) and the plugins (`alerting...`) exactly
as the scheduler and the workers will.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

for folder in (REPO_ROOT / "dags", REPO_ROOT / "plugins"):
    if str(folder) not in sys.path:
        sys.path.insert(0, str(folder))
