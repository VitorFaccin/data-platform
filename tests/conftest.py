"""Shared test setup.

Puts dags/ on sys.path the same way Airflow does at parse time, so tests import the
DAG-local packages (`<domain>.<dag_name>...`) exactly as the scheduler will.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DAGS_ROOT = REPO_ROOT / "dags"

if str(DAGS_ROOT) not in sys.path:
    sys.path.insert(0, str(DAGS_ROOT))
