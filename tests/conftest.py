"""Shared test setup.

Mirrors the runtime sys.path: dags/ and plugins/ (added by Airflow) and the repository
root (PYTHONPATH in compose), so tests import `<domain>.<dag_name>`, `alerting` and
`include` exactly as the scheduler and the workers do.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

for folder in (REPO_ROOT, REPO_ROOT / "dags", REPO_ROOT / "plugins"):
    if str(folder) not in sys.path:
        sys.path.insert(0, str(folder))
