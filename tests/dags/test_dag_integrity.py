"""The highest-ROI gate in the repository: every DAG parses, or the build fails.

This is the Airflow-community equivalent of an entrypoint check — a broken import,
a missing dependency or a renamed file is caught here in seconds, instead of as an
import error in the scheduler after deploy. It needs no running Airflow: DagBag
parses the folder the same way the dag-processor does.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag
from airflow.serialization.serialized_objects import DagSerialization

REPO_ROOT = Path(__file__).resolve().parents[2]
DAGS_ROOT = REPO_ROOT / "dags"


def _expected_dag_files() -> set[Path]:
    """Return the files the house layout says must define a DAG.

    Returns:
        set[Path]: Every ``dags/<domain>/<name>/<name>.py`` that exists, resolved.
    """
    return {
        (folder / f"{folder.name}.py").resolve()
        for folder in DAGS_ROOT.glob("*/*")
        if (folder / f"{folder.name}.py").is_file()
    }


@pytest.fixture(scope="session")
def dag_bag() -> DagBag:
    """Parse dags/ once per session, exactly as the dag-processor does.

    Returns:
        DagBag: Every DAG found, plus any import errors.
    """
    return DagBag(dag_folder=str(DAGS_ROOT))


def test_no_import_errors(dag_bag: DagBag) -> None:
    assert dag_bag.import_errors == {}, f"DAGs failed to import: {dag_bag.import_errors}"


def test_every_dag_serializes(dag_bag: DagBag) -> None:
    """A DAG that parses but does not serialize never reaches the scheduler.

    The dag-processor stores every DAG in serialized form; DagBag parsing alone does not
    exercise that step. Caught in practice: a ``zoneinfo`` timezone in a timetable parses
    fine and fails here, leaving every run stuck in ``queued``.
    """
    for dag_id, dag in dag_bag.dags.items():
        try:
            DagSerialization.to_dict(dag)
        except Exception as error:
            pytest.fail(f"{dag_id} does not serialize: {error}")


def test_every_dag_file_registers_a_dag(dag_bag: DagBag) -> None:
    """A DAG file that parses cleanly but registers nothing is the silent failure.

    A decorated @dag function that is never called, or a DAG built inside a branch that
    does not run, imports without error and simply never appears in the scheduler. An
    empty dags/ is valid (the platform ships without domains); a file that should define
    a DAG and does not is not. This also catches a DAGS_ROOT pointing at the wrong folder.
    """
    registered = {Path(dag.fileloc).resolve() for dag in dag_bag.dags.values()}
    silent = _expected_dag_files() - registered
    assert not silent, f"DAG files that registered no DAG: {sorted(map(str, silent))}"


def test_every_dag_folder_is_a_complete_package(dag_bag: DagBag) -> None:
    """House convention: folder-per-DAG, file named after the folder, README present."""
    for dag_id, dag in dag_bag.dags.items():
        dag_file = Path(dag.fileloc)
        folder = dag_file.parent
        assert dag_file.stem == folder.name, (
            f"{dag_id}: file {dag_file.name} must be named after its folder ({folder.name})"
        )
        assert (folder / "README.md").exists(), f"{dag_id}: missing README.md in {folder}"


def test_every_task_alerts_on_failure(dag_bag: DagBag) -> None:
    """A failure nobody hears about is a failure that lasts until someone looks.

    Set once per DAG: default_args={"on_failure_callback": GoogleChatNotifier()}
    (plugins/alerting). It fires after the last retry, so retried blips stay quiet.
    """
    for dag_id, dag in dag_bag.dags.items():
        for task in dag.tasks:
            assert task.on_failure_callback, (
                f"{dag_id}.{task.task_id}: no on_failure_callback — wire GoogleChatNotifier "
                "through default_args"
            )


def test_no_dag_uses_catchup_accidentally(dag_bag: DagBag) -> None:
    """Backfills are explicit acts here, never a side effect of a schedule change.

    catchup=True turns an edited start_date into an unplanned backfill against a public
    source. Airflow 3 already defaults to False; pinning it keeps the default from
    becoming an assumption nobody wrote down.
    """
    for dag_id, dag in dag_bag.dags.items():
        assert dag.catchup is False, f"{dag_id}: catchup must be explicitly False"
