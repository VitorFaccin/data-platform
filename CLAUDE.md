# CLAUDE.md — working in this repository

Guidance for coding agents. Same content whatever the agent; `AGENTS.md` points here.

## Orientation

Batch data platform on Apache Airflow 3.3.1: a **Python-first lakehouse**. Brazilian
public data (CVM funds, CNPJ registry, BCB series) lands as **Delta Lake** tables in
bronze/silver/gold, transformed by **Polars**, with gold served through BigQuery.
Local-first: `docker compose up` runs everything with `DATA_PLATFORM_SINK=local` (Delta
tables under `warehouse/`); `gcp` routes the same DAGs to GCS + BigQuery declared in
`infra/` (Terraform, not yet applied).

## Reading order

1. `README.md` — what this is and the decisions with rationale.
2. `docs/ARCHITECTURE.md` — the DAG package model, layer promises, why Python-first,
   the two allowed write shapes.
3. The first domain under `dags/` (when it exists) — every later DAG copies its shape.
4. `infra/README.md` — the GCP footprint and the apply/destroy workflow.

## Decisions not to reverse silently

- **One DAG = one folder** under `dags/<domain>/<dag_name>/`; the DAG file and the test
  file are NAMED AFTER THE FOLDER (`<dag_name>/<dag_name>.py`,
  `tests/<domain>/<dag_name>/test_<dag_name>.py`). Never `dag.py`, never `main.py`.
- **`core/` holds exactly `schema.py` and `domain.py`, and stays pure**: no airflow, no
  google.*, no requests, no deltalake, no disk. What is not a contract or a rule is I/O →
  `adapters.py`. No `utils.py`, no `helpers.py`, no subpackages in `core/`.
- **Python-first transforms.** Bronze → gold logic is pure Polars functions in
  `core/domain.py`, unit-tested. No dbt; SQL only at the serving edge. The trade-off and
  what replaces each dbt feature is in `docs/ARCHITECTURE.md` — keep that table true.
- **Tables are Delta Lake, never loose parquet.**
- **Every write is idempotent by construction**: partition overwrite WITH a predicate,
  or `MERGE` on the business key. Never a blind append. Each DAG has a test that runs the
  write twice and asserts nothing changed.
- **Contracts are DataFrame schemas** (Polars dtypes + business key) in `core/schema.py`.
  Pydantic only for small typed inputs (params, API envelopes), never per row.
- **No top-level code in DAG files.** Heavy imports (`polars`, `deltalake`, google
  clients) go inside task functions; the processor re-parses continuously.
- **Unexpected input format RAISES** (`UnexpectedLayout`) — never a permissive cast.
- **Imports are fully qualified from the dags root** (`from cvm.<dag_name>.core import
  domain`) so two DAG folders can both have a `core/` without colliding in `sys.modules`.
- **Terraform owns containers (buckets, datasets, IAM, budget); the pipeline owns
  tables** (schema in `core/schema.py`, created by the first Delta write).
- **BigQuery serves gold only**; bronze and silver stay in the lake.
- **Spark only with a benchmark behind it**, and serverless (Dataproc). No Spark cluster
  in compose, no Databricks.
- **`.airflowignore` is glob-syntax** (set explicitly in compose) and must list any new
  non-DAG module pattern.
- Cross-DAG dependencies use **Assets**, not cron offsets and not sensors.

## Conventions

- Python: ruff-enforced (see `ruff.toml` for the why of each rule), Google docstrings,
  typed signatures, 100 columns.
- Tests mirror `dags/` by domain: `tests/<domain>/<dag_name>/test_<dag_name>.py`, with
  fixtures next to the test. Fixtures never contain real personal data.
- Comments explain WHY, not what. A future reader must find the reasoning, not narration.
- Conventional commits (`feat:`, `fix:`, `test:`, `docs:`, `ci:`, `infra:`).
- English everywhere in code and docs. Gold column names may follow the source's
  Portuguese vocabulary (`dim_fundo`, `fato_informe_diario`) — they are domain terms.
- Work lands through branches and pull requests, never direct pushes to `main`.

## Gotchas

- A Delta `mode="overwrite"` without a predicate replaces the WHOLE table.
- Runtime dependencies are baked into the image (`Dockerfile` consumes
  `requirements.txt` at build). Adding one = edit `requirements.txt` + rebuild
  (`docker compose up --build`). Never reintroduce `_PIP_ADDITIONAL_REQUIREMENTS`:
  boot-time installs re-resolve the tree on every container start.
- In Airflow 3.3, `DagBag` lives in `airflow.dag_processing.dagbag`; the
  `airflow.models.dagbag` path is a deprecated shim, and `include_examples` is gone.
- An Asset event signals producer success, not data quality. Quality gates are tasks.
- The `infra/` backend block is commented out so `terraform validate` runs offline in
  CI. Uncommenting it is part of the (manual, documented) first apply.

## Before calling a DAG change done

1. `ruff check .` and `pytest` pass locally.
2. The DAG's folder README still tells the truth (idempotency key, write shape, failure
   isolation, params, traps, consumers).
3. New non-DAG files match an `.airflowignore` pattern.
4. `docs/DATA_CONTRACT.md` lists any table the change publishes.
5. `docker compose up` + trigger still works for the local sink.
