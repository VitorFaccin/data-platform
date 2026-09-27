# CLAUDE.md — working in this repository

Guidance for coding agents. Same content whatever the agent; `AGENTS.md` points here.

## Orientation

Batch data platform on Apache Airflow 3.3.1: a **Python-first lakehouse**. Brazilian
public data (CVM funds, CNPJ registry, BCB series) lands as **Delta Lake** tables in
bronze/silver/gold, transformed by **Polars**, with gold served through BigQuery.
Local-first: `docker compose up` runs everything with `DATA_PLATFORM_MODE=local` (landing
and Delta tables in the `warehouse` Docker volume, secrets from `.env`); `cloud` routes the
same DAGs to GCS + Secret Manager declared in `infra/` (Terraform, not yet applied).

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
- **Names say layer, domain and dataset**: folder `dags/<domain>/<layer>_<dataset>/`,
  `dag_id` `<layer>_<domain>_<dataset>` (`dags/cvm/bronze_fund_daily/` →
  `bronze_cvm_fund_daily`), tables `<layer>/<domain>/<dataset>`. Datasets get technical
  English names; the source's own name (e.g. CVM's "Informe Diário") goes in the README.
- **One DAG per layer**: a DAG writes exactly one layer; the next layer is its own DAG,
  scheduled by the previous layer's Asset.
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
- **Imports at the top of every module** (PEP 8, enforced by ruff `PLC0415`), never
  inside functions. No top-level *work* in DAG files (I/O, `Variable.get`, computation):
  the processor re-parses continuously. Measured cost of the imports: ~0.2 s per parse.
- **Task callables are module-level functions**; the `@dag` function only wires them.
- **Unexpected input format RAISES** (`UnexpectedLayoutError`) — never a permissive cast.
- **Imports are fully qualified from the dags root** (`from cvm.<dag_name>.core import
  domain`) so two DAG folders can both have a `core/` without colliding in `sys.modules`.
- **Terraform owns containers (buckets, datasets, IAM, budget); the pipeline owns
  tables** (schema in `core/schema.py`, created by the first Delta write).
- **BigQuery serves gold only**; bronze and silver stay in the lake.
- **The local/cloud switch is `DATA_PLATFORM_MODE`, read ONLY by `include/runtime.py`.**
  DAGs ask it for locations (`runtime.lakehouse()`) and secrets (`runtime.get_secret`);
  never read the mode, bucket variables or secret env vars directly, never hardcode a path.
- **Spark only with a benchmark behind it**, and serverless (Dataproc). No Spark cluster
  in compose, no Databricks.
- **`.airflowignore` is glob-syntax** (set explicitly in compose) and must list any new
  non-DAG module pattern.
- Cross-DAG dependencies use **Assets**, not cron offsets and not sensors.
- **Every task alerts on failure**: each DAG sets
  `default_args={"on_failure_callback": GoogleChatNotifier()}` (`from
  alerting.google_chat import GoogleChatNotifier`). The integrity gate enforces it.
- **Credentials never enter git**: keys live in `secrets/` (gitignored, mounted
  read-only); the webhook URL lives in `.env`. Never log the webhook URL — it embeds a
  key and token.

## Conventions

- Python: ruff-enforced (see `ruff.toml` for the why of each rule), typed signatures,
  100 columns. Every function has a Google docstring with typed `Args` (`name (type):
  ...`) and `Returns` (`type: ...`), one line each; the *why* goes in the docstring.
  Inline comments are rare — only for a reason the docstring cannot carry.
- Constants that define a public source (URLs, layouts) live in code; environment
  config in env vars; credentials never in code — see ARCHITECTURE "Configuration".
- Tests mirror `dags/` by domain: `tests/<domain>/<dag_name>/test_<dag_name>.py`, with
  fixtures next to the test. Fixtures never contain real personal data.
- Comments explain WHY, not what. A future reader must find the reasoning, not narration.
- Conventional commits (`feat:`, `fix:`, `test:`, `docs:`, `ci:`, `infra:`).
- English everywhere in code, docs, table names and new columns. Columns copied from a
  source keep the source's names (`cnpj_fundo_classe`, `vl_quota`).
- Work lands through branches and pull requests, never direct pushes to `main`.

## Gotchas

- A Delta `mode="overwrite"` without a predicate replaces the WHOLE table.
- The local lakehouse is a named Docker volume, never a bind mount: Docker Desktop's
  Windows bind mounts break delta-rs multipart writes (files over ~10 MB).
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
5. Every task has the failure alert (the integrity gate says so).
6. `docker compose up` + trigger still works in local mode.
