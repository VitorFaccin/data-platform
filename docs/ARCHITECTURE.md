# Architecture

This document explains the repository's shape and its load-bearing decisions in more
depth than the README table. It is the record to revisit when someone proposes changing
them.

## The three layers inside a DAG package

```
dags/<domain>/<dag_name>/
├── <dag_name>.py    ORCHESTRATION   what runs, in which order, what happens on failure
├── adapters.py      I/O             every side effect; the only file that touches the world
└── core/            PURE            schema.py (contracts) + domain.py (rules) — nothing else
```

The flow of a task body is always the same sandwich: **impure → pure → impure**
(read → parse/validate/transform → write). Retries belong to the impure edges — retrying
a pure function is a no-op by definition, so retry config lives on tasks whose work is I/O.

Dependencies point inward: the DAG file imports adapters and core; adapters imports
core.schema; core imports nothing of ours. `core/` never imports airflow, google.*,
requests, deltalake, or touches disk — which is what lets `tests/<domain>/<dag>/` run in
milliseconds against an in-memory fixture.

**What a contract is here.** Tables are validated as whole DataFrames, not row by row:
`schema.py` declares each table as a Polars schema (column → dtype, nullability) plus the
business key that makes writes idempotent. Pydantic models are for small typed inputs —
DAG params, one API response envelope — never for millions of rows, where per-row object
construction would dominate the runtime.

## Naming, and one DAG per layer

A name tells where the DAG sits without opening it: folder `dags/<domain>/<layer>_<dataset>/`,
`dag_id` `<layer>_<domain>_<dataset>`, tables at `<layer>/<domain>/<dataset>`. For example
`dags/cvm/bronze_fund_daily/` defines `bronze_cvm_fund_daily`, which writes
`bronze/cvm/fund_daily`. Datasets get technical English names; a source's own name (CVM
calls this one "Informe Diário") is recorded in the DAG README for whoever searches the
source.

The layer prefix is a promise that **a DAG writes exactly one layer**. Silver is its own
DAG, scheduled by the bronze Asset; gold is scheduled by the silver Assets. Each DAG
stays small, fails and retries on its own, and the hand-offs between layers are visible
as Assets in the UI instead of hidden inside one long task chain.

## Module conventions

**Imports at the top of the module (PEP 8), never inside functions** — enforced by ruff
(`PLC0415`). The common Airflow advice is the opposite: import heavy libraries inside the
task, because the dag-processor re-imports every DAG file on every parse. Measured in
`apache/airflow:3.3.1-python3.13` (fresh process, best of five):

| Import | Cost |
|---|---|
| `airflow.sdk` — unavoidable in a DAG file | ~935 ms |
| `polars` | ~77 ms |
| `deltalake` | ~26 ms |

Top-level imports — polars, deltalake and the Google clients together — add ~0.2 s to a
parse Airflow already makes ~1 s long. That buys a
module whose dependencies are readable in one place and a linter that can check them. If
a future dependency costs seconds (a large ML framework), the trade-off is re-measured.

What stays banned at module level is **work**: network or disk I/O, `Variable.get`,
database lookups, computation. Those run on every parse, every ~30 seconds, forever.

**Task callables are module-level functions**, decorated with `@task`; the `@dag`
function only wires them (`ingest_month.expand(target=plan_months())`). The DAG reads as
a list of steps followed by a ten-line dependency graph, instead of a function nesting
everything.

**Docstrings carry the reasoning; comments are rare.** Every function has a Google
docstring with typed `Args` and `Returns`, and the *why* of a non-obvious rule goes in
its first paragraph. An inline comment is reserved for a reason that cannot live in a
docstring.

## Configuration: constants, config and secrets

Three kinds of value, three homes. The test is two questions: *does it change between
environments?* and *does it grant access to anything?*

| Kind | Changes per environment? | Grants access? | Home | Examples |
|---|---|---|---|---|
| **Constant** | no | no | code, reviewed with the logic that depends on it | `SOURCE_URL` of a public dataset, file layouts, window sizes |
| **Config** | yes | no | environment variables (compose `.env` locally, deployment config in the cloud) | `DATA_PLATFORM_MODE`, `GCP_PROJECT_ID`, bucket names |
| **Secret** | yes | **yes** | never in code or git: `.env` / `secrets/` locally, Secret Manager in the cloud | service-account key, webhook URL (embeds a token), API tokens, DB passwords |

A public source URL is a constant, not a secret: anyone can download from it, and it is
coupled to the parser — a new URL almost always means a new layout, which must go
through code review and tests together. A bucket name or project id is config, not a
secret: knowing it grants nothing without credentials.

**One switch for storage and secrets.** `DATA_PLATFORM_MODE` (`local` | `cloud`) is read
by exactly one module, [`include/runtime.py`](../include/runtime.py). DAGs ask it for
locations (`runtime.lakehouse()` → local directories or the two GCS buckets) and for
secrets (`runtime.get_secret("google_chat_webhook_url")` → `.env` locally, the Secret
Manager secret `google-chat-webhook-url` in cloud mode). No DAG branches on the mode, so
the task code is identical in both.

Airflow's own secrets backend (`CloudSecretManagerBackend`) was the alternative: code asks
Airflow for a Variable and the backend fetches it from Secret Manager. It was not chosen
because the runtime here is a local Airflow writing to the cloud: the backend is Airflow
configuration, a second switch next to the storage one, and it would route every Variable
lookup through GCP even for values that are not secrets. When Airflow itself moves to GCP
(Composer, GKE), the backend becomes the natural fit — and `get_secret` is the one
function that would change.

**Credentials** for GCP come from Application Default Credentials: locally a file in
`secrets/` (see its README); in the cloud, workloads run as an attached service account
(Workload Identity) and no key exists at all.

## The layers of the lakehouse — what each one promises

| Layer | Promise | Written as |
|---|---|---|
| **landing** | The bytes exactly as downloaded, plus their checksum and source ETag. The audit trail: any later layer can be rebuilt from here without asking the source again. | Raw files, one folder per source period |
| **bronze** | The source's rows, typed, one row per source row, nothing interpreted. Layout is asserted, not assumed. | Delta, partitioned by source period, partition overwrite |
| **silver** | Deduplicated, conformed keys (a CNPJ is formatted one way everywhere), history made explicit (SCD2 where the source overwrites). | Delta, `MERGE` on the business key |
| **gold** | Star schemas and marts with a documented grain, each answering a stated business question. | Delta, published to BigQuery |

A layer never reaches past its neighbour: silver reads bronze, never landing. That keeps
every rebuild a chain of steps that were each tested on their own.

## Why Python-first, not SQL-first

**Context.** Two families of data architecture are common in production. *Code-first on a
lake*: DataFrame jobs (Spark, Polars) read and write tables in object storage and an
orchestrator sequences them — the Databricks-style lakehouse. *SQL-first in a warehouse*:
raw data is loaded into BigQuery/Snowflake and transformed there in SQL, usually with dbt.
Most real platforms mix both; the question is which one carries the modeling.

**Decision.** Code-first. Every transformation from bronze to gold is a pure Polars
function in the DAG's `core/domain.py`. SQL appears at the serving edge — ad-hoc queries
in DuckDB locally and BigQuery in the cloud — where it is the right tool.

**Why.**

1. **Transformations are unit-testable.** A pure function with a ten-row fixture and
   pytest proves a dedup rule, an SCD2 diff or a layout parser in milliseconds. Testing
   the same logic in SQL needs a warehouse, or a mocking layer that is its own project.
2. **The hard logic in these sources is code-shaped.** Twenty years of CVM files change
   layout across regulations; republished months must be detected by ETag and hash; SCD2
   is a diff between two snapshots. These are branches, loops and assertions — natural in
   Python, contorted in SQL.
3. **One language, one harness.** Ingestion (HTTP, zips, encodings) is Python no matter
   what. Keeping transforms in Python means the same test style, the same linting, the
   same review from the first byte to the gold table.
4. **The scale path stays in the same paradigm.** Should a job outgrow one node, Polars
   → PySpark is a change of DataFrame API, not a rewrite into another language.

**What we give up, and how each item is paid for.** dbt bundles several things a
SQL-first stack gets for free. None of them is optional in production, so each one needs
an explicit replacement here:

| dbt gives | Paid for here by |
|---|---|
| Model dependency graph | Airflow task dependencies inside a DAG; **Assets** between DAGs |
| Data tests (`unique`, `not_null`, relationships) | Quality-gate tasks that run before the Asset is emitted and fail the run |
| Incremental models, snapshots (SCD2) | Delta `MERGE` and partition overwrite — decision 5, and the idempotency tests |
| Docs and lineage | Per-DAG README (grain, key, consumers); Asset graph in the Airflow UI; OpenLineage as a later option |

**When to revisit.** If gold became owned by a team of SQL analysts, dbt on top of the
silver layer would be the right call — the silver Delta tables are readable by BigQuery
and by dbt-duckdb, so that change would add a layer, not replace one.

## Idempotency by construction

Airflow re-runs work all the time: automatic retries, a manual *clear*, a backfill over a
range that partly succeeded before. The platform therefore allows exactly two write
shapes, chosen per table in its README:

| Shape | Used for | Why a re-run is harmless |
|---|---|---|
| **Partition overwrite with a predicate** | bronze; snapshots published per period | Replaces exactly the partition the run owns, e.g. `reference_month = '2026-09-01'`; other partitions are untouched |
| **`MERGE` on the business key** | silver entities; SCD2 dimensions | A key that already exists with the same values produces no change; new or changed keys update in place |

A blind append is not on the list: a retried append doubles the rows and nothing fails.
Every DAG proves its write shape with a test that runs the write twice against a
temporary Delta table and asserts the second run changed nothing.

## Where compute runs

Locally, a task runs its Polars work inside the LocalExecutor worker — the laptop *is* the
node, and there is nothing to gain from indirection. In production the task code stays
the same but moves off the Airflow worker (KubernetesPodOperator or Cloud Run Jobs), so a
memory-hungry transform cannot starve the scheduler or its neighbours.

Spark enters only with a measurement behind it. The planned experiment is the full CVM
backfill (~32 GB uncompressed): Polars, DuckDB, Spark in local mode and Spark on Dataproc
Serverless, compared on time and cost. The pure-function boundary is what keeps that
swap cheap — only the engine inside `domain.py` changes, never the DAG or the adapters.

## Why not the flat canonical layout (all logic in include/)

The canonical Astronomer layout puts DAG files flat in `dags/` and everything else in
`include/`. It works, but it optimises for the scheduler's convenience, not the
maintainer's: changing one pipeline means touching two or three distant folders, and
`include/` accumulates modules whose owner is unclear.

Folder-per-DAG optimises for the change: one pipeline = one folder = one review = one
revert. Deleting a DAG is deleting a folder. The costs, and how they are paid:

| Cost | Payment |
|---|---|
| The processor would parse non-DAG modules | `dags/.airflowignore` (glob) excludes `core/`, `adapters.py` and READMEs — and the safe-mode heuristic already skips files without the words "airflow"/"dag" |
| Two DAG folders both named `core` collide on import | Imports are **fully qualified from the dags root** (`cvm.<dag_name>.core`), which Airflow supports because it puts `dags/` on `sys.path`; each folder is a real package (`__init__.py`) |
| Unfamiliar to some Airflow engineers | The top level stays canonical; the deviation is inside `dags/` and documented here |

Airflow 3's DAG-bundle direction — the DAG versioned together with the code it
orchestrates — endorses the same grouping.

## Where shared code goes

`include/` — with a hard admission rule (see its README): system-talking, business-blind.
Anything that knows a table name stays in the DAG folder, duplicated if a second DAG
needs something similar. Duplication of business logic is visible and local; a shared
"utils" drifting under six consumers is neither.

## The two-repo boundary

This repository is the pipeline half of a two-repo portfolio; the agent half
([agent-services](https://github.com/VitorFaccin/agent-services)) consumes the gold tables
this platform publishes. The boundary is physical — full histories of tens of GB do not
fit a git repository — and is specified in [DATA_CONTRACT.md](DATA_CONTRACT.md).
