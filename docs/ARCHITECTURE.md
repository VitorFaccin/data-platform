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
