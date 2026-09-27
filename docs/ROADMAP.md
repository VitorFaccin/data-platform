# Roadmap — the next pieces, and the design already agreed for each

Work lands in small pieces: the structure of a piece is agreed, then only that piece is
built, reviewed and merged. This file records what is next and the decisions already
taken for it, so the reasoning does not live only in a conversation.

**Done:** platform foundation · `bronze_cvm_fund_daily` · local/cloud mode switch.

## 1. Bronze announces what changed — the first Asset

`bronze_cvm_fund_daily` gains a final `publish` task with
`outlets=[Asset("bronze/cvm/fund_daily")]`.

- **An Asset is a signal, not data**: a record in Airflow's metadata database. When the
  task that declares it succeeds, Airflow stores an *asset event*; any DAG scheduled on
  that Asset gets a run. The UI draws producers and consumers as a lineage graph.
- **The event carries the months that changed** (`extra={"months": ["2026-09"]}`), so
  silver reprocesses those months only.
- **No change, no event**: `publish` uses the trigger rule "none failed, at least one
  success". If every month was skipped (nothing changed at CVM), `publish` is skipped and
  silver does not run for nothing.

## 2. `silver_cvm_fund_daily` — one clean row per fund class per day

Its own DAG (one DAG per layer), scheduled by the bronze Asset.

```
dags/cvm/silver_fund_daily/
├── silver_fund_daily.py   read months from the triggering events → transform → gate → write → publish
├── adapters.py            read bronze partitions, write silver, write rejects
├── core/schema.py         silver schema (English, conformed), grain, rejects schema
├── core/domain.py         pure rules: conform, dedupe, split rejects, reconcile counts
└── README.md
tests/cvm/silver_fund_daily/test_silver_fund_daily.py
```

What happens to each changed month:

| Step | Rule |
|---|---|
| **Conform** | CNPJ to 14 digits (the registry's spelling); columns renamed to the platform's English vocabulary: `fund_class_cnpj`, `subclass_id`, `report_date`, `total_assets`, `quota_value`, `net_assets`, `subscriptions`, `redemptions`, `shareholders`; derived `net_flow = subscriptions − redemptions` |
| **Dedupe** | byte-identical rows collapse to one; rows sharing the grain with identical figures but different types (the CVM 175 transition) keep the post-175 type |
| **Reject** | rows sharing the grain with *different* figures cannot be resolved by a rule: they go to `silver/cvm/fund_daily_rejects` with the reason, never silently dropped |
| **Gate** (fails the run) | no duplicate grain; no null key; `silver + collapsed duplicates + rejects = bronze` for the month |
| **Write** | partition overwrite of the month — bronze changes a whole month at a time, so silver does too. `MERGE` is for entity tables (the registry), not for a monthly-replaced fact |
| **Publish** | Asset `silver/cvm/fund_daily` with the months written |

Bronze keeps the source's names and defects; silver is where the platform's vocabulary
and quality rules start. Returns are **not** computed here: a daily return needs the
previous business day, which may sit in another month — that belongs to gold's fact.

## 3. `include/delta.py` — extracted when silver becomes the second consumer

Partition overwrite with predicate, generic `MERGE`, "does the table exist", OPTIMIZE and
VACUUM move from the bronze adapters to `include/delta.py` in the silver pull request,
and both DAGs use them from there. Shared code enters `include/` when its second
consumer exists, not before; it is named after the system it talks to (no `utils.py`).
`include/http.py` follows the same rule when a second file source arrives.

## 4. `maintenance_lakehouse` — OPTIMIZE and VACUUM, weekly

- **OPTIMIZE** compacts many small parquet files into few large ones (every write and
  `MERGE` adds files; reads slow down as they pile up). Content does not change.
- **VACUUM** deletes files the current table version no longer references and that are
  older than the retention (7 days). Time travel beyond that window is given up; storage
  stops growing with every republication.

```
maintenance_lakehouse   weekly, Sunday after the ingestion window
  discover_tables       every directory with a _delta_log under the table root
  maintain.expand(t)    OPTIMIZE → VACUUM (retention 168 h), one mapped instance per table
```

A separate DAG, not a step at the end of each ingestion: daily runs stay fast, the
retention policy lives in one place, and a maintenance failure never marks an ingestion as
broken. Discovery means a new table is maintained without anyone registering it. It must
not overlap writes to the same table — hence the Sunday slot after the full window.

## 5. Fund registry — `bronze_cvm_fund_registry` → `silver_cvm_fund_registry` (SCD2)

The three registry files (fund, class, subclass — see [CVM_DATA.md](CVM_DATA.md)).
The source overwrites the snapshot daily; silver builds the history as SCD2 (`valid_from`,
`valid_to`, `is_current`) with `MERGE`, so a report from January is attributed to the
manager the fund had in January.

## 6. Gold — tables that answer questions

Gold is designed from questions, not from tables. Candidates, all answerable with the two
CVM datasets (the benchmark comparison also needs the Central Bank's CDI):

| Question | Needs |
|---|---|
| Where is the money going? Net flows per month by classification (Renda Fixa, Ações, Multimercado) and by manager | daily reports + registry |
| Which classes beat their own declared benchmark — over the month, over 12 months? | daily reports (quota) + registry (benchmark) + BCB CDI |
| How concentrated is the industry? Managers' share of net assets, and how it moves | daily reports + registry, **excluding funds of funds** |
| Is retail participation growing? Shareholders by classification and audience | daily reports + registry |

Model sketch: `fact_fund_daily` (grain: class × day — net assets, quota, flows,
shareholders, daily return), `dim_fund_class` (SCD2: name, classification, benchmark,
audience, status, manager, administrator), `dim_date`; marts per question published to
BigQuery. The double-counting rule for funds of funds is decided and documented there.

## Later

- `bcb` domain: CDI and Selic series (REST API) — needed by the benchmark question.
- The 2000–2020 yearly files (`HIST/`) and the Spark benchmark on that backfill.
- `cnpj` domain: the company registry, a conformed `dim_company` shared with `cvm`.
- First run in cloud mode: Delta on real GCS (not yet verified — no billing project).
