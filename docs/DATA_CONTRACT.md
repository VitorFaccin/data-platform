# Data contract — what this platform produces for its consumers

**Status: draft.** The producer side (this repo) is under construction; the main consumer
is the agent layer in [agent-services](https://github.com/VitorFaccin/agent-services).
Each repo documents its own end, the way two services in a company would.

## How a table is published

| Aspect | Rule |
|---|---|
| **Location** | Delta table at `<layer>/<domain>/<table>/` — under the `warehouse` Docker volume locally, under the lakehouse bucket in GCP |
| **Serving** | Gold tables only, in BigQuery as `gold.<domain>__<table>` (double underscore: unambiguous split between domain and table) |
| **Signal** | One Airflow **Asset** per published table, emitted only after the producer's quality gates pass. Consumers schedule on it — never on a cron offset |
| **Schema and grain** | Declared in the producer's `core/schema.py`; the grain and business key are stated in the producer DAG's README |
| **Changes** | Adding a nullable column is compatible. Removing, renaming or retyping a column is a breaking change: new table version, announced here first |

## Produced today

Nothing yet — the platform foundation ships without domains. Each domain's pull request
adds its rows here.

## Planned

| Domain | Gold table (planned) | Grain | Intended consumer |
|---|---|---|---|
| `cvm` | `fact_fund_daily` | one fund class × one day | analysis; `warehouse_analyst` agent |
| `cvm` | `dim_fund` (SCD2) | one fund × one validity interval | same |
| `cnpj` | `dim_company` | one company (CNPJ root) | conformed across `cvm` and `cnpj`; `entity_resolver` agent |
| `bcb` | `series_daily` | one series × one day | return benchmarking (CDI/Selic) |

Grains are provisional until each producer's README pins them.
