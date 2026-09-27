# include/

Shared platform code — and the admission rule that keeps this folder from becoming a
junk drawer:

> Code enters `include/` only if it **knows how to talk to a system** (storage, secrets,
> HTTP) and knows **nothing about any dataset, table or business rule**. The test: if a
> table name or a dataset-specific rule appears in the code, it belongs inside that DAG's
> folder, duplicated if necessary.

| Module | What | Why it is shared |
|---|---|---|
| `delta.py` | The two allowed write shapes (partition overwrite with predicate, `MERGE` on a key), partition reads and listing | Every layer writes Delta the same way; extracted when silver became the second consumer |
| `runtime.py` | The `DATA_PLATFORM_MODE` switch: storage locations (local volume or GCS buckets), write-once landing, secrets (`.env` or Secret Manager) | Every DAG and the alerting plugin must behave the same way in each mode; one module deciding means no DAG branches on the mode itself |

Importable as `from include import runtime`: compose puts `/opt/airflow` on
`PYTHONPATH`, and the tests put the repository root on `sys.path`.
