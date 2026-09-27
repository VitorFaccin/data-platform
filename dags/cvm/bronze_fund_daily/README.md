# `bronze_cvm_fund_daily` — CVM daily fund reports → bronze

CVM publishes this dataset as **Informe Diário** (`FI/DOC/INF_DIARIO`), the name to
search for at the source.

Every investment fund in Brazil reports, per day: total portfolio value, net assets, quota
value, subscriptions, redemptions and number of shareholders. CVM publishes them as one
zip per month. This DAG keeps a faithful, typed copy of those files in bronze — and only
re-ingests a month when its **content** actually changed.

**Scope of this DAG today:** monthly files (2021-01 onwards), local sink, bronze only.
Silver, the downstream Asset, the gcp sink and the 2000–2020 yearly files (`HIST/`) are
later pieces.

## Flow

```
plan_months ──► ingest_month.expand(target)          one mapped instance per month
                 ├─ HEAD: ETag equals the manifest's?  → skip (nothing downloaded)
                 ├─ GET → sha256 equals the manifest's? → refresh manifest, skip
                 ├─ keep the zip in landing (immutable, named by sha256)
                 ├─ parse: layout v1/v2, every value format-checked, then typed
                 ├─ bronze: overwrite ONLY that month's partition
                 └─ manifest: MERGE on (reference_month, sha256)
```

## Schedule — and the weekend delay, on purpose

Daily at **05:00 America/Sao_Paulo**. What CVM does, measured on the source:

| CVM rewrites | When (BRT) | Checked by |
|---|---|---|
| current + previous month | nightly, ~00:50 | every run (2-month window) |
| the whole last-12-months window | Saturdays, ~06:00–06:20 | **Sunday** 05:00 run (13-month window) |

Weekday runs stay small and predictable: two HEAD requests, usually one download. A heavy
republication lands in the Sunday run, which is expected to be longer, instead of delaying
a weekday run whose fresh data someone is waiting for.

**Accepted delay:** Saturday's rewrite is picked up on Sunday (~23 h later), and a
correction CVM makes to an older month on a weekday waits for the next Sunday (up to a
week). Fresh data — the current month — is never delayed. `full_window=true` forces the
13-month check on any day.

## Params

| Param | Default | Effect |
|---|---|---|
| `months` | empty | Explicit `YYYY-MM` list (a backfill, a re-check). Must be ≥ 2021-01 and not in the future. |
| `full_window` | `false` | Check the 13-month window regardless of the weekday. |
| `force` | `false` | Re-ingest even when ETag and content are unchanged. |

Trigger with these in the UI (*Trigger DAG* → the form) or the CLI:

| Situation | Trigger with | What happens |
|---|---|---|
| CVM says it reprocessed Sep–Oct 2025 | `months: ["2025-09", "2025-10"]` | Changed files are detected (ETag, then sha256) and re-ingested; unchanged ones skip |
| Our parser was fixed and must re-run on old months | `months: [...]`, `force: true` | Re-ingested regardless; landing reuses the file already kept for that sha256 |
| Backfill every monthly file | `months: ["2021-01", …, current]` | Months already in bronze with the same ETag skip in seconds |

```bash
airflow dags trigger bronze_cvm_fund_daily --conf '{"months": ["2025-09", "2025-10"]}'
```

## Idempotency

| Table | Write shape | Key |
|---|---|---|
| bronze `bronze/cvm/fund_daily` | partition overwrite, predicate `reference_month = <month>` | one partition per month |
| manifest `landing/cvm/fund_daily/_manifest` | `MERGE` | `(reference_month, sha256)` |
| landing `landing/cvm/fund_daily/reference_month=YYYY-MM/<sha256>.zip` | write-once | the content fingerprint |

Re-running any month, any number of times, leaves bronze with exactly that month's rows
once. Bronze rows carry no ingestion timestamp on purpose, so a re-run produces the same
bytes; *when* lives in the manifest.

## Failure isolation

- One mapped instance per month: a malformed month fails alone; the others land.
- Instances run **one at a time** (`max_active_tis_per_dagrun=1`) because all of them
  commit to the same two Delta tables; parallel commits would conflict on the log.
- Order inside an instance is chosen for crash safety: landing → bronze → manifest. A
  crash after bronze and before the manifest makes the next run redo the month; it can
  never leave the manifest claiming a version bronze does not hold.
- HTTP work retries 3 times with exponential backoff; after the last one, Google Chat.

## Resources

Measured on the largest monthly file so far (2026-07: 12 MB zip, 588k rows), one
`ingest_month` instance in the platform image:

| Step | Peak memory |
|---|---|
| download | 63 MB |
| parse (108 MB DataFrame) | 356 MB |
| Delta write | **671 MB** |

A whole month fits in memory with room to spare, so the task processes it in one piece —
chunking would add complexity for no gain. Instances run one at a time, so this is also
the DAG's peak. Were the work to move to a Kubernetes pod, a 1 GiB request and a 2 GiB
limit cover it with ~3× headroom. The yearly `HIST/` files (up to ~10× the rows) are the
point where streaming becomes necessary; that piece measures again before choosing.

## Bronze contract

`core/schema.py` is the source of truth. One table for both layouts:

| Column | Type | Note |
|---|---|---|
| `tp_fundo_classe`, `cnpj_fundo_classe` | string | v1 files publish them as `TP_FUNDO`, `CNPJ_FUNDO` — renamed, values untouched |
| `id_subclasse` | string, nullable | null for every v1 row and for classes without subclasses |
| `dt_comptc` | date | always inside `reference_month` (asserted) |
| `vl_total`, `vl_patrim_liq`, `captc_dia`, `resg_dia` | decimal(38,2) | money, exact |
| `vl_quota` | decimal(38,12) | quota, 12 places as published |
| `nr_cotst` | int64 | |
| `layout_version` | int8 | 1 = until 2023-11, 2 = from 2023-12 (CVM Resolution 175) |
| `reference_month` | date | partition |
| `source_sha256` | string | the landing file the row came from |

Intended grain: one row per `(cnpj_fundo_classe, id_subclasse, dt_comptc)`. **Bronze does
not enforce it** — the source breaks it (see traps); the manifest's `duplicate_key_rows`
counts the offenders per version, and silver enforces the grain.

## Traps (all measured on the real files)

- **CVM's ETag is modification time + size** (`"6ab7411c-8e4cbe"` = epoch seconds + byte
  count, nginx's default). Every rewrite changes it, even with identical bytes — hence the
  sha256 second check. On a Saturday, that saves rewriting ~12 unchanged partitions.
- **The layout changed in 2023-12** (CVM 175): 9 → 10 columns, `CNPJ_FUNDO` →
  `CNPJ_FUNDO_CLASSE`, new `ID_SUBCLASSE`. Before it the CNPJ identifies a *fund*, after
  it a fund *class*; bronze aligns the labels, silver owns the semantic reconciliation.
- **Polars rounds silently when casting to Decimal**: `"1.239"` → `1.24`. Every numeric
  value is regex-checked against its published precision *before* the cast.
- **The source publishes duplicate rows — routinely.** In the first 15 months ingested,
  7 had them: 2024-01 (44 rows), 2025-09 (56), 2025-10 (4), 2025-11 (24), 2025-12 (72),
  2026-01 (14), 2026-07 (6). Two kinds: byte-identical repeated lines, and one fund
  published twice under two types (`FI` *and* `CLASSES - FIF`, same values) mid CVM 175
  transition. Rejecting them would fail those months forever; bronze keeps them, the
  manifest counts them, silver resolves them.
- **Negative net assets and quotas exist** (hundreds of rows per month). Bronze keeps
  them — it records what was published; whether they are valid is silver's call.
- **Files have a header, `;` separators and CRLF line ends**, and their content is ASCII.
  The parser decodes strictly as UTF-8; a non-UTF-8 byte fails the month instead of
  guessing a codec.
- **Docker Desktop's Windows bind mounts break delta-rs** on files over ~10 MB
  (multipart write → "Upload aborted"). The local lakehouse is a named volume for that
  reason (docker-compose.yml).
- **A 404 is only normal for the current month** (early on the 1st, before CVM publishes
  it) — assumed, to be confirmed on the first 1st of a month in production. On any past
  month a 404 fails the task.

## Consumers

None yet. Next piece: the `bronze/cvm/fund_daily` Asset and the `silver_cvm_fund_daily`
DAG it schedules (`MERGE` on the grain) — one DAG per layer, so this DAG never writes
silver.
