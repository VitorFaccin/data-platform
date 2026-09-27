# `silver_cvm_fund_daily` — CVM daily fund reports, conformed

One clean row per fund class (or subclass) per day, in the platform's vocabulary, built
from [`bronze_cvm_fund_daily`](../bronze_fund_daily/README.md). Bronze keeps the source's
names and defects; this is where the platform's rules start.

## Trigger

No cron. The DAG is scheduled on the Asset `bronze/cvm/fund_daily`
([`cvm/assets.py`](../assets.py)): it runs when bronze announces that months changed, and
the Asset event says which ones (`extra={"months": [...]}`). When several bronze runs
land before silver starts, their months are united. When bronze changed nothing, it emits
no event and silver does not run.

```
plan_months ──► transform_month.expand(month) ──► publish
  months from       read bronze month → conform →      Asset silver/cvm/fund_daily
  the events        resolve duplicates → reconcile →   with the months written
                    replace the month in silver + discards
```

| Param | Default | Effect |
|---|---|---|
| `months` | empty | Manual run: rebuild these months (`YYYY-MM`). Empty = every month in bronze. |

## What each month goes through

| Step | Rule |
|---|---|
| **Conform** | CNPJ to 14 digits (the registry's spelling); English names; `net_flow = subscriptions − redemptions` |
| **Resolve duplicates** | same figures → one row, post-CVM 175 type first; different figures with one class report and the rest legacy `FI` → the class report; anything else **fails the run** |
| **Reconcile** (before any write) | `silver + discarded = bronze`; no duplicate grain; no null key; every row in the month |
| **Write** | replace the month's partition in silver and in the discard log — always both, so a month that lost its discards gets them cleared |

The duplicate rules were measured on the real files before being written: in the first 15
months, 110 keys were duplicated; 89 had identical figures and 21 were a fund reporting in
both the legacy (`FI`) and the post-175 (`CLASSES - FIF`) format with slightly different
figures, e.g. the same day with 12,290 vs 12,306 shareholders. After CVM 175 the class is
the reporting entity, so its report wins. No other kind of conflict appeared; if one does,
the run fails with the key and the types in the message and a person decides the rule.

## Contract

| Column | Type | Note |
|---|---|---|
| `fund_class_cnpj` | string | 14 digits |
| `subclass_id` | string, nullable | |
| `report_date` | date | |
| `fund_type` | string | the report's type (`CLASSES - FIF`, `FI`, …) |
| `total_assets`, `net_assets`, `subscriptions`, `redemptions`, `net_flow` | decimal(38,2) | |
| `quota_value` | decimal(38,12) | |
| `shareholders` | int64 | |
| `reference_month` | date | partition |
| `source_sha256` | string | the landing file the row came from (lineage to bronze) |

Grain, enforced: one row per `(fund_class_cnpj, subclass_id, report_date)`.
The discard log `silver/cvm/fund_daily_discarded` has the same columns plus
`discard_reason` (`exact_duplicate` or `superseded_by_class_report`).

## Idempotency

Both tables are written by partition overwrite of the month — bronze changes a whole month
at a time, so silver does too. Re-running a month rebuilds it from bronze and yields the
same rows. `MERGE` is for entity tables such as the fund registry, not for a fact replaced
month by month.

## Failure isolation

- One mapped instance per month; a month that fails reconciliation or hits an
  unresolvable conflict fails alone and writes nothing.
- Instances run one at a time (they commit to the same two tables).
- After the last retry, Google Chat.

## Not here, on purpose

- **Returns.** A daily return needs the previous business day, which may sit in the
  previous month — gold's fact table computes it.
- **Registry attributes** (name, classification, manager). They come from the fund
  registry DAGs and meet this table in gold.

## Consumers

Gold (planned — [docs/ROADMAP.md](../../../docs/ROADMAP.md)), through the Asset
`silver/cvm/fund_daily`.
