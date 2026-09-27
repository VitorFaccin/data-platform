# plugins/

Airflow extensions shared by every DAG — notifiers, operators, hooks. Airflow puts this
folder on `sys.path`, so a module here is importable from any DAG by its package name.

A plugin is *earned* by a concrete need appearing in more than one DAG — it is not
scaffolding to fill in advance. What is here, and why it earned its place:

| Package | What | Why it is shared |
|---|---|---|
| `alerting/` | `GoogleChatNotifier` — posts a task failure to a Google Chat space, one thread per DAG run | Every DAG must alert on failure; the integrity gate enforces it on every task |

Same admission rule as `include/`: nothing here may know a table name or a business rule.
