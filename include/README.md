# include/

Shared helpers that more than one DAG uses — and the admission rule that keeps this
folder from becoming a junk drawer:

> Code enters `include/` only if it **knows how to talk to a system** (HTTP session with
> retries, GCS client, BigQuery helpers) and knows **nothing about any dataset, table or
> business rule**. The test: if a table name or a dataset-specific rule appears in the
> code, it belongs inside that DAG's folder, duplicated if necessary.

Empty right now on purpose: until two DAGs need the same system code there is nothing
shared, and an abstraction extracted before its second consumer exists is usually the
wrong abstraction.
