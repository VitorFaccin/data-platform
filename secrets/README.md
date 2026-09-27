# secrets/

Local-only credentials, mounted read-only into the Airflow containers at
`/opt/airflow/secrets`. **Everything in this folder except this README is gitignored**,
and both pre-commit (`detect-private-key`) and CI refuse a committed private key.

## GCP service account (for `DATA_PLATFORM_SINK=gcp`)

1. Create a JSON key for the service account the DAGs run as
   (`airflow-data-platform`, declared in [`infra/iam.tf`](../infra/iam.tf)).
2. Save it here as `gcp-sa.json` (or set `GCP_SA_KEY_FILE` in `.env` to its file name).
3. Set `DATA_PLATFORM_SINK=gcp` and the bucket/project variables in `.env`.

Compose points `GOOGLE_APPLICATION_CREDENTIALS` at the mounted file, which every Google
client library picks up with no code change.

A key file is the pragmatic choice for a laptop, not the ideal one: it is a long-lived
credential that works from anywhere it leaks to. Keep it here only while testing and
delete the key in the GCP console afterwards. Deployed on GCP, workloads use the
attached service account (or Workload Identity) and no key exists at all.
