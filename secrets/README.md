# secrets/

Local-only credentials, mounted read-only into the Airflow containers at
`/opt/airflow/secrets`. **Everything in this folder except this README is gitignored**,
and both pre-commit (`detect-private-key`) and CI refuse a committed private key.

Only cloud mode (`DATA_PLATFORM_MODE=cloud`) needs anything here. Compose points
`GOOGLE_APPLICATION_CREDENTIALS` at `secrets/$GCP_CREDENTIALS_FILE`, which every Google
client library — and delta-rs — picks up with no code change.

## Recommended: your user's credentials, no key file

Application Default Credentials for your Google account, generated into this folder so
they never mix with any other `gcloud` login on the machine (a work account, say). In
PowerShell, from the repository root:

```powershell
$env:CLOUDSDK_CONFIG = "$PWD\secrets\gcloud"     # this terminal only; your global gcloud is untouched
gcloud auth application-default login             # browser opens: pick the account that owns the project
gcloud auth application-default set-quota-project <project-id>
```

The file lands at `secrets/gcloud/application_default_credentials.json` — the default of
`GCP_CREDENTIALS_FILE` in `.env.example`. It holds a refresh token: treat it as a secret,
and revoke it when done (`gcloud auth application-default revoke`, same terminal, same
`CLOUDSDK_CONFIG`).

## Alternative: a service-account key

Create a JSON key for the pipeline's service account (`airflow-data-platform`,
[`infra/iam.tf`](../infra/iam.tf)), save it here and set `GCP_CREDENTIALS_FILE` to its file
name. A key is a long-lived credential that works from anywhere it leaks to — keep it only
while testing and delete it in the console afterwards.

## Where credentials live outside a laptop

| Who authenticates | How | Credential stored |
|---|---|---|
| The pipeline running on GCP (Composer, GKE, Cloud Run) | runs *as* the service account (attached SA / Workload Identity) | none |
| CI/CD applying Terraform or pushing images | Workload Identity Federation (OIDC) | none — only the provider id |
| A developer's machine | the options above | this folder, gitignored |
