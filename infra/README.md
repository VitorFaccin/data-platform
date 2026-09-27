# infra/ — the GCP footprint, as code

**Status: written and CI-validated, not yet applied.** This folder declares every cloud
resource the pipeline needs — reading it answers "what must exist in GCP?" without
anything running. Applying it is a manual, cheap, reversible act.

Study guide (in Portuguese), concept by concept, lives in the Notion project
*"[Portfólio] data-platform — infra com Terraform"*.

| File | Declares |
|---|---|
| `providers.tf` | Google provider + (commented) GCS state backend |
| `apis.tf` | the five `google_project_service` the footprint calls — first apply is self-sufficient, `disable_on_destroy = false` |
| `variables.tf` | project, region, billing account, budget |
| `storage.tf` | landing bucket (raw files, lifecycle to ARCHIVE after 90 days) + lakehouse bucket (Delta tables, no age lifecycle — VACUUM owns cleanup) |
| `bigquery.tf` | the `gold` dataset — the serving edge; **container only, tables belong to the pipeline** |
| `iam.tf` | one service account, bucket- and dataset-scoped roles — least privilege, named |
| `budget.tf` | billing alert at 50/90/100% of R$20 |
| `outputs.tf` | the values `.env` needs (`GCS_LANDING_BUCKET`, `GCS_LAKEHOUSE_BUCKET`, SA e-mail, gold dataset) |

## The workflow (when applying for real)

```bash
gcloud auth application-default login
# The state bucket is the one resource made BY HAND (chicken-and-egg: Terraform
# cannot store its state in a bucket it has not created yet). Versioned, because
# the state is the one file you cannot regenerate.
gcloud storage buckets create gs://<project>-tf-state \
  --location=southamerica-east1 --uniform-bucket-level-access
gcloud storage buckets update gs://<project>-tf-state --versioning
cp terraform.tfvars.example terraform.tfvars   # fill in
terraform init        # after uncommenting backend "gcs" in providers.tf
terraform plan        # READ THIS — never apply what you have not read
terraform apply       # ~14 resources; if it races API propagation, apply again
# ... run the DAG with DATA_PLATFORM_SINK=gcp, take the evidence ...
terraform destroy     # tear down after the demo run — nothing here needs to stay up
```

CI runs `fmt -check` + `validate` offline on every push, so this folder cannot rot
between applies.
