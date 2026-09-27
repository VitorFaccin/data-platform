# The APIs this footprint calls, declared instead of hand-enabled. Without this the
# first apply on a fresh project fails with "API not enabled" (billingbudgets is never
# on by default) and the fix becomes a gcloud command nobody remembers.
#
# If the very first apply races API propagation (the API turned on seconds ago), a
# second `terraform apply` converges — idempotence is the point. The one case this
# cannot solve: a project so bare that serviceusage itself is off; that takes a single
# manual `gcloud services enable serviceusage.googleapis.com`.
resource "google_project_service" "required" {
  for_each = toset([
    "bigquery.googleapis.com",             # datasets (bigquery.tf) + the pipeline's jobs
    "storage.googleapis.com",              # landing bucket (storage.tf)
    "billingbudgets.googleapis.com",       # budget alert (budget.tf) — never on by default
    "iam.googleapis.com",                  # service account (iam.tf)
    "cloudresourcemanager.googleapis.com", # project-level IAM binding (iam.tf)
    "secretmanager.googleapis.com",        # the webhook secret (secrets.tf)
  ])

  service = each.value

  # `terraform destroy` removes OUR resources, not the project's ability to use an
  # API — disabling APIs on destroy can break unrelated tooling and buys nothing.
  disable_on_destroy = false
}
