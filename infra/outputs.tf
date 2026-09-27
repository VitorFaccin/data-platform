# What the pipeline needs to know about the infrastructure — these values feed the
# .env consumed by docker-compose (GCS_LANDING_BUCKET, GCS_LAKEHOUSE_BUCKET, ...).

output "landing_bucket" {
  description = "GCS bucket for raw downloads (the audit trail)."
  value       = google_storage_bucket.landing.name
}

output "lakehouse_bucket" {
  description = "GCS bucket holding the bronze/silver/gold Delta tables."
  value       = google_storage_bucket.lakehouse.name
}

output "airflow_service_account" {
  description = "Identity the DAGs authenticate as."
  value       = google_service_account.airflow.email
}

output "bq_gold_dataset" {
  description = "BigQuery dataset the gold tables are published to."
  value       = google_bigquery_dataset.gold.dataset_id
}
