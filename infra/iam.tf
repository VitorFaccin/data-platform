# Least privilege, named and reviewable.
#
# The Airflow tasks authenticate as THIS service account — never as a personal user,
# never as the project-wide default SA with Editor.

resource "google_service_account" "airflow" {
  account_id   = "airflow-data-platform"
  display_name = "Airflow — data-platform pipelines"
  description  = "Runs the batch DAGs. Scoped to the landing + lakehouse buckets and the gold dataset only."
}

# Read and write raw files — on this bucket, not storage.admin on the project.
resource "google_storage_bucket_iam_member" "airflow_landing" {
  bucket = google_storage_bucket.landing.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.airflow.email}"
}

# Read, write and delete Delta files. objectAdmin (not objectCreator) because Delta
# commits and VACUUM remove superseded files.
resource "google_storage_bucket_iam_member" "airflow_lakehouse" {
  bucket = google_storage_bucket.lakehouse.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.airflow.email}"
}

# Publish gold tables — dataset-level dataEditor, not project-level.
resource "google_bigquery_dataset_iam_member" "airflow_gold" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.airflow.email}"
}

# Run load and query jobs (project-level by necessity — jobs are a project resource).
resource "google_project_iam_member" "airflow_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}
