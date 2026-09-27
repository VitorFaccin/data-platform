# Secret CONTAINERS only — never their values. A value declared here would sit in plain
# text in the Terraform state; instead it is added once, by hand, after apply:
#   printf '%s' "$WEBHOOK_URL" | gcloud secrets versions add google-chat-webhook-url --data-file=-
# Same split as tables: Terraform owns the container, something else owns the content.
#
# The secret id follows include/runtime.py: logical name in snake_case, hyphens in GCP.

resource "google_secret_manager_secret" "google_chat_webhook_url" {
  depends_on = [google_project_service.required]

  secret_id = "google-chat-webhook-url"

  replication {
    auto {}
  }
}

# Read access for the pipeline's identity on THIS secret only, not the whole project.
resource "google_secret_manager_secret_iam_member" "airflow_webhook_reader" {
  secret_id = google_secret_manager_secret.google_chat_webhook_url.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.airflow.email}"
}
