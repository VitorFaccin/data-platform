# Two buckets, because the two kinds of object age differently.

# Landing zone: raw files exactly as downloaded, before any parsing — the audit trail
# every layer can be rebuilt from without asking the source again.
resource "google_storage_bucket" "landing" {
  name     = "${var.project_id}-landing"
  location = var.region

  # Nobody gets object-level ACLs; access is IAM on the bucket. Modern default.
  uniform_bucket_level_access = true

  # Cost control as code: raw files older than 90 days are read only for a rebuild,
  # so they move to the coldest class instead of accumulating at standard price.
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
  }

  # Demo environment: allow `terraform destroy` to remove a non-empty bucket.
  # In production this would be false — destroy should NEVER silently delete data.
  force_destroy = true
}

# Lakehouse: the bronze/silver/gold Delta tables. Deliberately NO age-based lifecycle:
# a Delta table's live parquet files can be years old and are still read on every
# query, and its _delta_log must stay intact — ARCHIVE here would bill retrieval on
# every read and could break the log. Old table versions are removed by Delta's own
# VACUUM, which knows which files are still referenced; an age rule does not.
resource "google_storage_bucket" "lakehouse" {
  name     = "${var.project_id}-lakehouse"
  location = var.region

  uniform_bucket_level_access = true

  # Same demo-environment exception as the landing bucket.
  force_destroy = true
}
