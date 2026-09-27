# Terraform entrypoint: which providers we talk to and where the STATE lives.
#
# STATUS: written, not yet applied. This folder is validated in CI (fmt + validate,
# offline) so it cannot rot; applying it is a manual, documented act — see infra/README.md.

terraform {
  required_version = ">= 1.9"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # Remote state in GCS. The state file maps declared resources to real ones and
  # MUST NOT live in git (it can contain secrets, and two machines applying from
  # local state corrupt each other). The bucket is created ONCE by hand — the
  # chicken-and-egg is real and documented, not hidden:
  #   gcloud storage buckets create gs://<project>-tf-state \
  #     --location=southamerica-east1 --uniform-bucket-level-access
  #   gcloud storage buckets update gs://<project>-tf-state --versioning
  # Versioning because the state is the one file you cannot regenerate: a corrupted
  # or fat-fingered overwrite must be recoverable from a previous version.
  #
  # Commented out until first apply so `terraform validate` runs offline in CI.
  # backend "gcs" {
  #   bucket = "REPLACE-project-tf-state"
  #   prefix = "data-platform"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
