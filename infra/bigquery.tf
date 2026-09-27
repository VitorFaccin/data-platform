# The serving edge. Bronze and silver live as Delta tables in the lakehouse bucket;
# only gold is published here, where analysts and the agent layer query it
# (README, decision 7).
#
# Terraform owns the CONTAINER (the dataset); the pipeline owns the TABLES, whose
# schemas are declared next to the code that writes them (core/schema.py). A table
# managed here would drift on the first schema change and Terraform would try to
# "fix" it back. README, decision 6.

# delete_contents_on_destroy is the dataset twin of the buckets' force_destroy, and the
# same demo-environment exception applies: the pipeline creates TABLES inside this
# dataset, so a destroy after a demo run would otherwise fail on "dataset is not empty".
# In production this would be false — destroy must never silently take data with it.
resource "google_bigquery_dataset" "gold" {
  dataset_id                 = "gold"
  location                   = var.region
  description                = "Gold tables published from the lakehouse, named <domain>__<table>. See docs/DATA_CONTRACT.md."
  delete_contents_on_destroy = true
}
