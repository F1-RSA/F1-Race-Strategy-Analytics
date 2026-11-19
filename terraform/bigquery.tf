# One dataset per layer: raw (external tables over the bucket, F3), staging and marts (dbt, F4).
locals {
  datasets = {
    f1_raw     = "Raw layer: external tables over the raw zone in Cloud Storage."
    f1_staging = "dbt staging models: typed and renamed views over f1_raw."
    f1_marts   = "dbt marts: fact and dimension tables for the analysis."
  }
}

resource "google_bigquery_dataset" "this" {
  for_each = local.datasets

  dataset_id  = each.key
  description = each.value
  location    = var.region
  labels      = var.labels

  delete_contents_on_destroy = var.force_destroy

  depends_on = [google_project_service.this]
}
