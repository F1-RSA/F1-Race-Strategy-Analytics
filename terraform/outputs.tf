output "project_id" {
  description = "Project the resources were created in."
  value       = var.project_id
}

output "bucket_name" {
  description = "Raw zone bucket (GCS_BUCKET for the cloud ingestion)."
  value       = google_storage_bucket.raw.name
}

output "datasets" {
  description = "BigQuery dataset IDs per layer."
  value       = { for k, d in google_bigquery_dataset.this : k => d.dataset_id }
}

output "region" {
  description = "Location of the bucket and the datasets."
  value       = var.region
}
