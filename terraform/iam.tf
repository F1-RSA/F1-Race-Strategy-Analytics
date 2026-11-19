# Identity of the pipeline (ingestion, external tables, dbt). Terraform creates no key: a key
# would be stored in plain text in the state file.
resource "google_service_account" "pipeline" {
  account_id   = "f1-pipeline"
  display_name = "F1 RSA pipeline"
  description  = "Writes the raw zone and runs BigQuery jobs for the F1 Race Strategy Analytics pipeline."

  depends_on = [google_project_service.this]
}

# Read, write and overwrite objects in the raw bucket only (no bucket settings, no other buckets).
resource "google_storage_bucket_iam_member" "pipeline_raw_objects" {
  bucket = google_storage_bucket.raw.name
  role   = "roles/storage.objectAdmin"
  member = google_service_account.pipeline.member
}

# Create and replace tables and views in the three datasets only.
resource "google_bigquery_dataset_iam_member" "pipeline_data_editor" {
  for_each = google_bigquery_dataset.this

  dataset_id = each.value.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = google_service_account.pipeline.member
}

# Running a query is a job, and jobs exist only at project level.
resource "google_project_iam_member" "pipeline_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = google_service_account.pipeline.member
}
