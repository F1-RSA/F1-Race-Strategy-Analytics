# Raw zone of the data lake. Project IDs are globally unique, so the bucket name derived from
# the project ID is unique as well and needs no random suffix.
resource "google_storage_bucket" "raw" {
  name     = "${var.project_id}-f1-raw"
  location = var.region
  labels   = var.labels

  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = var.force_destroy

  # A rerun overwrites the same object on purpose, so old versions are not kept.
  versioning {
    enabled = false
  }

  # No soft delete: deleted objects would otherwise be billed for another 7 days, and the raw
  # zone can be reloaded from the APIs.
  soft_delete_policy {
    retention_duration_seconds = 0
  }

  depends_on = [google_project_service.this]
}
