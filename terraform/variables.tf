variable "project_id" {
  description = "ID of an existing Google Cloud project with billing enabled. The only required variable."
  type        = string
}

variable "region" {
  description = "Region of the bucket and the BigQuery datasets. Both must be in the same location so BigQuery can read the bucket through external tables."
  type        = string
  default     = "europe-west6"
}

variable "force_destroy" {
  description = "Let terraform destroy delete the bucket and the datasets even if they still contain data. The raw zone can be reloaded from the APIs at any time."
  type        = bool
  default     = true
}

variable "labels" {
  description = "Labels added to every resource that supports them."
  type        = map(string)
  default = {
    project = "f1-rsa"
    managed = "terraform"
  }
}
