terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # Local state (terraform.tfstate, ignored by git). A remote backend would need a bucket
  # before the first apply; see docs/decisions.md, F1.
}

provider "google" {
  project = var.project_id
  region  = var.region

  # Bill API calls made with user credentials (gcloud auth application-default login) to this
  # project, so no separate quota project has to be configured.
  user_project_override = true
  billing_project       = var.project_id
}
