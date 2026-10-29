terraform {
  required_version = "~> 1.16.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = "europe-west2"
}

variable "project_id" {
  type = string
}

variable "bucket_name" {
  type = string
}

resource "google_storage_bucket" "terraform_state" {
  project       = var.project_id
  name          = var.bucket_name
  location      = "EUROPE-WEST2"
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  lifecycle {
    prevent_destroy = true
  }
}

output "terraform_state_bucket" {
  value = google_storage_bucket.terraform_state.name
}
