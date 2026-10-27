# Dedicated operational storage for Apache Beam / Dataflow.
# This bucket is not a Bronze, Silver, Gold or Consumption layer.

resource "google_storage_bucket" "dataflow_operational" {
  project  = var.project_id
  name     = "pdm-dataflow-${var.project_id}"
  location = var.region

  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  force_destroy = false

  versioning {
    enabled = false
  }

  # Development POC retention: expire operational objects
  # seven days after creation.
  lifecycle_rule {
    action {
      type = "Delete"
    }

    condition {
      age = 7
    }
  }

  labels = {
    platform    = "dp-pdm-ai"
    environment = "dev"
    domain      = "payments"
    purpose     = "dataflow"
    managed_by  = "terraform"
  }

  lifecycle {
    prevent_destroy = true

    precondition {
      condition     = var.governance_approved
      error_message = "Dataflow storage requires explicit governance approval."
    }
  }
}

# Dataflow workers require object access for staged packages,
# execution files and temporary BigQuery load artifacts.

resource "google_storage_bucket_iam_member" "dataflow_operational_worker" {
  bucket = google_storage_bucket.dataflow_operational.name
  role   = "roles/storage.objectAdmin"

  member = "serviceAccount:${google_service_account.dataflow_worker.email}"
}
