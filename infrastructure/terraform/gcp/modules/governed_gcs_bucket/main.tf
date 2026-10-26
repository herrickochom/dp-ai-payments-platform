resource "google_storage_bucket" "governed" {
  project  = var.project_id
  name     = var.bucket_name
  location = var.location

  storage_class = var.storage_class

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  force_destroy = false

  versioning {
    enabled = var.versioning_enabled
  }

  labels = merge(
    var.labels,
    {
      managed_by = "terraform"
    }
  )

  lifecycle {
    prevent_destroy = true

    precondition {
      condition     = var.governance_approved
      error_message = "GCS deployment requires explicit governance approval."
    }
  }
}
