# GCP-006D1: Bucket-scoped interoperability IAM.
# No service accounts, credentials or federation resources are created.

locals {
  interoperability_iam_enabled = (
    var.enable_interoperability_iam_deployment &&
    var.enable_governed_storage_deployment &&
    var.governance_approved
  )
}

resource "google_storage_bucket_iam_member" "interoperability_writer" {
  count = local.interoperability_iam_enabled ? 1 : 0

  bucket = module.governed_storage_domains["interoperability"].bucket_name
  role   = "roles/storage.objectCreator"
  member = var.interoperability_writer_principal

  lifecycle {
    precondition {
      condition = (
        trimspace(var.interoperability_writer_principal) != "" &&
        var.interoperability_writer_principal != var.interoperability_reader_principal
      )
      error_message = "Writer and reader must be distinct, non-empty approved principals."
    }
  }
}

resource "google_storage_bucket_iam_member" "interoperability_reader" {
  count = local.interoperability_iam_enabled ? 1 : 0

  bucket = module.governed_storage_domains["interoperability"].bucket_name
  role   = "roles/storage.objectViewer"
  member = var.interoperability_reader_principal

  lifecycle {
    precondition {
      condition = (
        trimspace(var.interoperability_reader_principal) != "" &&
        var.interoperability_writer_principal != var.interoperability_reader_principal
      )
      error_message = "Writer and reader must be distinct, non-empty approved principals."
    }
  }
}
