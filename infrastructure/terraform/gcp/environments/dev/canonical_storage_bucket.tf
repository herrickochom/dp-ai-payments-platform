# canonical-storage-bucket: canonical GCS Raw v1 destination. Separate from existing storage domains.
module "canonical_storage_bucket" {
  source              = "../../modules/governed_gcs_bucket"
  project_id          = var.project_id
  bucket_name         = "dp-pdm-ai-platform-dev"
  location            = var.region
  storage_class       = "STANDARD"
  versioning_enabled  = false
  governance_approved = var.governance_approved
  labels = merge(local.common_labels, {
    storage_domain = "canonical-raw-v1"
  })
}
