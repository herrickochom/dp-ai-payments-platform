# GCP-KP-001X: canonical GCS Raw v2 destination. Separate from existing storage domains.
module "kp001x_canonical_bucket" {
  source              = "../../modules/governed_gcs_bucket"
  project_id          = var.project_id
  bucket_name         = "dp-pdm-ai-platform-dev"
  location            = var.region
  storage_class       = "STANDARD"
  versioning_enabled  = true
  governance_approved = var.governance_approved
  labels = merge(local.common_labels, {
    storage_domain = "canonical-raw-v2"
  })
}
