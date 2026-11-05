# KP-013 — Dataplex lake/zones/assets, no discovery, no scans.
# Written by dataplex_configuration_correction.sh; safe to remove.
module "dataplex_payments" {
  source = "../../modules/dataplex_payments"

  project_id       = "project-e48f194e-f44f-458d-b4f"
  region           = "europe-west2"
  bucket           = "dp-pdm-ai-platform-dev"
  curated_datasets = ["bronze", "staging", "silver", "gold", "consumption"]
  enable_discovery = false
  enable_scans     = false
}
