# Dataplex lake, zones and assets; discovery and scans disabled.
# Written by dataplex_configuration_correction.sh; safe to remove.
module "dataplex_payments" {
  source = "../../modules/dataplex_payments"

  project_id       = "project-e48f194e-f44f-458d-b4f"
  region           = "europe-west2"
  bucket           = "dp-pdm-ai-platform-dev"
  enable_discovery = false
  enable_scans     = false
}
