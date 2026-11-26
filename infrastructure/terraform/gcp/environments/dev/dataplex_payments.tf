# Dataplex lake, zones and assets; discovery and scans disabled.
# Written by dataplex_configuration_correction.sh; safe to remove.
module "dataplex_payments" {
  source = "../../modules/dataplex_payments"

  project_id       = var.project_id
  region           = var.region
  bucket           = var.canonical_data_bucket
  enable_discovery = false
  enable_scans     = false
}
