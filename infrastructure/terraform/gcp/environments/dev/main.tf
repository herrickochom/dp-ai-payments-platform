locals {
  common_labels = {
    platform    = var.platform_name
    environment = var.environment
    managed_by  = "terraform"
  }
}

# Cloud resources will be introduced only after
# architecture, security and cost review.
