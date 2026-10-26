locals {
  common_labels = {
    platform    = var.platform_name
    environment = var.environment
    managed_by  = "terraform"
  }

  resource_prefix = "${var.platform_name}-${var.environment}"
}

# Cloud resources will be introduced only after
# architecture, security and cost review.
