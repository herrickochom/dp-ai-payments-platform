# ISOLATED PROPOSAL ONLY: not referenced by any environment; no deployment permitted without approval.
resource "google_sql_database_instance" "mdm" {
  project             = var.project_id
  name                = var.instance_name
  region              = var.region
  database_version    = var.database_version
  deletion_protection = var.deletion_protection

  settings {
    tier              = var.tier
    edition           = "ENTERPRISE"
    availability_type = var.availability_type
    disk_autoresize   = true
    backup_configuration {
      enabled                        = false
      point_in_time_recovery_enabled = false
    }
    ip_configuration {
      ipv4_enabled = var.public_ip_enabled
      dynamic "authorized_networks" {
        for_each = var.authorized_networks
        content {
          name  = authorized_networks.value.name
          value = authorized_networks.value.value
        }
      }
      private_network                               = var.private_network_self_link
      enable_private_path_for_google_cloud_services = false
      ssl_mode                                      = "ENCRYPTED_ONLY"
    }
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "google_sql_database" "mdm" {
  project  = var.project_id
  instance = google_sql_database_instance.mdm.name
  name     = var.database_name
}
