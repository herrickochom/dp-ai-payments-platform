output "deployment_context" {
  description = "GCP development deployment metadata."

  value = {
    project_id     = var.project_id
    region         = var.region
    environment    = var.environment
    platform       = var.platform_name
    resource_count = 0
  }
}
