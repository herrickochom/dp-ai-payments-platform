output "platform_contract" {
  description = "Provider-neutral Terraform foundation metadata."

  value = {
    platform_name     = var.platform_name
    environment       = var.environment
    provider_selected = false
  }
}
