variable "environment" {
  description = "Deployment environment."
  type        = string

  validation {
    condition = contains(
      ["dev", "staging", "prod"],
      var.environment
    )

    error_message = (
      "environment must be dev, staging, or prod."
    )
  }
}

variable "platform_name" {
  description = "Stable platform identifier."
  type        = string
  default     = "dp-pdm-ai-platform"

  validation {
    condition = (
      length(trimspace(var.platform_name)) > 0
    )

    error_message = (
      "platform_name must not be empty."
    )
  }
}

variable "region" {
  description = <<-EOT
    Provider-specific deployment region.

    This remains unset until a production cloud or
    infrastructure provider is explicitly selected.
  EOT

  type      = string
  default   = null
  nullable  = true
  sensitive = false
}
