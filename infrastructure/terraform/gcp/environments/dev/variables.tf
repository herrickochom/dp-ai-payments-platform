variable "project_id" {
  description = "Existing Google Cloud project ID."
  type        = string

  validation {
    condition = can(regex(
      "^[a-z][a-z0-9-]{4,28}[a-z0-9]$",
      var.project_id
    ))
    error_message = "Provide a valid GCP project ID."
  }
}

variable "region" {
  description = "Google Cloud deployment region."
  type        = string
  default     = "europe-west2"
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging or prod."
  }
}

variable "platform_name" {
  description = "Stable platform identifier."
  type        = string
  default     = "dp-pdm-ai-platform"
}
