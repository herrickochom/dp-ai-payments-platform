variable "enable_biglake_iceberg_catalog" {
  description = "Explicit opt-in; false creates no resources. Never enable without approved plan, cost ceiling and IAM."
  type        = bool
  default     = false
}
variable "project_id" {
  type    = string
  default = "project-e48f194e-f44f-458d-b4f"
}
variable "catalog_name" {
  type    = string
  default = "pdm-payments-iceberg-dev"
}
variable "approved_gcs_prefix" {
  description = "Proposed, NOT approved: must be reviewed before deployment."
  type        = string
  default     = "gs://dp-pdm-ai-platform-dev/lakehouse/iceberg/payments/"
  validation {
    condition     = startswith(var.approved_gcs_prefix, "gs://dp-pdm-ai-platform-dev/lakehouse/iceberg/payments/")
    error_message = "KP-014 must remain under the isolated pilot prefix."
  }
}
variable "namespace_id" {
  type    = string
  default = "payments"
}
variable "table_name" {
  type    = string
  default = "payment_events"
}
