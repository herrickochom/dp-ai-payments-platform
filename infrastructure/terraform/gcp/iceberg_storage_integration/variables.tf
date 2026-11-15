variable "enable_iceberg_pilot" {
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
  default = "pdm-iceberg-pilot"
}
variable "approved_gcs_prefix" {
  description = "Proposed, NOT approved: must be reviewed before deployment."
  type        = string
  default     = "gs://dp-pdm-ai-platform-dev/iceberg/pilot/"
  validation {
    condition     = startswith(var.approved_gcs_prefix, "gs://dp-pdm-ai-platform-dev/iceberg/pilot/")
    error_message = "The Iceberg pilot must remain under the isolated pilot prefix."
  }
}
variable "namespace_id" {
  type    = string
  default = "payments_pilot"
}
variable "table_name" {
  type    = string
  default = "payments_events_iceberg_pilot"
}
