variable "enable_governed_storage_deployment" {
  description = "Enable governed GCS resources only after explicit deployment approval."
  type        = bool
  default     = false
}

variable "governance_approved" {
  description = "Configuration-level approval gate, not an independent authorisation mechanism."
  type        = bool
  default     = false
}
