variable "quarantine_retention_days" {
  description = "Development quarantine partition retention in days."
  type        = number
  default     = 30

  validation {
    condition = (
      var.quarantine_retention_days >= 1 &&
      var.quarantine_retention_days <= 365 &&
      floor(var.quarantine_retention_days) == var.quarantine_retention_days
    )
    error_message = "Quarantine retention must be an integer between 1 and 365 days."
  }
}
