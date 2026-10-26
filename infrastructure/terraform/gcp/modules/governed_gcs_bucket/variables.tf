variable "project_id" {
  description = "Google Cloud project containing the bucket."
  type        = string
}

variable "bucket_name" {
  description = "Globally unique GCS bucket name."
  type        = string

  validation {
    condition = (
      length(var.bucket_name) >= 3 &&
      length(var.bucket_name) <= 63 &&
      can(regex(
        "^[a-z0-9][a-z0-9._-]*[a-z0-9]$",
        var.bucket_name
      ))
    )
    error_message = "Bucket name must meet the module naming requirements."
  }
}

variable "location" {
  description = "GCS bucket location."
  type        = string
  default     = "europe-west2"
}

variable "storage_class" {
  description = "Initial GCS storage class."
  type        = string
  default     = "STANDARD"

  validation {
    condition = contains(
      ["STANDARD", "NEARLINE", "COLDLINE", "ARCHIVE"],
      var.storage_class
    )
    error_message = "Unsupported GCS storage class."
  }
}

variable "versioning_enabled" {
  description = "Enable object versioning. Additional versions incur storage charges."
  type        = bool
  default     = true
}

variable "labels" {
  description = "Resource ownership and governance labels."
  type        = map(string)
  default     = {}
}

variable "governance_approved" {
  description = "Explicit approval gate for bucket creation or updates."
  type        = bool
  default     = false
}
