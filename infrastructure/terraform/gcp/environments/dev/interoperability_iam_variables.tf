# GCP-006D1: Governed interoperability IAM.
# Deployment is disabled by default.

variable "enable_interoperability_iam_deployment" {
  description = "Enable interoperability bucket IAM only after explicit approval."
  type        = bool
  default     = false
}

variable "interoperability_writer_principal" {
  description = "Externally managed IAM principal authorised to create test objects."
  type        = string
  default     = ""

  validation {
    condition = (
      var.interoperability_writer_principal == "" ||
      can(regex(
        "^(serviceAccount:|principal://|principalSet://)",
        var.interoperability_writer_principal
      ))
    )
    error_message = "Writer must be an approved service-account or federated principal identifier."
  }
}

variable "interoperability_reader_principal" {
  description = "Externally managed IAM principal authorised to read test objects."
  type        = string
  default     = ""

  validation {
    condition = (
      var.interoperability_reader_principal == "" ||
      can(regex(
        "^(serviceAccount:|principal://|principalSet://)",
        var.interoperability_reader_principal
      ))
    )
    error_message = "Reader must be an approved service-account or federated principal identifier."
  }
}
