# GCP-KP-001U: optional per-domain filter. Default preserves existing all-domains behaviour.
variable "gcp_kp001u_enabled_storage_domains" {
  description = "Storage domains eligible for deployment when the existing deployment gate is enabled."
  type        = set(string)
  default     = ["operational", "historical_analytics", "historical_records", "archive", "interoperability"]
  validation {
    condition = length(setsubtract(var.gcp_kp001u_enabled_storage_domains, toset([
      "operational", "historical_analytics", "historical_records", "archive", "interoperability"
    ]))) == 0
    error_message = "Only the five defined governed storage domains are supported."
  }
}
