variable "project_id" { type = string }
variable "region" {
  type    = string
  default = "europe-west2"
  validation {
    condition     = var.region == "europe-west2"
    error_message = "MDM Cloud SQL must remain in europe-west2."
  }
}
variable "instance_name" { type = string }
variable "private_network_self_link" {
  type        = string
  description = "Existing VPC self-link. Private services access must be established separately and verified before provisioning."
}
variable "database_name" {
  type    = string
  default = "pdm_mdm"
}
variable "database_version" {
  type    = string
  default = "POSTGRES_16"
  validation {
    condition     = var.database_version == "POSTGRES_16"
    error_message = "PostgreSQL version must be explicitly reviewed before changing."
  }
}
variable "tier" {
  type        = string
  description = "Explicitly approved Cloud SQL machine tier; no default to prevent unreviewed cost."
}
variable "availability_type" {
  type        = string
  description = "Explicitly approved ZONAL or REGIONAL availability setting."
  validation {
    condition     = contains(["ZONAL", "REGIONAL"], var.availability_type)
    error_message = "Choose ZONAL or REGIONAL explicitly."
  }
}
variable "deletion_protection" {
  type    = bool
  default = true
  validation {
    condition     = var.deletion_protection
    error_message = "Deletion protection must remain enabled."
  }
}
