variable "project_id" {
  description = "Existing GCP project."
  type        = string
}

variable "region" {
  description = "GCP region."
  type        = string
  default     = "europe-west2"
}

variable "environment_name" {
  description = "Cloud Composer environment name."
  type        = string
}

variable "composer_image_version" {
  description = "Approved, fully pinned Composer 3 image version."
  type        = string

  validation {
    condition = can(regex(
      "^composer-3-airflow-[0-9]+\\.[0-9]+\\.[0-9]+-build\\.[0-9]+$",
      var.composer_image_version
    ))
    error_message = "A fully pinned Composer 3 image is required."
  }
}

variable "network" {
  description = "Approved VPC network identifier."
  type        = string
}

variable "subnetwork" {
  description = "Approved regional subnet identifier."
  type        = string
}
