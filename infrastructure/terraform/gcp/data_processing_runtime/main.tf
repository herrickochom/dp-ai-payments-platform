terraform {
  required_version = "~> 1.16.0"
  required_providers {
    google = { source = "hashicorp/google", version = "~> 7.0" }
  }
}
provider "google" {
  project = var.project_id
  region  = var.region
}
variable "project_id" { type = string }
variable "region" { type = string }
variable "network_name" {
  type    = string
  default = "default"
}
variable "subnet_cidr" {
  type    = string
  default = "10.155.0.0/24"
}
data "google_compute_network" "existing" {
  name    = var.network_name
  project = var.project_id
}
resource "google_compute_subnetwork" "dataflow_pilot" {
  name                     = "pdm-stage11-dataflow-private"
  project                  = var.project_id
  region                   = var.region
  network                  = data.google_compute_network.existing.id
  ip_cidr_range            = var.subnet_cidr
  private_ip_google_access = true
  description              = "Isolated subnet for bounded Stage 11 synthetic Dataflow pilot"
}
resource "google_service_account" "dataflow_worker" {
  project      = var.project_id
  account_id   = "pdm-stage11-df-worker"
  display_name = "Stage 11 bounded synthetic Dataflow worker"
}
# No project-wide IAM roles are granted here. A future execution plan must
# identify exact dataset/bucket and grant resource-scoped permissions.
output "worker_service_account" { value = google_service_account.dataflow_worker.email }
output "subnet_self_link" { value = google_compute_subnetwork.dataflow_pilot.self_link }
