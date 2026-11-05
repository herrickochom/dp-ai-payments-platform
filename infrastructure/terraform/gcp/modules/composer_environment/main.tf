# STAGE 2: ISOLATED MODULE
#
# This module is intentionally not referenced by the
# GCP development Terraform root.
#
# No API activation is performed here.
# No cloud deployment is authorised.

resource "google_service_account" "composer" {
  project      = var.project_id
  account_id   = "pdm-composer-worker"
  display_name = "PDM Cloud Composer Worker"
}

resource "google_project_iam_member" "composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer.email}"
}

resource "google_composer_environment" "composer" {
  project = var.project_id
  region  = var.region
  name    = var.environment_name

  deletion_policy = "PREVENT"

  config {
    software_config {
      image_version = var.composer_image_version

      airflow_config_overrides = {
        core-load_examples = "False"
      }
    }

    node_config {
      service_account = google_service_account.composer.email
      network         = var.network
      subnetwork      = var.subnetwork
    }

    enable_private_environment = true
  }

  depends_on = [
    google_project_iam_member.composer_worker
  ]

  lifecycle {
    prevent_destroy = true
  }
}
