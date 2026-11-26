# Stage 8-9: Cloud Composer development integration.
# Deployment requires separate explicit approval.

module "composer_environment" {
  source = "../../modules/composer_environment"

  project_id             = var.project_id
  region                 = var.region
  environment_name       = "pdm-dev-composer"
  composer_image_version = "composer-3-airflow-3.3.1-build.9"

  network    = google_compute_network.platform.id
  subnetwork = google_compute_subnetwork.data.id
}
