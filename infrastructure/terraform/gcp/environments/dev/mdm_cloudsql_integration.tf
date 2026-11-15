# Operational MDM Cloud SQL; offline wiring only. Deployment requires separate approval.
module "mdm_cloudsql" {
  source                    = "../../modules/mdm_cloudsql"
  project_id                = "project-e48f194e-f44f-458d-b4f"
  instance_name             = "pdm-dev-mdm-postgres"
  tier                      = "db-custom-1-3840"
  availability_type         = "ZONAL"
  private_network_self_link = google_compute_network.platform.self_link
  depends_on                = [google_service_networking_connection.mdm_cloudsql_psa]
}
