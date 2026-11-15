# MDM Cloud SQL private services access prerequisites.
# Offline definition only: no Cloud SQL module wiring or automatic deployment.
# Reserved range explicitly supplied by operator after network review.
resource "google_compute_global_address" "mdm_cloudsql_psa" {
  name          = "pdm-dev-mdm-cloudsql-psa"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  address       = "10.156.0.0"
  prefix_length = 24
  network       = google_compute_network.platform.id
}

resource "google_service_networking_connection" "mdm_cloudsql_psa" {
  network                 = google_compute_network.platform.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.mdm_cloudsql_psa.name]
}
