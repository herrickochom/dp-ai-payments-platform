resource "google_compute_network" "platform" {
  project                 = var.project_id
  name                    = "pdm-dev-vpc"
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"
}

resource "google_compute_subnetwork" "data" {
  project                  = var.project_id
  name                     = "pdm-dev-ew2-data-subnet"
  region                   = var.region
  network                  = google_compute_network.platform.id
  ip_cidr_range            = "10.155.0.0/24"
  private_ip_google_access = true
}
