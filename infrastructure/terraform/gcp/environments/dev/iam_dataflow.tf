resource "google_service_account" "dataflow_worker" {
  project      = var.project_id
  account_id   = "pdm-dev-dataflow-worker"
  display_name = "PDM Dev Dataflow Worker"
  description  = "Dedicated execution identity for Dataflow workloads in the development environment."
}
