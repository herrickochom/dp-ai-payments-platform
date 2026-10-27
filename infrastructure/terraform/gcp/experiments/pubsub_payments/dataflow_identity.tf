resource "google_service_account" "dataflow_worker" {
  project = var.project_id

  account_id   = "pdm-dataflow-worker"
  display_name = "Payments Dataflow Worker"
}

resource "google_project_iam_member" "dataflow_worker_role" {
  project = var.project_id
  role    = "roles/dataflow.worker"

  member = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

resource "google_storage_bucket_iam_member" "dataflow_storage_reader" {
  bucket = module.payments_bronze_storage.bucket_name
  role   = "roles/storage.objectViewer"

  member = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

resource "google_bigquery_dataset_iam_member" "dataflow_bigquery_writer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.payments.dataset_id

  role = "roles/bigquery.dataEditor"

  member = "serviceAccount:${google_service_account.dataflow_worker.email}"
}

resource "google_project_iam_member" "dataflow_bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"

  member = "serviceAccount:${google_service_account.dataflow_worker.email}"
}
