data "google_project" "current" {
  project_id = var.project_id
}

resource "google_project_service_identity" "pubsub" {
  provider = google-beta

  project = var.project_id
  service = "pubsub.googleapis.com"
}

resource "google_storage_bucket_iam_member" "pubsub_export_writer" {
  bucket = module.payments_bronze_storage.bucket_name
  role   = "roles/storage.objectCreator"

  member = "serviceAccount:${google_project_service_identity.pubsub.email}"
}

resource "google_pubsub_subscription" "payments_bronze_export" {
  project = var.project_id

  name  = "pdm-payments-bronze-export-v1"
  topic = module.pubsub_payments.topic_id

  ack_deadline_seconds       = 20
  message_retention_duration = "86400s"

  cloud_storage_config {
    bucket = module.payments_bronze_storage.bucket_name

    filename_prefix = "payments/events/"
    filename_suffix = ".json"

    max_duration = "300s"

    text_config {}
  }

  depends_on = [
    google_storage_bucket_iam_member.pubsub_export_writer
  ]
}
