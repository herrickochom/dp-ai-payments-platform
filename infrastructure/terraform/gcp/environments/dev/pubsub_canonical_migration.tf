# Additive migration only. Do not change or remove existing Pub/Sub resources.
# No automatic cutover, deployment, publishing, consumption, or retirement.
resource "google_pubsub_topic" "payments_source_events_v1" {
  project                    = var.project_id
  name                       = "pdm-dev-payments-source-events-v1"
  message_retention_duration = "86400s"
  message_storage_policy {
    allowed_persistence_regions = ["europe-west2"]
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "google_pubsub_subscription" "payments_raw_ingestion" {
  project                    = var.project_id
  name                       = "pdm-dev-payments-raw-ingestion-sub"
  topic                      = google_pubsub_topic.payments_source_events_v1.id
  ack_deadline_seconds       = 20
  message_retention_duration = "86400s"
  expiration_policy {
    ttl = ""
  }
  lifecycle {
    prevent_destroy = true
  }
}
