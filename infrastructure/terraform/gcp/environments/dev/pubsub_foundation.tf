resource "google_pubsub_topic" "payment_events" {
  project = var.project_id
  name    = "pdm-dev-payment-events"

  message_retention_duration = "86400s"

  message_storage_policy {
    allowed_persistence_regions = ["europe-west2"]
    enforce_in_transit          = true
  }
}

resource "google_pubsub_subscription" "payment_events_dataflow" {
  project = var.project_id
  name    = "pdm-dev-payment-events-dataflow-sub"
  topic   = google_pubsub_topic.payment_events.id

  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"

  expiration_policy {
    ttl = ""
  }
}
