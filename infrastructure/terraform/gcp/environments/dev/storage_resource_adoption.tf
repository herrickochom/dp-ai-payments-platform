# Existing synthetic pilot resources.
resource "google_pubsub_topic" "native_events_pilot" {
  project                    = var.project_id
  name                       = "pdm-native-events-poc-v1"
  message_retention_duration = "86400s"
  message_storage_policy {
    allowed_persistence_regions = ["europe-west2"]
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "google_pubsub_subscription" "native_events_pilot" {
  project                    = var.project_id
  name                       = "pdm-native-events-poc-sub-v1"
  topic                      = google_pubsub_topic.native_events_pilot.id
  ack_deadline_seconds       = 20
  message_retention_duration = "86400s"
  expiration_policy {
    ttl = ""
  }
  lifecycle {
    prevent_destroy = true
  }
}
