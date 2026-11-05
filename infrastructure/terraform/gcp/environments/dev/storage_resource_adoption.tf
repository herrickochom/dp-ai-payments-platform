# Existing synthetic pilot resources.
resource "google_pubsub_topic" "kp001x_pilot" {
  project                    = "project-e48f194e-f44f-458d-b4f"
  name                       = "pdm-native-events-poc-v1"
  message_retention_duration = "86400s"
  message_storage_policy {
    allowed_persistence_regions = ["europe-west2"]
  }
  lifecycle {
    prevent_destroy = true
  }
}

resource "google_pubsub_subscription" "kp001x_pilot" {
  project                    = "project-e48f194e-f44f-458d-b4f"
  name                       = "pdm-native-events-poc-sub-v1"
  topic                      = google_pubsub_topic.kp001x_pilot.id
  ack_deadline_seconds       = 20
  message_retention_duration = "86400s"
  expiration_policy {
    ttl = ""
  }
  lifecycle {
    prevent_destroy = true
  }
}
