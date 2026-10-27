resource "google_pubsub_schema" "payments" {
  project    = var.project_id
  name       = var.schema_name
  type       = "AVRO"
  definition = var.avro_definition
}

resource "google_pubsub_topic" "payments" {
  project = var.project_id
  name    = var.topic_name

  schema_settings {
    schema   = google_pubsub_schema.payments.id
    encoding = "JSON"
  }

  message_retention_duration = "86400s"

  message_storage_policy {
    allowed_persistence_regions = var.allowed_persistence_regions
  }
}

resource "google_pubsub_subscription" "payments" {
  project = var.project_id
  name    = var.subscription_name
  topic   = google_pubsub_topic.payments.id

  ack_deadline_seconds       = 20
  message_retention_duration = "86400s"
  retain_acked_messages      = true

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "60s"
  }

  expiration_policy {
    ttl = "86400s"
  }
}
