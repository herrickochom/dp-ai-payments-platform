# Dedicated identity for publishing payment events.
resource "google_service_account" "payment_event_publisher" {
  project      = var.project_id
  account_id   = "pdm-dev-payment-publisher"
  display_name = "PDM Dev Payment Event Publisher"
  description  = "Dedicated service identity for payment-event publishing."
}

# Allow publishing only to the payment-events topic.
resource "google_pubsub_topic_iam_member" "payment_event_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.payment_events.name
  role    = "roles/pubsub.publisher"
  member  = google_service_account.payment_event_publisher.member
}

# Allow the Dataflow worker to consume from its subscription.
resource "google_pubsub_subscription_iam_member" "dataflow_payment_event_subscriber" {
  project      = var.project_id
  subscription = google_pubsub_subscription.payment_events_dataflow.name
  role         = "roles/pubsub.subscriber"
  member       = google_service_account.dataflow_worker.member
}
