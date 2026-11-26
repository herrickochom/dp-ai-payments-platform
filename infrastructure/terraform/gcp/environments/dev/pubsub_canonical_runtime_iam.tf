# Canonical Pub/Sub IAM. Subscriber grant is temporary for local bounded validation.
# Revoke the user grant when the dedicated Raw v1 worker service account is deployed.
resource "google_pubsub_topic_iam_member" "canonical_payment_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.payments_source_events_v1.name
  role    = "roles/pubsub.publisher"
  member  = google_service_account.payment_event_publisher.member
}

resource "google_pubsub_subscription_iam_member" "canonical_raw_v1_local_subscriber" {
  project      = var.project_id
  subscription = google_pubsub_subscription.payments_raw_ingestion.name
  role         = "roles/pubsub.subscriber"
  member       = "user:ohms_h@yahoo.com"
}
