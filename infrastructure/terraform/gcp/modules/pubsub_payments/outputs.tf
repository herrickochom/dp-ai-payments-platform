output "topic_id" {
  value = google_pubsub_topic.payments.id
}

output "subscription_id" {
  value = google_pubsub_subscription.payments.id
}

output "schema_id" {
  value = google_pubsub_schema.payments.id
}
