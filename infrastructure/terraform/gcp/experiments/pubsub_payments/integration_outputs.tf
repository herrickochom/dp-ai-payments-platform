output "pubsub_export_subscription" {
  value = google_pubsub_subscription.payments_bronze_export.name
}

output "dataflow_worker_email" {
  value = google_service_account.dataflow_worker.email
}
