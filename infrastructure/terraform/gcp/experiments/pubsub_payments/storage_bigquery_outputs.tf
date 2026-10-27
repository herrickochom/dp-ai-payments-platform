output "bronze_bucket_name" {
  value = module.payments_bronze_storage.bucket_name
}

output "bigquery_dataset_id" {
  value = google_bigquery_dataset.payments.dataset_id
}

output "bigquery_table_id" {
  value = google_bigquery_table.payment_events.table_id
}
