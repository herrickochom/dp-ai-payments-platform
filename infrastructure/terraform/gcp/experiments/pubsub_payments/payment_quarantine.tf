# Durable quarantine for rejected Bronze payment events.
# Separate from the seven-day Dataflow operational bucket.

resource "google_bigquery_table" "payment_events_quarantine" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.payments.dataset_id
  table_id   = "payment_events_quarantine"

  deletion_protection = true

  time_partitioning {
    type          = "DAY"
    field         = "processing_timestamp"
    expiration_ms = var.quarantine_retention_days * 86400000
  }

  schema = jsonencode([
    {
      name = "raw_payload"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "error_reason"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "processing_timestamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    }
  ])

  labels = {
    environment = "dev"
    domain      = "payments"
    purpose     = "quarantine"
  }
}

output "payment_quarantine_table_id" {
  value = google_bigquery_table.payment_events_quarantine.table_id
}
