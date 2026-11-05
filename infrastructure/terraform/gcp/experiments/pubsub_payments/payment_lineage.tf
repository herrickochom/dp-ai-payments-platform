# Stage 10 execution lineage ledger. This resource alone does not prove that
# Dataflow writes lineage: producer-side integration is still required.
resource "google_bigquery_table" "payment_execution_lineage" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.payments.dataset_id
  table_id            = "payment_execution_lineage"
  deletion_protection = true
  schema = jsonencode([
    { name = "execution_id", type = "STRING", mode = "REQUIRED" },
    { name = "attempt_number", type = "INTEGER", mode = "REQUIRED" },
    { name = "dataflow_job_id", type = "STRING", mode = "REQUIRED" },
    { name = "spec_digest", type = "STRING", mode = "REQUIRED" },
    { name = "event_id", type = "STRING", mode = "REQUIRED" },
    { name = "output_sha256", type = "STRING", mode = "REQUIRED" },
    { name = "recorded_at", type = "TIMESTAMP", mode = "REQUIRED" }
  ])
  labels = { environment = "dev", domain = "payments" }
}
