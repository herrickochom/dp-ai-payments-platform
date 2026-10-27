# Governed Bronze ingestion bucket.
# Reuses the existing platform storage module.

module "payments_bronze_storage" {
  source = "../../modules/governed_gcs_bucket"

  project_id = var.project_id

  bucket_name = "pdm-bronze-${var.project_id}"
  location    = var.region

  storage_class      = "STANDARD"
  versioning_enabled = true

  governance_approved = var.governance_approved

  labels = {
    platform    = "dp-pdm-ai"
    environment = "dev"
    domain      = "payments"
    layer       = "bronze"
  }
}

# BigQuery analytical dataset.

resource "google_bigquery_dataset" "payments" {
  project    = var.project_id
  dataset_id = "pdm_payments_dev"

  friendly_name = "Payments Data AI Development"
  description   = "Development analytics for validated payment events."

  location = var.region

  delete_contents_on_destroy = false

  labels = {
    environment = "dev"
    domain      = "payments"
  }
}

# Partitioned payment event table.

resource "google_bigquery_table" "payment_events" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.payments.dataset_id
  table_id   = "payment_events"

  deletion_protection = true

  time_partitioning {
    type  = "DAY"
    field = "event_timestamp"
  }

  clustering = [
    "payment_id",
    "event_type"
  ]

  schema = jsonencode([
    {
      name = "event_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "payment_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "event_type"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "amount_minor"
      type = "INTEGER"
      mode = "REQUIRED"
    },
    {
      name = "currency"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "event_timestamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    }
  ])

  labels = {
    environment = "dev"
    domain      = "payments"
  }
}
