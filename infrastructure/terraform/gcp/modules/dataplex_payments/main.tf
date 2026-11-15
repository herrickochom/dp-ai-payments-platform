# -----------------------------------------------------------------------------
# Dataplex lake + Raw zone + GCS asset (1 GCS + N BQ).
# Discovery and quality scans are OFF by default (billed features). Flip the
# corresponding variable after explicit approval — never silently enabled.
# API endpoint: dataplex.googleapis.com (product renamed "Knowledge Catalog"
# in April 2026; the API name is unchanged).
# -----------------------------------------------------------------------------

resource "google_dataplex_lake" "payments" {
  project      = var.project_id
  location     = var.region
  name         = "payments-lake"
  display_name = "DP AI Payments Platform"
  description  = "Kafka -> Raw v1 -> Bronze -> dbt -> Consumption"
  labels = {
    platform = "dp-pdm-ai-platform"
    domain   = "payments"
  }
}

resource "google_dataplex_zone" "raw" {
  project      = var.project_id
  location     = var.region
  lake         = google_dataplex_lake.payments.name
  name         = "raw-zone"
  display_name = "Raw zone"
  type         = "RAW"

  resource_spec {
    location_type = "SINGLE_REGION"
  }

  discovery_spec {
    enabled = var.enable_discovery
  }
}

# NOTE: a Dataplex STORAGE_BUCKET asset registers the entire bucket.
# Prefix-level scoping (e.g. only raw/v1/) is not supported at asset level;
# use Dataplex data-quality/curation rules later if scoping is required.
resource "google_dataplex_asset" "gcs_raw_v1" {
  project       = var.project_id
  location      = var.region
  lake          = google_dataplex_lake.payments.name
  dataplex_zone = google_dataplex_zone.raw.name
  name          = "gcs-raw-v1"
  display_name  = "GCS bucket (whole bucket, not raw/v1-scoped)"

  resource_spec {
    name = "projects/${var.project_id}/buckets/${var.bucket}"
    type = "STORAGE_BUCKET"
  }

  discovery_spec {
    enabled = var.enable_discovery
  }
}

# ---- Optional data quality scans (premium SKU, count = 0 by default) ----
resource "google_dataplex_datascan" "bronze_key_not_null" {
  count        = var.enable_scans ? 1 : 0
  project      = var.project_id
  location     = var.region
  data_scan_id = "dq-bronze-key-not-null"
  display_name = "Bronze required-key not-null"

  data {
    resource = "//bigquery.googleapis.com/projects/${var.project_id}/datasets/bronze/tables/payments_raw_v1"
  }

  data_quality_spec {
    rules {
      column    = "event_id"
      dimension = "COMPLETENESS"
      non_null_expectation {}
    }
    rules {
      column    = "_kafka_topic"
      dimension = "COMPLETENESS"
      non_null_expectation {}
    }
  }

  execution_spec {
    trigger {
      on_demand {}
    }
  }
}
