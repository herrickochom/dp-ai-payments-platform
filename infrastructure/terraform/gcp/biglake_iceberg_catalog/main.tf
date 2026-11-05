# KP-014: isolated module. Not referenced by environments/dev. Default creates zero resources.
# NO API enablement, IAM changes, new buckets, external calls or writes in this module.
locals {
  bronze_fields = [
    { id = 1, name = "event_id", type = "string", required = true },
    { id = 2, name = "message_id", type = "string", required = false },
    { id = 3, name = "event_timestamp", type = "string", required = true },
    { id = 4, name = "source_system", type = "string", required = true },
    { id = 5, name = "message_type", type = "string", required = true },
    { id = 6, name = "event_family", type = "string", required = false },
    { id = 7, name = "payload_amount", type = "double", required = false },
    { id = 8, name = "payload_currency", type = "string", required = false },
    { id = 9, name = "payload_debtor", type = "string", required = false },
    { id = 10, name = "payload_creditor", type = "string", required = false },
    { id = 11, name = "payload_status", type = "string", required = false },
    { id = 12, name = "payload_x_attributes", type = "string", required = false },
    { id = 13, name = "event_data", type = "string", required = false },
    { id = 14, name = "parsed_event_data", type = "string", required = false },
    { id = 15, name = "_kafka_topic", type = "string", required = true },
    { id = 16, name = "_kafka_partition", type = "long", required = true },
    { id = 17, name = "_kafka_offset", type = "long", required = true },
    { id = 18, name = "_kafka_timestamp", type = "timestamptz", required = true },
    { id = 19, name = "_kafka_category", type = "string", required = false },
    { id = 20, name = "_kafka_source_group", type = "string", required = false },
    { id = 21, name = "_kafka_source_system", type = "string", required = false },
    { id = 22, name = "_raw_object_path", type = "string", required = true },
    { id = 23, name = "_bronze_pipeline_version", type = "string", required = true },
    { id = 24, name = "_bronze_loaded_at", type = "timestamptz", required = true },
    { id = 25, name = "parsed_event_timestamp", type = "timestamptz", required = true },
  ]
}
resource "google_biglake_iceberg_catalog" "payments" {
  count            = var.enable_biglake_iceberg_catalog ? 1 : 0
  project          = var.project_id
  name             = var.catalog_name
  catalog_type     = "CATALOG_TYPE_BIGLAKE"
  default_location = var.approved_gcs_prefix
  credential_mode  = "CREDENTIAL_MODE_END_USER"
  deletion_policy  = "PREVENT"
  lifecycle { prevent_destroy = true }
}
resource "google_biglake_iceberg_namespace" "payments" {
  count           = var.enable_biglake_iceberg_catalog ? 1 : 0
  project         = var.project_id
  catalog         = google_biglake_iceberg_catalog.payments[0].name
  namespace_id    = var.namespace_id
  deletion_policy = "PREVENT"
  lifecycle { prevent_destroy = true }
}
resource "google_biglake_iceberg_table" "payment_events" {
  count           = var.enable_biglake_iceberg_catalog ? 1 : 0
  project         = var.project_id
  catalog         = google_biglake_iceberg_catalog.payments[0].name
  namespace       = google_biglake_iceberg_namespace.payments[0].namespace_id
  name            = var.table_name
  location        = "gs://dp-pdm-ai-platform-dev/lakehouse/iceberg/payments/payments/payment_events/01siv1j34x5u24m3"
  deletion_policy = "PREVENT"
  lifecycle { prevent_destroy = true }
  schema {
    dynamic "fields" {
      for_each = local.bronze_fields
      content {
        id       = fields.value.id
        name     = fields.value.name
        type     = fields.value.type
        required = fields.value.required
      }
    }
  }
  partition_spec {
    fields {
      name      = "event_day"
      source_id = 25
      transform = "day"
    }
  }
}
