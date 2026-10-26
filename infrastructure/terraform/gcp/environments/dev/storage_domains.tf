# GCP-006C: Governed storage domain integration.
# Configuration only. Deployment is disabled by default.

locals {
  storage_domains = {
    operational = {
      storage_class = "STANDARD"
      purpose       = "Operational data and approved low-latency workloads"
      datasets = [
        "operational_events",
        "payment_transactions",
      ]
    }

    historical_analytics = {
      storage_class = "NEARLINE"
      purpose       = "Infrequently accessed historical analytics"
      datasets = [
        "historical_analytics",
      ]
    }

    historical_records = {
      storage_class = "COLDLINE"
      purpose       = "Governed historical financial records"
      datasets = [
        "historical_payment_records",
      ]
    }

    archive = {
      storage_class = "ARCHIVE"
      purpose       = "Long-term financial records and audit evidence"
      datasets = [
        "audit_evidence",
        "long_term_financial_archives",
      ]
    }

    interoperability = {
      storage_class = "STANDARD"
      purpose       = "Isolated non-production interoperability testing"
      datasets = [
        "temporary_test_data",
      ]
    }
  }

  protected_storage_categories = [
    "iceberg_tables",
    "flink_recovery_state",
  ]
}

module "governed_storage_domains" {
  source = "../../modules/governed_gcs_bucket"

  for_each = var.enable_governed_storage_deployment ? local.storage_domains : {}

  project_id  = var.project_id
  bucket_name = lower("${local.resource_prefix}-${replace(each.key, "_", "-")}")
  location    = var.region

  storage_class       = each.value.storage_class
  versioning_enabled  = true
  governance_approved = var.governance_approved

  labels = merge(
    local.common_labels,
    {
      storage_domain = replace(each.key, "_", "-")
    }
  )
}
