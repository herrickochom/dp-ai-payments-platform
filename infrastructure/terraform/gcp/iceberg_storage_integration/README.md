# KP-014 isolated Iceberg Terraform module

Offline-only generated module, NOT wired into environments/dev. Default enable flag is false. No `terraform apply` or GCP API enablement is performed.

Design: BigLake-managed REST catalogue (`CATALOG_TYPE_BIGLAKE`), pilot namespace, Iceberg table with 24 original Bronze columns and a derived `parsed_event_timestamp` field. `payload_x_attributes` is serialized JSON string; conversion must be losslessly validated by the future writer. All 20,403 Bronze records have event date 2026-11-03. Partition pruning across dates cannot be demonstrated with this snapshot.

**Deployment blockers:** explicit catalogue/prefix/identity/cost approval; BigLake API currently not enabled; confirm supported BigLake REST API region, accepted table field type `timestamptz`, partition transform, GCS permissions, and managed-catalogue IAM; prepare writer and row-level reconciliation; review Terraform plan for destructive changes. No approval is implied by running this script.

To validate offline, use an already-installed local Google provider plugin in a separate working directory; never run `terraform apply` as part of validation. No existing root module or Terraform state is modified.
