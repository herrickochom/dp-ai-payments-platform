# MDM Cloud SQL — isolated Terraform proposal

This module is intentionally **not connected to an environment**. No resource is provisioned by generating it.

Before any live plan/apply: confirm the actual GCP project, VPC self-link, Private Services Access / service networking, DNS and routing from the Debezium runtime, Cloud SQL PostgreSQL version and machine tier, HA choice, budget, certificate strategy, secrets, database roles, CDC logical decoding/replication settings, backups and retention, and IAM. Private connectivity is not created by this module.

No MDM data is migrated by this module. No Debezium connector is activated. No passwords or credentials are generated or embedded. This is a starting point for an explicitly approved infrastructure review, not a production-ready deployment. Review Google provider compatibility and Cloud SQL TLS policy before use.

The MDM source schema remains at `platform/databases/pdm-source-postgres/initdb/10_mdm_schema.sql` and is not applied by Terraform.
