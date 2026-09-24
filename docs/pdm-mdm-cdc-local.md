# PDM MDM source and CDC preparation

The platform `postgres` service remains infrastructure. The optional `pdm-source-postgres` service has its own PostgreSQL 16.15 image, database credentials, volume, schema initialization, and logical WAL settings. The source starts only under the `cdc` profile. Its host port defaults to localhost port 5433. Required variable names are `PDM_SOURCE_DB`, `PDM_SOURCE_USER`, `PDM_SOURCE_PASSWORD`, `PDM_CDC_USER`, and `PDM_CDC_PASSWORD`; no values belong in this repository.

The source schema defines ten MDM tables. The four beneficiary/linkage tables are restricted: `beneficiary_master_sources`, `golden_beneficiaries_restricted`, `source_crosswalk`, and `beneficiary_identity_alerts_restricted`. The six SACCO, agent, and geography tables are governed master or reference data. The dedicated CDC role has only database connect, `mdm` schema usage, replication, and SELECT on `mdm.sacco_master_sources`; it has no grant on the restricted tables.

MDM persistence is explicit. From a suitable runtime with the source files available, invoke `mdm_generator.py --validate-only`, `mdm_generator.py --target json` for compatibility export, or `mdm_generator.py --target postgres` for operational materialization. PostgreSQL mode additionally requires `PDM_SOURCE_HOST`, `PDM_SOURCE_DB`, `PDM_SOURCE_USER`, and `PDM_SOURCE_PASSWORD`; `PDM_SOURCE_DB_PORT` defaults to 5432. PostgreSQL mode deletes and inserts all ten datasets in one transaction, rolling back on failure. The existing `generate_all_sources.py` workflow does not invoke it automatically. The gated `mdm_publisher.py` remains a transitional JSON publisher and must not be enabled to duplicate Debezium publication.

The initial CDC contract is `platform/cdc/contracts/pdm_mdm_source.json`: connector `pdm-mdm-sacco-postgres`, publication `pdm_mdm_ordinary_pub`, slot `pdm_mdm_ordinary_slot`, topic prefix `cdc.pdm_mdm`, and topic `cdc.pdm_mdm.mdm.sacco_master_sources`. Only `mdm.sacco_master_sources` is allowlisted. The template uses `pgoutput`, JSON key/value converters, explicit initial snapshot, and delete tombstones. The source registry marks this source configured but not CDC capable or activated; the governed activation allowlist remains empty, and the Raw consumer's active CDC topic map remains empty. No connector registration occurs at Compose startup.

The repository has no approved Debezium/Kafka Connect version pin or compatibility evidence. Accordingly, a `cdc` Kafka Connect runtime is not defined yet. Select and verify a version against PostgreSQL 16.15, Kafka 4.3.1, and the repository's connector contract before adding that service or registering the connector. The connector's secret-provider file must be supplied externally and must not use platform PostgreSQL credentials.

## Future acceptance sequence — do not run during repository preparation

1. Materialize synthetic MDM data into `pdm-source-postgres`; use pgAdmin to verify a SACCO source row.
2. Verify the PostgreSQL logical slot and its LSN, then activate the approved Debezium connector through the governed source and allowlist process.
3. Use Kafka UI to inspect `cdc.pdm_mdm.mdm.sacco_master_sources`: topic, partition, Kafka offset, operation, and source metadata. PostgreSQL **LSN and Kafka offset are different coordinates**.
4. Verify that the existing CDC consumer and Debezium normalizer write canonical CDC Avro beneath `raw/v2/cdc/` before committing Kafka offsets, followed by the governed downstream path.

Use SACCO, not restricted beneficiary identity, for the first acceptance run. Kafka UI login is not per-topic authorization.
