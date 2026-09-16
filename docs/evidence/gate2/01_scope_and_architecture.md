# 01 - Scope and Architecture

## Pipeline

Source Systems -> Producers -> Kafka -> Consumers -> MinIO/S3 Raw ->
dbt/DuckDB Staging -> Bronze Iceberg -> Silver Iceberg -> Gold Iceberg ->
Consumption Iceberg -> BI / ML / Agents

Kafka does not ingest directly into Bronze.

Kafka transports producer events. Consumers persist those events to Raw.
dbt/DuckDB reads Raw through ephemeral Staging views and writes persistent
Bronze Iceberg tables.

The precise description used for lineage is therefore:

`Kafka-derived Bronze tables`

or:

`Bronze tables derived from Kafka-ingested Raw data`

## Persistence boundaries

- Raw: persistent MinIO/S3 source-fidelity data.
- Staging: ephemeral DuckDB/dbt parsing and transformation views.
- Bronze: persistent Iceberg operational/source-aligned data.
- Silver: persistent Iceberg analytical and privacy-boundary data.
- Silver Vault: restricted identity/token linkage.
- Gold: persistent business-ready dimensional/fact layer.
- Consumption: persistent pseudonymous/non-identifying serving layer by
  default.

Because Staging is ephemeral inside disposable dbt/DuckDB containers,
Raw -> Staging -> Bronze must execute within the same dbt invocation when
the Staging view is required.

## Gate 2 privacy boundary

Raw has the highest source fidelity and may contain direct identifiers.

Staging is restricted and temporary.

Bronze is controlled operational storage. Source identifiers may remain
where required for provenance and operational semantics.

Silver is the principal analytical privacy boundary. Ordinary analytical
models use canonical tokens or surrogate keys. Clear identity is limited
to explicitly restricted identity models.

Gold contains no unnecessary direct identity.

Consumption is pseudonymous/non-identifying by default. Identity resolution
requires a controlled and audited restricted service.

## Identity tiers

1. Direct real-world identity: restricted vault only.
2. Internal `beneficiary_id`: restricted linkage key.
3. Canonical `beneficiary_token`: ordinary analytical identifier.

Payment technical identifiers remain distinct from beneficiary identity
identifiers.

## POC principle

The local environment simulates production semantics rather than production
scale.

Controls demonstrated locally do not imply that production TLS, KMS,
encryption, IAM or secret-management infrastructure has been implemented.
