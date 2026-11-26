# Enterprise Raw v1 Object Metadata Architecture

Contract version: 1.0.0

Design approval date: 26 November 2026

Status: Approved design; implementation pending.

## Purpose

Govern Raw object metadata without altering original source payloads.

## Architectural principles

- Lossless source preservation.
- Immutable Avro payloads.
- Reuse existing source admission and routing contracts.
- Apply metadata during initial object creation.
- Verify durable compliance before acknowledgement.
- Fail closed on conflicts or verification failure.
- No parallel topic registry.

## Mandatory custom metadata

| Attribute | Authoritative source |
|---|---|
| `data-domain` | `approved_domain_taxonomy` |
| `data-layer` | `constant:raw` |
| `storage-contract-version` | `constant:v1` |
| `metadata-contract-version` | `constant:1.0.0` |
| `processing-mode` | `topic_registry` |
| `category` | `routing_contract` |
| `source-group` | `routing_contract` |
| `source-system` | `routing_contract` |
| `source-topic` | `validated_source_envelope` |
| `source-partition` | `validated_source_envelope` |
| `source-offset` | `validated_source_envelope` |

## Enterprise domain taxonomy

| Source family | Business domain |
|---|---|
| `icmn` | `payments` |
| `cpo` | `payments` |
| `wendi` | `payments` |
| `mobile_networks` | `payments` |
| `payment_lifecycle` | `payments` |
| `agent_network` | `agent_management` |
| `pdmis` | `programme_management` |
| `mdm` | `master_data_management` |
| `mdm_cdc` | `master_data_management` |
| `shared_operations` | `platform_operations` |

Exact topic bindings must be validated against the existing
registry and routing contract during Raw Metadata Implementation and Validation.

## Storage architecture

- Raw: immutable Avro objects and lossless source envelopes.
- Staging: non-Iceberg processing and execution evidence.
- Bronze, Silver, Gold and Consumption:
  Parquet files managed through Iceberg tables.

## Security and governance

Reuse existing information sensitivity classifications.
Business domain and sensitivity are independent.
Do not expose sensitive payload values in object metadata.
Retention enforcement requires separate live verification.

## Integration

The reviewed pilot references raw_v1_bridge_live.py and
raw_v1_cloud.py. Extend existing storage and verification
mechanisms rather than introduce another ingestion pipeline.

Complete historical execution provenance remains outstanding.

## Recovery

Use conditional creation and generation/metageneration
preconditions. Never overwrite an existing Raw payload.
Do not acknowledge messages after failed verification.

## Approved deferrals

Dataplex deployment is deferred to a separate governance
workstream and remains mandatory before End-to-End Raw Metadata Operational Acceptance closure.

Current Kafka-to-Raw reconciliation remains mandatory
for operational acceptance.

## Acceptance limitations

Automated Raw Object Metadata Pilot demonstrated single-object metadata mutation
and payload integrity using a direct SDK patch.
The reusable utility's mutation branch remains untested.

No bulk metadata updates or cloud changes are authorised.

See the decision and remediation registers for obligations.
