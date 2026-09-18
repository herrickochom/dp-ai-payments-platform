# PDM CDC — Debezium

Debezium is the selected CDC adapter for mutable database source systems.

## Production path

Operational Database
-> Debezium / Kafka Connect
-> Kafka CDC topics
-> governed Raw consumer
-> Raw immutable CDC log
-> bounded incremental transformation
-> Bronze / Silver
-> MDM / Gold / Consumption

CDC is continuous and event driven.

Airflow does not poll databases or Kafka to implement CDC.

Kafka does not ingest directly into Bronze.

## Activation posture

CDC activation is fail closed.

The presence of Debezium configuration, Kafka Connect configuration, connector
templates, Terraform resources or source code does not activate CDC.

A source may be activated only when it is:

1. registered as `CDC_DATABASE`;
2. declared a mutable database source;
3. explicitly CDC capable;
4. explicitly CDC activated;
5. explicitly allowlisted by the CDC governance contract; and
6. accepted by production preflight controls.

The platform PostgreSQL database is infrastructure and must not be used as an
operational CDC source unless it is deliberately reclassified through the
source-governance process.

## Version and compatibility policy

Production Debezium and Kafka Connect deployments must use explicit immutable
release pins.

`latest`, floating major tags and floating major/minor tags are prohibited.

Before deployment, compatibility evidence must cover:

- Debezium;
- Kafka Connect;
- Kafka broker;
- PostgreSQL.

The actual approved production release pin belongs at the deployment/IaC
boundary. This source contract does not invent a production version before the
deployment target and compatibility matrix are selected.

## PostgreSQL source prerequisites

Every PostgreSQL CDC source requires:

- `wal_level=logical`;
- Debezium PostgreSQL connector using `pgoutput`;
- a dedicated least-privilege replication identity;
- no root/superuser application identity for routine capture;
- a pre-created publication;
- publication auto-creation disabled;
- a dedicated pre-created replication slot;
- explicit schema allowlist;
- explicit table allowlist;
- WAL retention policy;
- replication-slot capacity and retention monitoring;
- an explicit `REPLICA IDENTITY` policy for captured tables;
- suitable replica identity where DELETE or complete before-image semantics
  require it.

Replication slots must not be silently dropped merely because a connector
stops.

## Secrets

Production database credentials must not be committed to:

- source code;
- connector templates;
- Terraform source;
- Compose configuration;
- CI/CD workflow source.

Kafka Connect must resolve connector credentials through an approved
ConfigProvider mechanism.

The local template uses Kafka Connect `FileConfigProvider` syntax as the
provider-neutral connector contract. Production secret material must be
delivered from an approved secret/KMS backend through the deployment layer.

Terraform may manage references, IAM and secret integration, but plaintext
production secret values must not be committed to Terraform source.

## Capture semantics

Production connectors require explicit policy for:

- snapshot mode;
- heartbeat interval;
- transaction metadata;
- delete/tombstone handling;
- schema evolution;
- source LSN/position;
- source transaction metadata where available;
- event timestamp;
- ingestion timestamp.

`provide.transaction.metadata=true` is required.

`tombstones.on.delete=true` is required.

Snapshot mode must never rely on an accidental connector default.

## Offset and history durability

Kafka Connect connector offsets must use durable production storage.

Schema-history state, where required by the selected connector/version, must
also use durable production storage.

Loss of connector offsets or required history state is a recovery event and
must not result in an uncontrolled snapshot or replay.

## Observability

Production activation requires monitoring for:

- connector state and task failure;
- source-to-Kafka capture lag;
- heartbeat freshness;
- replication slot activity;
- retained WAL growth;
- connector offset progress;
- Kafka consumer progress;
- Raw persistence failures.

Alert thresholds are environment-specific and belong in deployment/IaC
configuration.

## Failure handling

CDC Raw records may contain restricted source identity.

CDC processing must therefore not copy raw keys, before images, after images or
source payloads into ordinary logs or unrestricted failure topics.

A production DLQ/quarantine design must preserve the privacy classification of
the source and must retain enough non-sensitive metadata for operational
diagnosis.

The governed Raw consumer remains responsible for the durable Raw-before-offset
commit boundary.

## Recovery

Production onboarding requires a tested recovery/runbook covering:

- connector restart;
- task failure;
- replication-slot interruption;
- WAL retention pressure;
- offset loss or corruption;
- controlled snapshot policy;
- schema evolution;
- downstream replay/idempotency;
- Raw reconciliation.

No connector is activated merely by the presence of this directory.
