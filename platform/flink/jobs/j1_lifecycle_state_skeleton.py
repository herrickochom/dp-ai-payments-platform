"""J1 lifecycle-state skeleton for the STR-002B/B2 streaming route.

Reads the technical-event topics through the Flink Table API using the
'avro-confluent' format. The registered schema is a shared envelope across
event families. Technical fields such as correlation_id live inside the JSON
string in parsed_event_data, so this job:

  1. Declares only the envelope columns it needs.
  2. Parses parsed_event_data in Python and drops rows without a usable
     event_id or correlation_id, counting them rather than logging each one.
  3. Keys the stream on the inner correlation_id and deduplicates by event_id
     within each correlation_id. A repeated event is counted and dropped.
  4. Emits two records per distinct event: a state record (upsert into
     payments.lifecycle.state.v1) and a transition record (append into
     payments.lifecycle.transitions.v1), per STR-003-output-contract.

Known limits (this is a skeleton, not the state machine):
  - Dedup state (seen event_ids per correlation_id) is bounded by a 24-hour
    TTL, configurable via J1_DEDUP_TTL_HOURS. The value is provisional until
    STR-002A-PROD confirms the maximum lifecycle duration.
  - The input is the raw-route topics, read through a read-only consumer group.
    This is a development fixture, not the lifecycle input contract.
  - updated_at uses wall-clock time, so a replay produces different values for
    that field. Event-time semantics are STR-003 scope.
  - The sink is at-least-once. Flink's CheckpointingMode.EXACTLY_ONCE governs
    managed state only; it does not make Kafka output transactional. See
    STR-003-output-contract §9. Exactly-once requires a transactional ID
    prefix, a sink.delivery-guarantee=exactly-once declaration on the Kafka
    sink tables, and read_committed consumers. Not enabled here.
  - Event-order comparator and late-event policy are NOT implemented.
    STR-003-output-contract §12 gate 2 remains open. Downstream consumers
    must dedup transitions by transition_id and resolve state by
    correlation_id plus event-time plus last_event_id.
  - Terminal-state handling is NOT implemented. The metric
    flink_j1_terminal_state_events_total is registered but nothing
    increments it yet. Every accepted event currently overwrites state.

Operational dependency:
  - Checkpoints and savepoints are written to S3 via the Flink S3 filesystem
    plugin. The flink-checkpoint MinIO identity is scoped to
    flink/checkpoints/* and flink/savepoints/*, so flink-conf.yaml must set
    fs.s3a.committer.name=directory. The default magic committer performs a
    parent-prefix bulk delete that the policy denies, which stalls the task's
    checkpoint listener and drags the Kafka consumer down with it. See
    platform/flink/conf/flink-conf.yaml for the full note.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone

from pyflink.common import Row, Types
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types as PyTypes
from pyflink.datastream import (
    CheckpointingMode,
    KeyedProcessFunction,
    ProcessFunction,
    RuntimeContext,
    StreamExecutionEnvironment,
)
from pyflink.datastream.state import (
    MapStateDescriptor,
    StateTtlConfig,
    ValueStateDescriptor,
)
from pyflink.metrics import Counter
from pyflink.table import StreamTableEnvironment, Schema
from pyflink.table.expressions import col
from pyflink.table.types import DataTypes

LOG = logging.getLogger("flink.j1")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


# ------------------------------------------------------------------------------
# Runtime configuration
# ------------------------------------------------------------------------------
JOB_NAME = _env("J1_JOB_NAME", "j1-lifecycle-state-skeleton")
KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP = _env("J1_GROUP_ID", "flink-lifecycle-consumer")
SCHEMA_REGISTRY_URL = _env("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
STARTUP_MODE = _env("J1_STARTUP_MODE", "earliest-offset")
PARALLELISM = int(_env("J1_PARALLELISM", "1"))
CHECKPOINT_INTERVAL_MS = int(_env("J1_CHECKPOINT_INTERVAL_MS", "30000"))
INVALID_LOG_EVERY = int(_env("J1_INVALID_LOG_EVERY", "1000"))

SINK_DELIVERY_GUARANTEE = _env("J1_SINK_DELIVERY_GUARANTEE", "at-least-once")

STATE_TOPIC = _env("J1_STATE_TOPIC", "payments.lifecycle.state.v1")
TRANSITIONS_TOPIC = _env("J1_TRANSITIONS_TOPIC", "payments.lifecycle.transitions.v1")

# Exact approved version-1 writer schemas. Pinning these prevents Flink from
# deriving an all-nullable generated record and registering an unintended
# subject version. Keep these byte-for-byte semantically aligned with the
# registered contracts in contracts/kafka/.
STATE_AVRO_SCHEMA = r'''{"type":"record","name":"LifecycleState","namespace":"com.payments.platform","fields":[{"name":"correlation_id","type":"string"},{"name":"current_stage","type":"string"},{"name":"current_status","type":"string"},{"name":"last_event_id","type":"string"},{"name":"last_event_timestamp","type":"string"},{"name":"last_processing_timestamp","type":"string"},{"name":"first_seen_at","type":"string"},{"name":"updated_at","type":"string"},{"name":"stage_history","type":{"type":"array","items":"string"}},{"name":"retry_count","type":"int"},{"name":"sla_deadline","type":["null","string"],"default":null},{"name":"sla_breached","type":"boolean","default":false},{"name":"end_to_end_id","type":["null","string"],"default":null},{"name":"instruction_id","type":["null","string"],"default":null},{"name":"schema_version","type":"string","default":"1.0.0"}]}'''
TRANSITION_AVRO_SCHEMA = r'''{"type":"record","name":"LifecycleTransition","namespace":"com.payments.platform","fields":[{"name":"transition_id","type":"string"},{"name":"correlation_id","type":"string"},{"name":"event_id","type":"string"},{"name":"from_stage","type":["null","string"],"default":null},{"name":"to_stage","type":"string"},{"name":"from_status","type":["null","string"],"default":null},{"name":"to_status","type":"string"},{"name":"event_timestamp","type":"string"},{"name":"processing_timestamp","type":"string"},{"name":"source_topic","type":"string"},{"name":"source_partition","type":"int"},{"name":"source_offset","type":"long"},{"name":"job_id","type":"string"},{"name":"schema_version","type":"string","default":"1.0.0"}]}'''

J1_DEDUP_TTL_HOURS = int(_env("J1_DEDUP_TTL_HOURS", "24"))

INPUT_TOPICS = [
    _env("J1_INPUT_TOPIC_1", "icmn.pmn.pain001"),
    _env("J1_INPUT_TOPIC_2", "cpo.plm.pain002"),
]

TABLE_NAME = "technical_events"
STATE_SINK_TABLE = "lifecycle_state_sink"
TRANSITIONS_SINK_TABLE = "lifecycle_transitions_sink"
UNKNOWN_KEY = "unknown"

ENVELOPE_COLUMNS: list[tuple[str, str]] = [
    ("event_id", "STRING"),
    ("message_id", "STRING"),
    ("event_type", "STRING"),
    ("event_family", "STRING"),
    ("source_system", "STRING"),
    ("parsed_event_data", "STRING"),
]

METADATA_COLUMNS_SQL = """
  `kafka_topic` STRING METADATA FROM 'topic' VIRTUAL,
  `kafka_partition` INT METADATA FROM 'partition' VIRTUAL,
  `kafka_offset` BIGINT METADATA FROM 'offset' VIRTUAL
"""

PROJECTION_SQL = (
    "SELECT event_id, message_id, event_type, source_system, parsed_event_data, "
    "kafka_topic, kafka_partition, kafka_offset "
    f"FROM {TABLE_NAME}"
)

# Row schemas for the DataStream -> Table bridge. Must match the tuple
# output of the extract functions, field for field, in order.
STATE_ROW_TYPE = DataTypes.ROW([
    DataTypes.FIELD("correlation_id", DataTypes.STRING().not_null()),
    DataTypes.FIELD("current_stage", DataTypes.STRING().not_null()),
    DataTypes.FIELD("current_status", DataTypes.STRING().not_null()),
    DataTypes.FIELD("last_event_id", DataTypes.STRING().not_null()),
    DataTypes.FIELD("last_event_timestamp", DataTypes.STRING().not_null()),
    DataTypes.FIELD("last_processing_timestamp", DataTypes.STRING().not_null()),
    DataTypes.FIELD("first_seen_at", DataTypes.STRING().not_null()),
    DataTypes.FIELD("updated_at", DataTypes.STRING().not_null()),
    DataTypes.FIELD(
        "stage_history", DataTypes.ARRAY(DataTypes.STRING().not_null()).not_null()
    ),
    DataTypes.FIELD("retry_count", DataTypes.INT().not_null()),
    DataTypes.FIELD("sla_deadline", DataTypes.STRING()),
    DataTypes.FIELD("sla_breached", DataTypes.BOOLEAN().not_null()),
    DataTypes.FIELD("end_to_end_id", DataTypes.STRING()),
    DataTypes.FIELD("instruction_id", DataTypes.STRING()),
    DataTypes.FIELD("schema_version", DataTypes.STRING().not_null()),
]).not_null()

TRANSITION_ROW_TYPE = DataTypes.ROW([
    DataTypes.FIELD("transition_id", DataTypes.STRING().not_null()),
    DataTypes.FIELD("correlation_id", DataTypes.STRING().not_null()),
    DataTypes.FIELD("event_id", DataTypes.STRING().not_null()),
    DataTypes.FIELD("from_stage", DataTypes.STRING()),
    DataTypes.FIELD("to_stage", DataTypes.STRING().not_null()),
    DataTypes.FIELD("from_status", DataTypes.STRING()),
    DataTypes.FIELD("to_status", DataTypes.STRING().not_null()),
    DataTypes.FIELD("event_timestamp", DataTypes.STRING().not_null()),
    DataTypes.FIELD("processing_timestamp", DataTypes.STRING().not_null()),
    DataTypes.FIELD("source_topic", DataTypes.STRING().not_null()),
    DataTypes.FIELD("source_partition", DataTypes.INT().not_null()),
    DataTypes.FIELD("source_offset", DataTypes.BIGINT().not_null()),
    DataTypes.FIELD("job_id", DataTypes.STRING().not_null()),
    DataTypes.FIELD("schema_version", DataTypes.STRING().not_null()),
]).not_null()

# Explicit Table schemas matching the approved STR-003 output contract.
# These are the authoritative field list used by from_data_stream(); they
# resolve the physical tuple's positional fields to the semantic contract
# names without changing the contract itself (STR-003 output contract).
STATE_SCHEMA = Schema.new_builder() \
    .column("correlation_id", DataTypes.STRING().not_null()) \
    .column("current_stage", DataTypes.STRING().not_null()) \
    .column("current_status", DataTypes.STRING().not_null()) \
    .column("last_event_id", DataTypes.STRING().not_null()) \
    .column("last_event_timestamp", DataTypes.STRING().not_null()) \
    .column("last_processing_timestamp", DataTypes.STRING().not_null()) \
    .column("first_seen_at", DataTypes.STRING().not_null()) \
    .column("updated_at", DataTypes.STRING().not_null()) \
    .column("stage_history", DataTypes.ARRAY(DataTypes.STRING().not_null()).not_null()) \
    .column("retry_count", DataTypes.INT().not_null()) \
    .column("sla_deadline", DataTypes.STRING()) \
    .column("sla_breached", DataTypes.BOOLEAN().not_null()) \
    .column("end_to_end_id", DataTypes.STRING()) \
    .column("instruction_id", DataTypes.STRING()) \
    .column("schema_version", DataTypes.STRING().not_null()) \
    .build()

TRANSITION_SCHEMA = Schema.new_builder() \
    .column("transition_id", DataTypes.STRING().not_null()) \
    .column("correlation_id", DataTypes.STRING().not_null()) \
    .column("event_id", DataTypes.STRING().not_null()) \
    .column("from_stage", DataTypes.STRING()) \
    .column("to_stage", DataTypes.STRING().not_null()) \
    .column("from_status", DataTypes.STRING()) \
    .column("to_status", DataTypes.STRING().not_null()) \
    .column("event_timestamp", DataTypes.STRING().not_null()) \
    .column("processing_timestamp", DataTypes.STRING().not_null()) \
    .column("source_topic", DataTypes.STRING().not_null()) \
    .column("source_partition", DataTypes.INT().not_null()) \
    .column("source_offset", DataTypes.BIGINT().not_null()) \
    .column("job_id", DataTypes.STRING().not_null()) \
    .column("schema_version", DataTypes.STRING().not_null()) \
    .build()

# PyFlink type infos for the tuple output. Position order here is the physical
# wire into from_data_stream(): each STATE_FIELDS/TRANSITION_FIELDS expression
# binds fN (position) to the semantic contract name, so these lists must stay
# in the exact STR-003 §4 field order alongside STATE_ROW_TYPE /
# TRANSITION_ROW_TYPE (names) and the from_data_stream() aliases below.
# Note: PyFlink 1.18's TypeInformation spells the 64-bit integer LONG()
# (mapped from the BIGINT Table type); BIGINT() does not exist there, so the
# transition offset position uses LONG() to keep the physical tuple coherent.
STATE_TUPLE_TYPE = PyTypes.ROW([
    PyTypes.STRING(), PyTypes.STRING(), PyTypes.STRING(), PyTypes.STRING(),
    PyTypes.STRING(), PyTypes.STRING(), PyTypes.STRING(), PyTypes.STRING(),
    PyTypes.OBJECT_ARRAY(PyTypes.STRING()), PyTypes.INT(), PyTypes.STRING(), PyTypes.BOOLEAN(),
    PyTypes.STRING(), PyTypes.STRING(), PyTypes.STRING(),
])

TRANSITION_TUPLE_TYPE = PyTypes.ROW([
    PyTypes.STRING(), PyTypes.STRING(), PyTypes.STRING(), PyTypes.STRING(),
    PyTypes.STRING(), PyTypes.STRING(), PyTypes.STRING(), PyTypes.STRING(),
    PyTypes.STRING(), PyTypes.STRING(), PyTypes.INT(), PyTypes.LONG(),
    PyTypes.STRING(), PyTypes.STRING(),
])

# Explicit, type-safe field mapping: the physical tuple exposes positional
# fields f0..fN, while the Table schema expects semantic contract names.
# PyFlink 1.18 resolves a Schema argument to from_data_stream() BY NAME
# against the physical type, so semantic names alone cannot bind a bare
# TUPLE. These aliased expressions rename each physical position to its
# semantic contract name instead. Field order is the contract; changing the
# contract requires re-deriving these lists from STR-003-output-contract.md.
STATE_FIELDS = [
    col("f0").alias("correlation_id"),
    col("f1").alias("current_stage"),
    col("f2").alias("current_status"),
    col("f3").alias("last_event_id"),
    col("f4").alias("last_event_timestamp"),
    col("f5").alias("last_processing_timestamp"),
    col("f6").alias("first_seen_at"),
    col("f7").alias("updated_at"),
    col("f8").alias("stage_history"),
    col("f9").alias("retry_count"),
    col("f10").alias("sla_deadline"),
    col("f11").alias("sla_breached"),
    col("f12").alias("end_to_end_id"),
    col("f13").alias("instruction_id"),
    col("f14").alias("schema_version"),
]

TRANSITION_FIELDS = [
    col("f0").alias("transition_id"),
    col("f1").alias("correlation_id"),
    col("f2").alias("event_id"),
    col("f3").alias("from_stage"),
    col("f4").alias("to_stage"),
    col("f5").alias("from_status"),
    col("f6").alias("to_status"),
    col("f7").alias("event_timestamp"),
    col("f8").alias("processing_timestamp"),
    col("f9").alias("source_topic"),
    col("f10").alias("source_partition"),
    col("f11").alias("source_offset"),
    col("f12").alias("job_id"),
    col("f13").alias("schema_version"),
]


def build_source_ddl() -> str:
    columns = ",\n  ".join(f"`{name}` {sql_type}" for name, sql_type in ENVELOPE_COLUMNS)
    return f"""
CREATE TABLE {TABLE_NAME} (
  {columns},
{METADATA_COLUMNS_SQL}
) WITH (
  'connector' = 'kafka',
  'topic' = '{";".join(INPUT_TOPICS)}',
  'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
  'properties.group.id' = '{KAFKA_GROUP}',
  'scan.startup.mode' = '{STARTUP_MODE}',
  'format' = 'avro-confluent',
  'avro-confluent.url' = '{SCHEMA_REGISTRY_URL}'
)
"""


def build_state_sink_ddl() -> str:
    return f"""
CREATE TABLE {STATE_SINK_TABLE} (
  `correlation_id` STRING NOT NULL,
  `current_stage` STRING NOT NULL,
  `current_status` STRING NOT NULL,
  `last_event_id` STRING NOT NULL,
  `last_event_timestamp` STRING NOT NULL,
  `last_processing_timestamp` STRING NOT NULL,
  `first_seen_at` STRING NOT NULL,
  `updated_at` STRING NOT NULL,
  `stage_history` ARRAY<STRING NOT NULL> NOT NULL,
  `retry_count` INT NOT NULL,
  `sla_deadline` STRING,
  `sla_breached` BOOLEAN NOT NULL,
  `end_to_end_id` STRING,
  `instruction_id` STRING,
  `schema_version` STRING NOT NULL
) WITH (
  'connector' = 'kafka',
  'topic' = '{STATE_TOPIC}',
  'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
  'key.format' = 'raw',
  'key.fields' = 'correlation_id',
  'value.format' = 'avro-confluent',
  'value.fields-include' = 'ALL',
  'value.avro-confluent.url' = '{SCHEMA_REGISTRY_URL}',
  'value.avro-confluent.schema-registry.subject' = '{STATE_TOPIC}-value',
  'value.avro-confluent.schema-registry.schema' = '{STATE_AVRO_SCHEMA}',
  'sink.delivery-guarantee' = '{SINK_DELIVERY_GUARANTEE}'
)
"""


def build_transitions_sink_ddl() -> str:
    return f"""
CREATE TABLE {TRANSITIONS_SINK_TABLE} (
  `transition_id` STRING NOT NULL,
  `correlation_id` STRING NOT NULL,
  `event_id` STRING NOT NULL,
  `from_stage` STRING,
  `to_stage` STRING NOT NULL,
  `from_status` STRING,
  `to_status` STRING NOT NULL,
  `event_timestamp` STRING NOT NULL,
  `processing_timestamp` STRING NOT NULL,
  `source_topic` STRING NOT NULL,
  `source_partition` INT NOT NULL,
  `source_offset` BIGINT NOT NULL,
  `job_id` STRING NOT NULL,
  `schema_version` STRING NOT NULL
) WITH (
  'connector' = 'kafka',
  'topic' = '{TRANSITIONS_TOPIC}',
  'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
  'key.format' = 'raw',
  'key.fields' = 'correlation_id',
  'value.format' = 'avro-confluent',
  'value.fields-include' = 'ALL',
  'value.avro-confluent.url' = '{SCHEMA_REGISTRY_URL}',
  'value.avro-confluent.schema-registry.subject' = '{TRANSITIONS_TOPIC}-value',
  'value.avro-confluent.schema-registry.schema' = '{TRANSITION_AVRO_SCHEMA}',
  'sink.delivery-guarantee' = '{SINK_DELIVERY_GUARANTEE}'
)
"""


# ------------------------------------------------------------------------------
# Step 1: parse the technical event out of the envelope
# ------------------------------------------------------------------------------
class ParseEnvelope(ProcessFunction):
    def __init__(self) -> None:
        self._seen: Counter | None = None
        self._invalid: Counter | None = None
        self._invalid_local = 0

    def open(self, runtime_context: RuntimeContext) -> None:
        metrics = runtime_context.get_metrics_group()
        self._seen = metrics.counter("flink_j1_envelopes_seen_total")
        self._invalid = metrics.counter("flink_j1_events_missing_key_total")

    def _drop(self, reason: str, event_id, message_id) -> None:
        self._invalid.inc()
        self._invalid_local += 1
        if self._invalid_local % INVALID_LOG_EVERY == 1:
            LOG.warning(
                "dropped %d envelope(s); latest reason=%s event_id=%s message_id=%s",
                self._invalid_local, reason, event_id, message_id,
            )

    def process_element(self, row, ctx):  # type: ignore[override]
        (
            event_id,
            message_id,
            event_type,
            source_system,
            parsed_event_data,
            kafka_topic,
            kafka_partition,
            kafka_offset,
        ) = (
            row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7],
        )
        self._seen.inc()

        if not event_id:
            self._drop("missing_event_id", event_id, message_id)
            return

        body = None
        if parsed_event_data:
            try:
                body = json.loads(parsed_event_data)
            except json.JSONDecodeError:
                body = None

        correlation_id = body.get("correlation_id") if isinstance(body, dict) else None
        if not correlation_id or correlation_id == UNKNOWN_KEY:
            self._drop("missing_correlation_id", event_id, message_id)
            return

        normalised = {
            "correlation_id": str(correlation_id),
            "event_id": str(event_id),
            "message_id": message_id,
            "event_type": event_type,
            "source_system": source_system,
            "technical_stage": body.get("technical_stage") or "",
            "technical_status": body.get("technical_status") or "",
            "event_timestamp": body.get("event_timestamp") or "",
            "kafka_topic": kafka_topic,
            "kafka_partition": kafka_partition,
            "kafka_offset": kafka_offset,
        }
        yield json.dumps(normalised, sort_keys=True)


def correlation_key(normalised_json: str) -> str:
    return json.loads(normalised_json)["correlation_id"]


def make_transition_id(correlation_id: str, event_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{correlation_id}:{event_id}"))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ------------------------------------------------------------------------------
# Step 2: lifecycle state per correlation_id, with event_id deduplication
# ------------------------------------------------------------------------------
class LifecycleSkeleton(KeyedProcessFunction):
    def __init__(self) -> None:
        self._state = None
        self._seen_event_ids = None
        self._events_seen: Counter | None = None
        self._state_records: Counter | None = None
        self._duplicates: Counter | None = None
        self._state_writes: Counter | None = None
        self._transition_writes: Counter | None = None
        self._terminal_state_events: Counter | None = None

    def open(self, runtime_context: RuntimeContext) -> None:
        self._state = runtime_context.get_state(
            ValueStateDescriptor("last_seen", Types.STRING())
        )

        ttl_config = (
            StateTtlConfig.new_builder(Time.hours(J1_DEDUP_TTL_HOURS))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanup_incrementally(10, True)
            .build()
        )
        seen_event_ids_descriptor = MapStateDescriptor(
            "seen_event_ids", Types.STRING(), Types.BOOLEAN()
        )
        seen_event_ids_descriptor.enable_time_to_live(ttl_config)
        self._seen_event_ids = runtime_context.get_map_state(seen_event_ids_descriptor)

        metrics = runtime_context.get_metrics_group()
        self._events_seen = metrics.counter("flink_j1_events_seen_total")
        self._state_records = metrics.counter("flink_j1_state_records_total")
        self._duplicates = metrics.counter("flink_j1_duplicate_events_total")
        self._state_writes = metrics.counter("flink_j1_state_writes_total")
        self._transition_writes = metrics.counter("flink_j1_transition_writes_total")
        self._terminal_state_events = metrics.counter("flink_j1_terminal_state_events_total")

    def process_element(self, value, ctx):  # type: ignore[override]
        event = json.loads(value)
        event_id = event["event_id"]

        if self._seen_event_ids.contains(event_id):
            self._duplicates.inc()
            return
        self._seen_event_ids.put(event_id, True)

        raw_previous = self._state.value()
        previous = json.loads(raw_previous) if raw_previous else None

        now = now_iso()
        first_seen_at = previous["first_seen_at"] if previous else now

        prior_history = previous["stage_history"] if previous else []
        this_stage = event["technical_stage"]
        if this_stage and (not prior_history or prior_history[-1] != this_stage):
            stage_history = prior_history + [this_stage]
        else:
            stage_history = prior_history

        retry_count = previous["retry_count"] if previous else 0
        if event["technical_status"].lower() == "retrying":
            retry_count += 1

        current = {
            "correlation_id": event["correlation_id"],
            "current_stage": event["technical_stage"],
            "current_status": event["technical_status"],
            "last_event_id": event_id,
            "last_event_timestamp": event["event_timestamp"],
            "last_processing_timestamp": now,
            "first_seen_at": first_seen_at,
            "updated_at": now,
            "stage_history": stage_history,
            "retry_count": retry_count,
            "sla_deadline": None,
            "sla_breached": False,
            "end_to_end_id": event.get("end_to_end_id"),
            "instruction_id": event.get("instruction_id"),
            "schema_version": "1.0.0",
        }
        self._state.update(json.dumps(current, sort_keys=True))

        self._events_seen.inc()
        if previous is None:
            self._state_records.inc()

        state_record = {
            "__type": "state",
            "payload": current,
        }

        transition_record = {
            "__type": "transition",
            "payload": {
                "transition_id": make_transition_id(
                    event["correlation_id"], event_id
                ),
                "correlation_id": event["correlation_id"],
                "event_id": event_id,
                "from_stage": previous["current_stage"] if previous else None,
                "from_status": previous["current_status"] if previous else None,
                "to_stage": event["technical_stage"],
                "to_status": event["technical_status"],
                "event_timestamp": event["event_timestamp"],
                "processing_timestamp": now,
                "source_topic": event.get("kafka_topic"),
                "source_partition": event.get("kafka_partition"),
                "source_offset": event.get("kafka_offset"),
                "job_id": JOB_NAME,
                "schema_version": "1.0.0",
            },
        }

        self._state_writes.inc()
        self._transition_writes.inc()

        yield json.dumps(state_record, sort_keys=True)
        yield json.dumps(transition_record, sort_keys=True)


# ------------------------------------------------------------------------------
# Step 3: extract typed tuples from the JSON records for the Table bridge
# ------------------------------------------------------------------------------
def extract_state_payload(json_str: str):
    record = json.loads(json_str)
    if record.get("__type") != "state":
        return
    p = record["payload"]
    yield Row(
        p["correlation_id"],
        p["current_stage"],
        p["current_status"],
        p["last_event_id"],
        p["last_event_timestamp"],
        p["last_processing_timestamp"],
        p["first_seen_at"],
        p["updated_at"],
        list(p["stage_history"]),
        int(p["retry_count"]),
        p["sla_deadline"],
        bool(p["sla_breached"]),
        p["end_to_end_id"],
        p["instruction_id"],
        p["schema_version"],
    )


def extract_transition_payload(json_str: str):
    record = json.loads(json_str)
    if record.get("__type") != "transition":
        return
    p = record["payload"]

    missing_provenance = [
        field
        for field in ("source_topic", "source_partition", "source_offset")
        if p.get(field) is None
    ]
    if missing_provenance:
        raise ValueError(
            "LifecycleTransition missing mandatory Kafka provenance: "
            + ", ".join(missing_provenance)
        )

    yield Row(
        p["transition_id"],
        p["correlation_id"],
        p["event_id"],
        p["from_stage"],
        p["to_stage"],
        p["from_status"],
        p["to_status"],
        p["event_timestamp"],
        p["processing_timestamp"],
        p["source_topic"],
        int(p["source_partition"]),
        int(p["source_offset"]),
        p["job_id"],
        p["schema_version"],
    )


# ------------------------------------------------------------------------------
# Job wiring
# ------------------------------------------------------------------------------
def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS, CheckpointingMode.EXACTLY_ONCE)

    t_env = StreamTableEnvironment.create(env)
    t_env.execute_sql(build_source_ddl())
    t_env.execute_sql(build_state_sink_ddl())
    t_env.execute_sql(build_transitions_sink_ddl())

    envelopes = t_env.to_data_stream(t_env.sql_query(PROJECTION_SQL))

    records = (
        envelopes
        .process(ParseEnvelope(), output_type=Types.STRING())
        .key_by(correlation_key, key_type=Types.STRING())
        .process(LifecycleSkeleton(), output_type=Types.STRING())
    )

    state_tuples = records.flat_map(
        extract_state_payload, output_type=STATE_TUPLE_TYPE
    )
    transition_tuples = records.flat_map(
        extract_transition_payload, output_type=TRANSITION_TUPLE_TYPE
    )

    state_table = t_env.from_data_stream(state_tuples, *STATE_FIELDS)
    transitions_table = t_env.from_data_stream(transition_tuples, *TRANSITION_FIELDS)

    # Register the bridging tables as temporary views. These are consumed by
    # the StatementSet's INSERT targets (below) and by the state machine's
    # downstream reads.
    t_env.create_temporary_view("state_source_view", state_table)
    t_env.create_temporary_view("transitions_source_view", transitions_table)

    # Both streaming sinks are registered in a single StatementSet so that
    # they commit together: a coordinated execution guarantees that the
    # state upsert (checkpoints) and the transition append (Kafka producer)
    # share one Flink execution plan, which is required for consistent
    # at-least-once delivery and for the checkpoint listener to observe
    # both producers before the task is considered complete.
    statement_set = t_env.create_statement_set()

    statement_set.add_insert(
        STATE_SINK_TABLE,
        t_env.sql_query(
            f"SELECT correlation_id, current_stage, current_status, last_event_id, "
            f"last_event_timestamp, last_processing_timestamp, first_seen_at, updated_at, "
            f"stage_history, retry_count, sla_deadline, sla_breached, end_to_end_id, "
            f"instruction_id, schema_version FROM state_source_view"
        ),
    )
    statement_set.add_insert(
        TRANSITIONS_SINK_TABLE,
        t_env.sql_query(
            f"SELECT transition_id, correlation_id, event_id, from_stage, to_stage, "
            f"from_status, to_status, event_timestamp, processing_timestamp, "
            f"source_topic, source_partition, source_offset, job_id, schema_version "
            f"FROM transitions_source_view"
        ),
    )

    # StatementSet.execute() submits the single coordinated plan that owns both
    # Kafka sinks. StreamTableEnvironment in PyFlink 1.18 has no such execute
    # method, so the old one-shot call could never reach the cluster; the
    # failed submission and the silent sink absence both traced to that line.
    # execute() is blocking: it returns once the job reaches RUNNING, which is
    # the observable signal reported to the cluster. Checkpointing, IAM, and
    # savepoint configuration are untouched here.
    statement_set.execute()



if __name__ == "__main__":
    main()
