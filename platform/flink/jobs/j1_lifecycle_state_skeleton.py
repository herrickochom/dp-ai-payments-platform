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
  4. Emits one JSON transition record per distinct event, carrying the
     envelope event_type (PMN or PLM), previous state, and current state.

Known limits (this is a skeleton, not the state machine):
  - Dedup state (seen event_ids per correlation_id) is bounded by a 24-hour
    TTL, configurable via J1_DEDUP_TTL_HOURS. The value is provisional until
    STR-002A-PROD confirms the maximum lifecycle duration.
  - Output goes to stdout via print(). Writing to payments.lifecycle.state.v1
    is B2 item 3 and needs the STR-003 output contract.
  - The input is the raw-route topics, read through a read-only consumer group.
    This is a development fixture, not the lifecycle input contract.
  - updated_at uses wall-clock time, so a replay produces different values for
    that field. Event-time semantics are STR-003 scope.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

from pyflink.common import Types
from pyflink.common.time import Time
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
from pyflink.table import StreamTableEnvironment

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

# Dedup state TTL.
#
# The dedup state is keyed by correlation_id and, within each key, holds the
# set of event_ids already applied. Without a TTL this set grows without
# bound for the life of the job. The TTL bounds it to event_ids seen within
# the window.
#
# Value: 24 hours. Chosen to exceed any plausible payment lifecycle duration
# in the synthetic data, and conservatively in production. Provisional:
# STR-002A-PROD must confirm the real maximum lifecycle duration and the
# value is revised against that. Exposed as an environment variable so the
# revision does not require a code change.
#
# Update type: OnCreateAndWrite. A dedup entry is written once, when the
# event_id is first seen, and never updated. OnCreateAndWrite matches that
# and avoids refreshing the clock on read.
#
# Visibility: NeverReturnExpired. An expired event_id must not satisfy a
# dedup check; if it did, a legitimate re-emission after the TTL window
# would be silently treated as a duplicate and dropped.
J1_DEDUP_TTL_HOURS = int(_env("J1_DEDUP_TTL_HOURS", "24"))

INPUT_TOPICS = [
    _env("J1_INPUT_TOPIC_1", "icmn.pmn.pain001"),
    _env("J1_INPUT_TOPIC_2", "cpo.plm.pain002"),
]

TABLE_NAME = "technical_events"
UNKNOWN_KEY = "unknown"

# Envelope columns only. These must exist in the registered schema, which
# was verified for icmn.pmn.pain001 (schema id 1). Fields that exist only
# inside parsed_event_data (such as correlation_id) are NOT columns.
ENVELOPE_COLUMNS: list[tuple[str, str]] = [
    ("event_id", "STRING"),
    ("message_id", "STRING"),
    ("event_type", "STRING"),
    ("event_family", "STRING"),
    ("source_system", "STRING"),
    ("parsed_event_data", "STRING"),
]

# Projection order must match ENVELOPE_COLUMNS, because ParseEnvelope indexes
# the row by position.
PROJECTION_SQL = (
    "SELECT event_id, message_id, event_type, source_system, parsed_event_data "
    f"FROM {TABLE_NAME}"
)


def build_source_ddl() -> str:
    """Return the CREATE TABLE statement for the Kafka source.

    'topic' takes a semicolon-separated list. The 'avro-confluent' format
    fetches each message's writer schema from Schema Registry by schema ID.
    """
    columns = ",\n  ".join(f"`{name}` {sql_type}" for name, sql_type in ENVELOPE_COLUMNS)
    return f"""
CREATE TABLE {TABLE_NAME} (
  {columns}
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


# ------------------------------------------------------------------------------
# Step 1: parse the technical event out of the envelope
# ------------------------------------------------------------------------------
class ParseEnvelope(ProcessFunction):
    """Extract the technical event from parsed_event_data.

    Emits a normalised JSON string for each valid event. Rows with a missing
    event_id, invalid JSON, or no usable correlation_id are counted and dropped.
    """

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
        event_id, message_id, event_type, source_system, parsed_event_data = (
            row[0], row[1], row[2], row[3], row[4],
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
        }
        yield json.dumps(normalised, sort_keys=True)


def correlation_key(normalised_json: str) -> str:
    return json.loads(normalised_json)["correlation_id"]


# ------------------------------------------------------------------------------
# Step 2: lifecycle state per correlation_id, with event_id deduplication
# ------------------------------------------------------------------------------
class LifecycleSkeleton(KeyedProcessFunction):
    """Track last-seen stage and status per correlation_id.

    Each correlation_id keeps a MapState of event_ids it has already applied.
    A repeated event_id is counted as a duplicate and produces no output and no
    state change. A new event_id updates the last-seen state and emits one
    transition record.

    The dedup MapState carries a TTL (J1_DEDUP_TTL_HOURS, default 24h) so that
    the set of seen event_ids does not grow without bound. The last_seen
    ValueState is intentionally left without a TTL: it is bounded by the number
    of live correlation IDs, not by event count.
    """

    def __init__(self) -> None:
        self._state = None
        self._seen_event_ids = None
        self._events_seen: Counter | None = None
        self._state_records: Counter | None = None
        self._duplicates: Counter | None = None

    def open(self, runtime_context: RuntimeContext) -> None:
        self._state = runtime_context.get_state(
            ValueStateDescriptor("last_seen", Types.STRING())
        )

        # TTL applies to the dedup MapState only. See the module-level
        # comment on J1_DEDUP_TTL_HOURS for the value, rationale, and the
        # provisional status of the value.
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

    def process_element(self, value, ctx):  # type: ignore[override]
        event = json.loads(value)
        event_id = event["event_id"]

        if self._seen_event_ids.contains(event_id):
            self._duplicates.inc()
            return
        self._seen_event_ids.put(event_id, True)

        raw_previous = self._state.value()
        previous = json.loads(raw_previous) if raw_previous else None

        current = {
            "last_stage": event["technical_stage"],
            "last_status": event["technical_status"],
            "last_seen_at": event["event_timestamp"],
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._state.update(json.dumps(current, sort_keys=True))

        self._events_seen.inc()
        if previous is None:
            self._state_records.inc()

        yield json.dumps(
            {
                "correlation_id": event["correlation_id"],
                "event_type": event["event_type"],
                "previous": previous,
                "current": current,
            },
            sort_keys=True,
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

    envelopes = t_env.to_data_stream(t_env.sql_query(PROJECTION_SQL))

    transitions = (
        envelopes
        .process(ParseEnvelope(), output_type=Types.STRING())
        .key_by(correlation_key, key_type=Types.STRING())
        .process(LifecycleSkeleton(), output_type=Types.STRING())
    )
    transitions.print()

    env.execute(JOB_NAME)


if __name__ == "__main__":
    main()