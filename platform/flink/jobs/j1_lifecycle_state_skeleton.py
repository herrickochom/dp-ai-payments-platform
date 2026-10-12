"""J1 lifecycle-state skeleton for the STR-002B/B2 streaming route.

This is not the canonical J1 state machine (that is STR-003). It consumes
the existing synthetic technical-event topics, decodes the Confluent
Schema Registry Avro wire format, keys by the canonical correlation_id,
and maintains minimal per-correlation-id state for verification.

The Avro decode happens in a map function, not in the Kafka source
deserializer, because PyFlink 1.18 does not expose a clean
KafkaRecordDeserializationSchema subclass for Python-side custom code.
The Kafka source reads raw bytes as ISO-8859-1 strings (byte-safe), and
the map function converts them to bytes, strips the 5-byte Confluent
wire header, and decodes the Avro payload via Schema Registry.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import (
    KeyedProcessFunction,
    RuntimeContext,
    StreamExecutionEnvironment,
)
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.metrics import Counter

LOG = logging.getLogger("flink.j1")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP = os.environ.get("J1_GROUP_ID", "flink-lifecycle-consumer")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

INPUT_TOPICS = [
    os.environ.get("J1_INPUT_TOPIC_1", "icmn.pmn.pain001"),
    os.environ.get("J1_INPUT_TOPIC_2", "cpo.plm.pain002"),
]


class AvroDecoder:
    """Wraps the Confluent Avro deserializer for use inside a Flink map.

    The raw Kafka value is bytes with a 5-byte Confluent Schema Registry
    wire header (magic byte + schema ID). The AvroDeserializer consumes
    this directly; no manual header stripping is needed.
    """

    def __init__(self) -> None:
        client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        self._deserializer = AvroDeserializer(client)

    def decode(self, raw_text: str):
        """Decode a raw Kafka value string to a Python dict, or None."""
        try:
            raw_bytes = raw_text.encode("iso-8859-1")
            value = self._deserializer(raw_bytes, None)
            return value
        except Exception as exc:  # noqa: BLE001
            LOG.warning("avro decode failed: %s", exc)
            return None


# Module-level decoder. PyFlink runs the map function in the TaskManager
# process; the decoder holds an HTTP client to Schema Registry.
_DECODER: AvroDecoder | None = None


def get_decoder() -> AvroDecoder:
    global _DECODER
    if _DECODER is None:
        _DECODER = AvroDecoder()
    return _DECODER


def decode_event(raw_text: str):
    """Map function: raw Kafka value string -> decoded dict or None."""
    return get_decoder().decode(raw_text)


class LifecycleSkeleton(KeyedProcessFunction):
    """Maintain last-seen stage, status, and timestamp per correlation_id."""

    def __init__(self) -> None:
        self._state = None
        self._events_seen: Counter | None = None
        self._state_records: Counter | None = None
        self._missing_key: Counter | None = None

    def open(self, runtime_context: RuntimeContext) -> None:
        descriptor = ValueStateDescriptor("last_seen", Types.PICKLED_BYTE_ARRAY())
        self._state = runtime_context.get_state(descriptor)
        metrics = runtime_context.get_metrics_group()
        self._events_seen = metrics.counter("flink_j1_events_seen_total")
        self._state_records = metrics.counter("flink_j1_state_records_total")
        self._missing_key = metrics.counter("flink_j1_events_missing_key_total")

    def process_element(self, value, ctx: KeyedProcessFunction.Context):  # type: ignore[override]
        if not isinstance(value, dict):
            LOG.warning("non-dict event received; skipping")
            return

        stage = value.get("technical_stage") or value.get("TechnicalStage") or ""
        status = value.get("technical_status") or value.get("TechnicalStatus") or ""
        occurred_at = value.get("event_timestamp") or value.get("EventTimestamp") or ""
        correlation_id = value.get("correlation_id") or value.get("CorrelationId")

        # correlation_id must never fall back to "unknown".
        # See STR-002A-DEV section 5.2.
        if not correlation_id or correlation_id == "unknown":
            if self._missing_key is not None:
                self._missing_key.inc()
            LOG.warning(
                "event without correlation_id: stage=%s status=%s",
                stage,
                status,
            )
            return

        previous = self._state.value()
        self._state.update(
            (
                f'{{"last_stage": "{stage}", '
                f'"last_status": "{status}", '
                f'"last_seen_at": "{occurred_at}", '
                f'"updated_at": "{datetime.now(timezone.utc).isoformat()}"}}'
            ).encode("utf-8")
        )

        if self._events_seen is not None:
            self._events_seen.inc()
        if previous is None and self._state_records is not None:
            self._state_records.inc()

        LOG.info(
            "correlation_id=%s stage=%s status=%s occurred_at=%s previous=%s",
            correlation_id,
            stage,
            status,
            occurred_at,
            "yes" if previous else "no",
        )


def build_source() -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
    #    .set_topics(",".join(INPUT_TOPICS))
        .set_topics(*INPUT_TOPICS)
        .set_group_id(KAFKA_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        # Kafka values are Avro bytes with a Confluent header. Reading them
        # as ISO-8859-1 strings preserves all byte values; the map function
        # re-encodes to bytes before Avro decoding.
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(30_000)

    stream = env.from_source(
        build_source(),
        WatermarkStrategy.no_watermarks(),
        "kafka-lifecycle-source",
    )

    # Decode Avro. The map function returns a dict or None. None means
    # decode failed; it is filtered out before keying.
    decoded = stream.map(decode_event, output_type=Types.PICKLED_BYTE_ARRAY())
    filtered = decoded.filter(lambda x: x is not None)

    # Re-key by the canonical correlation_id. The Kafka record key is
    # LOAN-* (business reference); correlation_id is a UUID. See
    # STR-002A-DEV section 5.4.
    def extract_key(event) -> str:
        if not isinstance(event, dict):
            return "__invalid__"
        cid = event.get("correlation_id") or event.get("CorrelationId")
        if not cid or cid == "unknown":
            return "__missing_key__"
        return str(cid)

    keyed = filtered.key_by(extract_key, key_type=Types.STRING())
    keyed.process(LifecycleSkeleton(), output_type=Types.STRING())

    env.execute("j1-lifecycle-state-skeleton")


if __name__ == "__main__":
    main()