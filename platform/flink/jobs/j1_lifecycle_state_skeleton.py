"""J1 lifecycle-state skeleton for the STR-002B/B1 smoke test.

This is not the canonical J1 state machine. It consumes the existing synthetic
technical-event topic and maintains minimal per-correlation-ID state.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import KeyedProcessFunction, RuntimeContext, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.metrics import Counter

LOG = logging.getLogger("flink.j1.skeleton")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP = os.environ.get("J1_GROUP_ID", "flink-j1-skeleton")
INPUT_TOPIC = os.environ.get("J1_INPUT_TOPIC", "icmn.pmn.pain001")


class LifecycleSkeleton(KeyedProcessFunction):
    """Maintain the last-seen stage and timestamp per correlation ID."""

    def __init__(self) -> None:
        self._state = None
        self._events_seen: Counter | None = None
        self._state_records: Counter | None = None

    def open(self, runtime_context: RuntimeContext) -> None:
        descriptor = ValueStateDescriptor("last_seen", Types.PICKLED_BYTE_ARRAY())
        self._state = runtime_context.get_state(descriptor)
        metrics = runtime_context.get_metrics_group()
        self._events_seen = metrics.counter("flink_j1_events_seen_total")
        self._state_records = metrics.counter("flink_j1_state_records_total")

    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        try:
            event = json.loads(value)
        except json.JSONDecodeError:
            LOG.warning("non-json event; skipping. raw=%r", value[:256])
            return

        stage = event.get("stage") or event.get("Stage") or "unknown"
        occurred_at = event.get("occurred_at") or event.get("EventTimestamp") or ""
        previous = self._state.value()
        self._state.update(
            json.dumps(
                {
                    "last_stage": stage,
                    "last_seen_at": occurred_at,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            ).encode("utf-8")
        )

        if self._events_seen is not None:
            self._events_seen.inc()
        if previous is None and self._state_records is not None:
            self._state_records.inc()

        LOG.info(
            "correlation_id=%s stage=%s occurred_at=%s previous=%s",
            ctx.get_current_key(),
            stage,
            occurred_at,
            "yes" if previous else "no",
        )


def build_source() -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(INPUT_TOPIC)
        .set_group_id(KAFKA_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
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
        "kafka-" + INPUT_TOPIC,
    )

    def extract_key(raw: str) -> str:
        try:
            event = json.loads(raw)
        except json.JSONDecodeError:
            return "unknown"
        return (
            event.get("correlation_id")
            or event.get("CorrelationId")
            or event.get("end_to_end_id")
            or event.get("EndToEndId")
            or "unknown"
        )

    keyed = stream.key_by(extract_key, key_type=Types.STRING())
    keyed.process(LifecycleSkeleton(), output_type=Types.STRING())
    env.execute("j1-lifecycle-state-skeleton")


if __name__ == "__main__":
    main()
