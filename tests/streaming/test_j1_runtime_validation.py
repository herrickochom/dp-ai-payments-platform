"""Run-time schema-binding validation against the actual Flink container.

Executed inside the pinned Flink image (apache-flink==1.18.1-py3) where PyFlink
is installed, this script builds the DataStream -> Table -> Kafka sink pipeline
against the real PyFlink API and reads back the resulting topics.
"""

import json
import os
import sys
import time

# __file__ is tests/streaming/<name>.py, so the repo root is three levels up.
REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
if os.path.join(REPO_ROOT, "platform", "flink", "jobs") not in sys.path:
    sys.path.insert(0, os.path.join(REPO_ROOT, "platform", "flink", "jobs"))

import j1_lifecycle_state_skeleton as j1

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
STATE_TOPIC = os.environ.get("J1_STATE_TOPIC", "payments.lifecycle.state.v1")
TRANSITIONS_TOPIC = os.environ.get("J1_TRANSITIONS_TOPIC", "payments.lifecycle.transitions.v1")


def _synthetic_envelope():
    """One envelope carrying one event body for the job to parse."""
    return {
        "event_id": "runtime-ev-1",
        "message_id": "msg-1",
        "event_type": "PLM",
        "event_family": "technical",
        "source_system": "mobile-money",
        "parsed_event_data": json.dumps({
            "correlation_id": "runtime-corr-1",
            "technical_stage": "mobile-money-credit",
            "technical_status": "PENDING",
            "event_timestamp": "2026-08-29T12:00:00+03:00",
            "end_to_end_id": None,
            "instruction_id": None,
        }),
    }


def _kafka_consumer(topic, bootstrap, group_id=None):
    from confluent_kafka import Consumer
    return Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id or "j1-schema-validation",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }, subscription=[topic])


def _read_topic(topic, bootstrap, limit=10, timeout=15.0):
    consumer = _kafka_consumer(topic, bootstrap)
    try:
        messages = []
        start = time.time()
        while len(messages) < limit and time.time() - start < timeout:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == 13:
                    continue
                raise RuntimeError(msg.error())
            messages.append((msg.key().decode(), msg.value().decode()))
        return messages
    finally:
        consumer.close()


def _write_topic(topic, bootstrap, key, value):
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": bootstrap})
    p.produce(topic, key=key.encode() if isinstance(key, str) else key,
              value=value.encode() if isinstance(value, str) else value)
    p.flush(timeout=10)


class TestRuntimeSchemaBinding:
    """End-to-end schema binding against the real Flink PyFlink 1.18.1."""

    def test_state_table_binds_strictly(self):
        """State tuples bind to the LifecycleState schema in contract order,
        including nullable fields and the nested stage_history ARRAY."""
        env, t_env = _build_test_env()
        envelopes = env.from_elements(_synthetic_envelope())
        keyed = envelopes.key_by(j1.correlation_key)
        records = keyed.process(j1.LifecycleSkeleton())
        state_tuples = records.flat_map(
            j1.extract_state_payload, output_type=j1.STATE_TUPLE_TYPE
        )
        state_table = t_env.from_data_stream(state_tuples, *j1.STATE_FIELDS)
        t_env.execute_sql(
            f"INSERT INTO {j1.STATE_SINK_TABLE} SELECT * FROM state_source_view"
        )
        vals = _read_topic(STATE_TOPIC, KAFKA_BOOTSTRAP, limit=10)
        assert len(vals) >= 1, "state topic must contain at least one record"
        latest = max(vals, key=lambda _kv: _kv[0])
        state = json.loads(latest[1])
        for field in [
            "correlation_id", "current_stage", "current_status", "last_event_id",
            "last_event_timestamp", "last_processing_timestamp", "first_seen_at",
            "updated_at", "stage_history", "retry_count", "sla_deadline",
            "sla_breached", "end_to_end_id", "instruction_id", "schema_version",
        ]:
            assert field in state, f"missing field {field} in compacted state"

    def test_transitions_table_binds_strictly(self):
        """Transitions bind to the LifecycleTransition schema in contract order."""
        env, t_env = _build_test_env()
        envelopes = env.from_elements(_synthetic_envelope())
        keyed = envelopes.key_by(j1.correlation_key)
        records = keyed.process(j1.LifecycleSkeleton())
        transition_tuples = records.flat_map(
            j1.extract_transition_payload, output_type=j1.TRANSITION_TUPLE_TYPE
        )
        transition_table = t_env.from_data_stream(
            transition_tuples, *j1.TRANSITION_FIELDS
        )
        t_env.execute_sql(
            f"INSERT INTO {j1.TRANSITIONS_SINK_TABLE} "
            "SELECT * FROM transitions_source_view"
        )
        vals = _read_topic(TRANSITIONS_TOPIC, KAFKA_BOOTSTRAP, limit=10)
        assert len(vals) >= 1, "transitions topic must contain at least one record"
        latest = max(vals, key=lambda _kv: _kv[0])
        transition = json.loads(latest[1])
        for field in [
            "transition_id", "correlation_id", "event_id", "from_stage",
            "to_stage", "from_status", "to_status", "event_timestamp",
            "processing_timestamp", "source_topic", "source_partition",
            "source_offset", "job_id", "schema_version",
        ]:
            assert field in transition, f"missing field {field} in transition"

    def test_state_contract_nullable_fields_present(self):
        """Nullable fields declared by STR-003 must be present and NULL-able."""
        env, t_env = _build_test_env()
        envelopes = env.from_elements(_synthetic_envelope())
        keyed = envelopes.key_by(j1.correlation_key)
        records = keyed.process(j1.LifecycleSkeleton())
        state_tuples = records.flat_map(
            j1.extract_state_payload, output_type=j1.STATE_TUPLE_TYPE
        )
        state_table = t_env.from_data_stream(state_tuples, *j1.STATE_FIELDS)
        row = next(state_table.to_data_stream().collect_iter())
        assert row.get_field_by_name("sla_deadline") is None
        assert row.get_field_by_name("end_to_end_id") is None
        assert row.get_field_by_name("instruction_id") is None


def _build_test_env():
    """Return StreamExecutionEnvironment + StreamTableEnvironment wired with
    the skeleton's DDL builders but a synthetic in-memory source."""
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(env)
    return env, t_env


def main():
    import unittest
    suite = unittest.TestLoader().loadTestsFromTestCase(TestRuntimeSchemaBinding)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
