"""Offline acceptance for the canonical Raw v1 Kafka-to-Pub/Sub publisher."""

import base64
import hashlib
import importlib.util
import json
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


ROOT = Path(__file__).resolve().parents[2]
PUBLISHER = (
    ROOT / "platform/gcp/kafka_pubsub_bridge/raw_v1_bridge_live.py"
)


class TopicPartition:
    def __init__(self, topic, partition, offset, error=None):
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.error = error


@pytest.fixture
def bridge(monkeypatch):
    kafka = types.ModuleType("confluent_kafka")
    kafka.TopicPartition = TopicPartition
    kafka.Consumer = Mock(
        side_effect=AssertionError("Kafka client must not initialize")
    )

    registry = types.ModuleType("confluent_kafka.schema_registry")
    registry.SchemaRegistryClient = Mock(
        side_effect=AssertionError("Registry client must not initialize")
    )

    google = types.ModuleType("google")
    cloud = types.ModuleType("google.cloud")
    pubsub = types.ModuleType("google.cloud.pubsub_v1")

    pubsub.PublisherClient = Mock(
        side_effect=AssertionError("Pub/Sub client must not initialize")
    )

    google.cloud = cloud
    cloud.pubsub_v1 = pubsub

    modules = {
        "confluent_kafka": kafka,
        "confluent_kafka.schema_registry": registry,
        "google": google,
        "google.cloud": cloud,
        "google.cloud.pubsub_v1": pubsub,
    }

    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)

    spec = importlib.util.spec_from_file_location(
        "raw_v1_bridge_acceptance_under_test", PUBLISHER
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return SimpleNamespace(
        module=module,
        publish=module.load_bridge().publish_wire_then_commit,
        kafka=kafka,
        registry=registry,
        pubsub=pubsub,
    )


@pytest.fixture
def message():
    msg = Mock()
    msg.topic.return_value = "pdmis.repayments"
    msg.partition.return_value = 0
    msg.offset.return_value = 7
    msg.timestamp.return_value = (1, 1763100000000)
    msg.value.return_value = b"\x00\x00\x00\x00\x09\x02"
    return msg


def clients(message, *, message_id="ack-123", commit_result="valid"):
    actions = []

    publisher = Mock()

    def acknowledge(timeout=None):
        actions.append("publish_ack")
        return message_id

    publisher.publish.return_value.result.side_effect = acknowledge

    consumer = Mock()

    def commit(*, offsets, asynchronous):
        assert asynchronous is False
        actions.append("kafka_commit")

        if commit_result == "valid":
            return [
                TopicPartition(
                    message.topic(),
                    message.partition(),
                    message.offset() + 1,
                )
            ]

        return commit_result

    consumer.commit.side_effect = commit

    return consumer, publisher, actions


def test_canonical_seven_field_transport(bridge, message):
    consumer, publisher, actions = clients(message)

    result = bridge.publish(
        consumer, publisher, "offline-topic", message
    )

    assert result == "ack-123"
    assert actions == ["publish_ack", "kafka_commit"]

    body = json.loads(publisher.publish.call_args.args[1])

    assert set(body) == {
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp_ms",
        "schema_registry_id",
        "payload_base64",
        "payload_sha256",
    }

    wire = message.value()

    assert body["kafka_topic"] == "pdmis.repayments"
    assert body["kafka_partition"] == 0
    assert body["kafka_offset"] == 7
    assert body["kafka_timestamp_ms"] == 1763100000000
    assert body["schema_registry_id"] == 9
    assert base64.b64decode(body["payload_base64"]) == wire
    assert body["payload_sha256"] == hashlib.sha256(wire).hexdigest()


@pytest.mark.parametrize("wire", [
    b"invalid",
    b"\x00\x00\x00\x00\x00\x02",
])
def test_invalid_wire_never_published_or_committed(
    bridge, message, wire
):
    message.value.return_value = wire
    consumer, publisher, _ = clients(message)

    with pytest.raises(RuntimeError):
        bridge.publish(consumer, publisher, "offline-topic", message)

    publisher.publish.assert_not_called()
    consumer.commit.assert_not_called()


def test_pubsub_publish_failure_never_commits(bridge, message):
    consumer, publisher, _ = clients(message)

    publisher.publish.return_value.result.side_effect = TimeoutError(
        "Pub/Sub acknowledgement timed out"
    )

    with pytest.raises(TimeoutError):
        bridge.publish(consumer, publisher, "offline-topic", message)

    consumer.commit.assert_not_called()


def test_missing_pubsub_ack_never_commits(bridge, message):
    consumer, publisher, _ = clients(message, message_id="")

    with pytest.raises(RuntimeError):
        bridge.publish(consumer, publisher, "offline-topic", message)

    consumer.commit.assert_not_called()


@pytest.mark.parametrize("result", [
    [],
    [TopicPartition("pdmis.repayments", 0, 8, RuntimeError("broker"))],
    [TopicPartition("pdmis.repayments", 0, 9)],
    [TopicPartition("pdmis.repayments", 1, 8)],
    [TopicPartition("pdmis.loans", 0, 8)],
])
def test_invalid_commit_response_rejected(
    bridge, message, result
):
    consumer, publisher, actions = clients(
        message, commit_result=result
    )

    with pytest.raises(RuntimeError):
        bridge.publish(consumer, publisher, "offline-topic", message)

    assert actions == ["publish_ack", "kafka_commit"]
    consumer.commit.assert_called_once()


def test_unauthorized_source_rejected_before_clients(
    bridge, message
):
    args = SimpleNamespace(
        source_topic="unauthorized.test.topic",
        kafka_bootstrap="unused",
        schema_registry_url="unused",
        project="unused",
        topic="pdm-dev-payments-source-events-v1",
        group="offline-test",
        max_messages=1,
    )

    with pytest.raises(RuntimeError, match="not approved"):
        bridge.module.run(args)

    bridge.kafka.Consumer.assert_not_called()
    bridge.registry.SchemaRegistryClient.assert_not_called()
    bridge.pubsub.PublisherClient.assert_not_called()



@pytest.mark.parametrize("decoded", [
    None,
    {},
    {"event_id": ""},
    {"event_id": 123},
])
def test_run_rejects_invalid_decoded_event_before_publish(
    bridge, message, monkeypatch, decoded
):
    """A decoded source event must satisfy the Raw v1 event identity."""
    from unittest.mock import Mock

    message.error.return_value = None
    registry = Mock()
    registry.get_schema.return_value = Mock()

    consumer = Mock()
    consumer.poll.side_effect = [message]
    consumer.close.return_value = None

    publisher = Mock()
    publisher.topic_path.return_value = "offline-topic"

    bridge.registry.SchemaRegistryClient.side_effect = None
    bridge.registry.SchemaRegistryClient.return_value = registry

    bridge.kafka.Consumer.side_effect = None
    bridge.kafka.Consumer.return_value = consumer

    bridge.pubsub.PublisherClient.side_effect = None
    bridge.pubsub.PublisherClient.return_value = publisher

    # Replace only the decoder constructor; no real registry or cloud calls.
    import importlib.util

    original_spec = importlib.util.spec_from_file_location

    def guarded_spec(name, location, *args, **kwargs):
        spec = original_spec(name, location, *args, **kwargs)

        if name == "raw_v1_publisher_cloud_adapter":
            original_exec = spec.loader.exec_module

            def guarded_exec(module):
                original_exec(module)
                module.RegistryDecoder = Mock(
                    return_value=Mock(return_value=decoded)
                )

            spec.loader.exec_module = guarded_exec

        return spec

    monkeypatch.setattr(
        importlib.util,
        "spec_from_file_location",
        guarded_spec,
    )

    args = SimpleNamespace(
        source_topic="pdmis.repayments",
        kafka_bootstrap="offline",
        schema_registry_url="offline",
        project="offline",
        topic="pdm-dev-payments-source-events-v1",
        group="offline-test",
        max_messages=1,
    )

    with pytest.raises(RuntimeError, match="invalid decoded source event"):
        bridge.module.run(args)

    registry.get_schema.assert_called_once_with(9)
    publisher.publish.assert_not_called()
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()


def test_run_decoder_exception_prevents_publish_and_commit(
    bridge, message, monkeypatch
):
    """A Schema Registry-backed Avro decode failure must fail closed."""
    from unittest.mock import Mock
    import importlib.util

    message.error.return_value = None
    registry = Mock()
    consumer = Mock()
    consumer.poll.return_value = message

    publisher = Mock()
    publisher.topic_path.return_value = "offline-topic"

    bridge.registry.SchemaRegistryClient.side_effect = None
    bridge.registry.SchemaRegistryClient.return_value = registry

    bridge.kafka.Consumer.side_effect = None
    bridge.kafka.Consumer.return_value = consumer

    bridge.pubsub.PublisherClient.side_effect = None
    bridge.pubsub.PublisherClient.return_value = publisher

    original_spec = importlib.util.spec_from_file_location

    def guarded_spec(name, location, *args, **kwargs):
        spec = original_spec(name, location, *args, **kwargs)

        if name == "raw_v1_publisher_cloud_adapter":
            original_exec = spec.loader.exec_module

            def guarded_exec(module):
                original_exec(module)
                module.RegistryDecoder = Mock(
                    return_value=Mock(
                        side_effect=ValueError("invalid Avro datum")
                    )
                )

            spec.loader.exec_module = guarded_exec

        return spec

    monkeypatch.setattr(
        importlib.util,
        "spec_from_file_location",
        guarded_spec,
    )

    args = SimpleNamespace(
        source_topic="pdmis.repayments",
        kafka_bootstrap="offline",
        schema_registry_url="offline",
        project="offline",
        topic="pdm-dev-payments-source-events-v1",
        group="offline-test",
        max_messages=1,
    )

    with pytest.raises(ValueError, match="invalid Avro datum"):
        bridge.module.run(args)

    publisher.publish.assert_not_called()
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()



def test_valid_avro_publish_ack_then_kafka_commit(monkeypatch):
    import importlib.util
    import io
    import json
    import sys
    import types
    from pathlib import Path
    from unittest.mock import Mock

    from fastavro import parse_schema, schemaless_writer
    from confluent_kafka.schema_registry import Schema

    root = Path(__file__).resolve().parents[2]
    publisher_path = (
        root / "platform/gcp/kafka_pubsub_bridge/raw_v1_bridge_live.py"
    )

    spec = importlib.util.spec_from_file_location(
        "raw_v1_valid_record_acceptance",
        publisher_path,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    schema = {
        "type": "record",
        "name": "AcceptanceEvent",
        "fields": [
            {"name": "event_id", "type": "string"},
            {"name": "amount", "type": "double"},
        ],
    }

    event = {
        "event_id": "acceptance-001",
        "amount": 125.5,
    }

    output = io.BytesIO()
    schemaless_writer(output, parse_schema(schema), event)

    wire = bytes([0]) + (9).to_bytes(4, "big") + output.getvalue()

    operations = []

    class OfflineRegistry:
        def __init__(self, configuration):
            self.configuration = configuration

        def get_schema(self, schema_id, *args, **kwargs):
            operations.append("schema_lookup")
            assert schema_id == 9
            return Schema(json.dumps(schema), "AVRO")

    class KafkaMessage:
        def error(self):
            return None

        def value(self):
            return wire

        def topic(self):
            return "pdmis.repayments"

        def partition(self):
            return 0

        def offset(self):
            return 17

        def timestamp(self):
            return (1, 1760000000000)

    class CommitResult:
        error = None
        topic = "pdmis.repayments"
        partition = 0
        offset = 18

    class OfflineConsumer:
        def __init__(self, configuration):
            self.configuration = configuration
            self.messages = [KafkaMessage()]

        def subscribe(self, topics):
            assert topics == ["pdmis.repayments"]

        def poll(self, timeout):
            if self.messages:
                return self.messages.pop(0)
            return None

        def commit(self, offsets, asynchronous):
            operations.append("kafka_commit")
            assert asynchronous is False
            assert len(offsets) == 1
            assert offsets[0].topic == "pdmis.repayments"
            assert offsets[0].partition == 0
            assert offsets[0].offset == 18
            return [CommitResult()]

        def close(self):
            operations.append("consumer_close")

    class PublishFuture:
        def result(self, timeout):
            operations.append("pubsub_ack")
            assert timeout == 30
            return "offline-message-001"

    class OfflinePublisher:
        def topic_path(self, project, topic):
            return "projects/offline/topics/raw-v1"

        def publish(self, topic_path, payload):
            operations.append("pubsub_publish")

            envelope = json.loads(payload)

            assert envelope["kafka_topic"] == "pdmis.repayments"
            assert envelope["kafka_partition"] == 0
            assert envelope["kafka_offset"] == 17
            assert envelope["schema_registry_id"] == 9

            import base64
            import hashlib

            assert base64.b64decode(
                envelope["payload_base64"]
            ) == wire

            assert envelope["payload_sha256"] == hashlib.sha256(
                wire
            ).hexdigest()

            return PublishFuture()

    import confluent_kafka
    import confluent_kafka.schema_registry
    import google.cloud.pubsub_v1

    monkeypatch.setattr(
        confluent_kafka,
        "Consumer",
        OfflineConsumer,
    )
    monkeypatch.setattr(
        confluent_kafka.schema_registry,
        "SchemaRegistryClient",
        OfflineRegistry,
    )
    monkeypatch.setattr(
        google.cloud.pubsub_v1,
        "PublisherClient",
        OfflinePublisher,
    )

    args = types.SimpleNamespace(
        source_topic="pdmis.repayments",
        schema_registry_url="http://offline.invalid",
        kafka_bootstrap="offline.invalid:9092",
        group="offline-acceptance",
        project="offline",
        topic="raw-v1",
        max_messages=1,
    )

    processed = module.run(args)

    assert processed == 1

    assert operations.count("pubsub_publish") == 1
    assert operations.count("pubsub_ack") == 1
    assert operations.count("kafka_commit") == 1
    assert operations.count("consumer_close") == 1

    assert operations.index("pubsub_publish") < operations.index(
        "pubsub_ack"
    )
    assert operations.index("pubsub_ack") < operations.index(
        "kafka_commit"
    )
