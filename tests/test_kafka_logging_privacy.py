"""Synthetic, fully mocked consumer error logging and replay preservation checks."""
import base64
import importlib.util
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

spec = importlib.util.spec_from_file_location(
    'consumer_logging_privacy', Path(__file__).resolve().parents[1]
    / 'services/kafka-consumer-events/kafka_consumer_events.py')
consumer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(consumer)

MARKERS = ('SYNTHETIC_NIN_DO_NOT_LOG', 'SYNTHETIC_ACCOUNT_DO_NOT_LOG',
           'SYNTHETIC_PAYLOAD_SECRET', 'SYNTHETIC_BENEFICIARY_DO_NOT_LOG',
           'SYNTHETIC_NAME_DO_NOT_LOG', '+256700000000', 'synthetic@example.invalid')
SECRET = ' '.join(MARKERS)


def message():
    msg = Mock()
    msg.topic.return_value = 'pdmis.loans'
    msg.partition.return_value = 2
    msg.offset.return_value = 17
    msg.key.return_value = SECRET.encode()
    msg.value.return_value = SECRET.encode()
    msg.timestamp.return_value = (1, 1750000000000)
    return msg


def assert_private(caplog):
    text = caplog.text
    for marker in (*MARKERS, base64.b64encode(SECRET.encode()).decode(),
                   'original_payload_base64', 'original_key_base64', 'business_key'):
        assert marker not in text
    assert all(record.exc_info is None for record in caplog.records)


def test_acknowledged_dlq_logs_metadata_and_preserves_replay(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG, logger=consumer.logger.name)
    msg, client, producer = message(), Mock(), Mock()
    calls = []

    def produce(topic, **kwargs):
        calls.append('publish')
        kwargs['callback'](None, Mock())

    producer.produce.side_effect = produce
    producer.flush.return_value = 0
    client.commit.side_effect = lambda *a, **kw: calls.append('commit')
    monkeypatch.setattr(consumer, 'store_event_to_s3', Mock(
        side_effect=consumer.PermanentProcessingError(SECRET)))
    result = consumer.process_message(client, producer, Mock(return_value={
        'event_id': SECRET, 'x_attributes': {'x-correlationId': SECRET}}), msg)
    assert result == 'dlq'
    assert calls == ['publish', 'commit']
    client.commit.assert_called_once_with(msg, asynchronous=False)
    args, kwargs = producer.produce.call_args
    envelope = json.loads(kwargs['value'])
    assert args[0] == consumer.Settings.KAFKA_DLQ_TOPIC
    assert kwargs['key'] == envelope['failure_id'] == 'pdmis.loans:2:17'
    assert kwargs['headers'] == {'x-failure-id': 'pdmis.loans:2:17'}
    assert base64.b64decode(envelope['original_payload_base64']) == msg.value()
    assert base64.b64decode(envelope['original_key_base64']) == msg.key()
    assert envelope['business_key'] == SECRET
    assert envelope['failure_reason'] == SECRET
    assert envelope['original_event_id'] == SECRET
    assert_private(caplog)
    record = json.loads(next(r.message for r in caplog.records if 'sent_to_dlq' in r.message))
    assert record == {'event': 'sent_to_dlq', 'topic': 'pdmis.loans', 'partition': 2,
                      'offset': 17, 'retry_count': 0,
                      'dlq_destination': consumer.Settings.KAFKA_DLQ_TOPIC,
                      'error_class': 'PermanentProcessingError'}


@pytest.mark.parametrize('raises', [False, True])
def test_failed_dlq_fail_stop_logs_no_business_key(monkeypatch, caplog, raises):
    for name, value in {"OBJECT_STORE_BUCKET": "dp-ai-payment", "RAW_ROOT": "raw", "RAW_VERSION": "v2", "RAW_PREFIX": "raw/v2"}.items():
        monkeypatch.setenv(name, value)
    caplog.set_level(logging.DEBUG, logger=consumer.logger.name)
    msg, client, producer = message(), Mock(), Mock()
    msg.error.return_value = None
    client.poll.return_value = msg
    if raises:
        producer.produce.side_effect = ValueError(SECRET)
    else:
        producer.flush.return_value = 1
    monkeypatch.setattr(consumer, 'serialize_to_avro', Mock(return_value=b'Obj\x01'))
    monkeypatch.setattr(consumer, 'SchemaRegistryClient', Mock())
    monkeypatch.setattr(consumer, 'AvroDeserializer', Mock(return_value=Mock(return_value={})))
    monkeypatch.setattr(consumer, 'Consumer', Mock(return_value=client))
    monkeypatch.setattr(consumer, 'Producer', Mock(return_value=producer))
    monkeypatch.setattr(consumer, 'store_event_to_s3', Mock(
        side_effect=consumer.PermanentProcessingError(SECRET)))
    with pytest.raises(SystemExit) as exc:
        consumer.main()
    assert exc.value.code == 1
    client.commit.assert_not_called()
    client.poll.assert_called_once()
    client.close.assert_called_once()
    assert 'fail_stop_dlq_publish_failed' in caplog.text
    assert_private(caplog)


@pytest.mark.parametrize('error', [consumer.AvroException(SECRET), ValueError(SECRET),
                                   type('SYNTHETIC_PAYLOAD_SECRET', (Exception,), {})(SECRET)])
def test_serialization_exception_text_and_traceback_not_logged(monkeypatch, caplog, error):
    caplog.set_level(logging.DEBUG, logger=consumer.logger.name)
    writer = Mock()
    writer.append.side_effect = error
    writer.close.side_effect = ValueError(SECRET)
    monkeypatch.setattr(consumer, 'DataFileWriter', Mock(return_value=writer))
    with pytest.raises(type(error)):
        consumer.serialize_to_avro({})
    assert 'avro_serialization_failed' in caplog.text
    assert_private(caplog)


def test_storage_exception_logs_metadata_only(monkeypatch, caplog):
    for name, value in {"OBJECT_STORE_BUCKET": "dp-ai-payment", "RAW_ROOT": "raw", "RAW_VERSION": "v2", "RAW_PREFIX": "raw/v2"}.items():
        monkeypatch.setenv(name, value)
    caplog.set_level(logging.DEBUG, logger=consumer.logger.name)
    client = Mock()
    client.head_object.side_effect = OSError(SECRET)
    monkeypatch.setattr(consumer, 'get_minio_client', Mock(return_value=client))
    with pytest.raises(OSError):
        consumer.store_event_to_s3({}, 'pdmis.loans', 2, 17, datetime.now(timezone.utc))
    assert 'raw_storage_failed' in caplog.text
    client.put_object.assert_not_called()
    assert_private(caplog)
