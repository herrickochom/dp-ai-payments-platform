import importlib.util
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / "platform/gcp/kafka_pubsub_bridge/source_operational_runtime_v1.py"

spec = importlib.util.spec_from_file_location("runtime_discriminator_test", PATH)
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)


def dispatch(data, legacy_process=None, source_store=None):
    return runtime.dispatch(
        data,
        "ack-1",
        object(),
        Mock(),
        legacy_process or Mock(return_value=("raw-object", "CREATED")),
        object(),
        Mock(),
        source_store=source_store,
    )


def test_invalid_json_rejected():
    with pytest.raises(runtime.OperationalError):
        dispatch(b"not-json")


def test_unknown_json_rejected():
    with pytest.raises(runtime.OperationalError):
        dispatch(b'{"unknown":true}')


def test_incomplete_legacy_rejected():
    with pytest.raises(runtime.OperationalError):
        dispatch(b'{"kafka_topic":"pdmis.repayments"}')


def test_invalid_source_discriminator_rejected_without_fallback():
    legacy = Mock()
    with pytest.raises(runtime.OperationalError):
        dispatch(
            json.dumps({
                "message_type": "invalid",
                "kafka_topic": "pdmis.repayments",
            }).encode(),
            legacy_process=legacy,
        )
    legacy.assert_not_called()


def test_legacy_transport_routes_only_after_validation():
    import base64
    import hashlib

    schema_id = 1
    wire = bytes([0]) + schema_id.to_bytes(4, "big") + b"{}"

    envelope = {
        "kafka_topic": "pdmis.repayments",
        "kafka_partition": 0,
        "kafka_offset": 1,
        "kafka_timestamp_ms": 1770000000000,
        "schema_registry_id": schema_id,
        "payload_base64": base64.b64encode(wire).decode(),
        "payload_sha256": hashlib.sha256(wire).hexdigest(),
    }

    legacy = Mock(return_value=("raw-object", "CREATED"))

    route, result = dispatch(
        json.dumps(envelope).encode(),
        legacy_process=legacy,
    )

    assert route == "LEGACY"
    assert result == ("raw-object", "CREATED")
    legacy.assert_called_once()


def test_legacy_digest_mismatch_rejected_before_processing():
    import base64

    wire = bytes([0]) + (1).to_bytes(4, "big") + b"{}"

    envelope = {
        "kafka_topic": "pdmis.repayments",
        "kafka_partition": 0,
        "kafka_offset": 1,
        "kafka_timestamp_ms": 1770000000000,
        "schema_registry_id": 1,
        "payload_base64": base64.b64encode(wire).decode(),
        "payload_sha256": "0" * 64,
    }

    legacy = Mock()

    with pytest.raises(runtime.OperationalError):
        dispatch(
            json.dumps(envelope).encode(),
            legacy_process=legacy,
        )

    legacy.assert_not_called()
