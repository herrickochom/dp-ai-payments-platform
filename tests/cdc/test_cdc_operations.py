import json
from pathlib import Path

import pytest

from services.shared.cdc.operations import (
    ALLOWED_FAILURE_FIELDS,
    PROHIBITED_FAILURE_FIELDS,
    cdc_failure_metadata,
    deterministic_cdc_failure_id,
    validate_safe_failure_metadata,
)


ROOT = Path(__file__).resolve().parents[2]

OPS_CONTRACT = (
    ROOT
    / "platform"
    / "cdc"
    / "contracts"
    / "cdc_operational_contract.json"
)

CONNECTOR_CONTRACT = (
    ROOT
    / "platform"
    / "cdc"
    / "contracts"
    / "debezium_connector_contract.json"
)


def load(path):
    return json.loads(path.read_text())


def test_operational_contract_requires_complete_monitoring():
    doc = load(OPS_CONTRACT)
    obs = doc["observability"]

    required = (
        "connector_state_required",
        "connector_task_state_required",
        "source_to_kafka_lag_required",
        "heartbeat_freshness_required",
        "replication_slot_activity_required",
        "retained_wal_growth_required",
        "connector_offset_progress_required",
        "kafka_consumer_progress_required",
        "raw_persistence_success_required",
        "raw_persistence_failure_required",
        "schema_drift_signal_required",
        "quarantine_count_required",
    )

    for field in required:
        assert obs[field] is True


def test_failure_contract_matches_code():
    doc = load(OPS_CONTRACT)
    policy = doc["safe_failure_metadata"]

    assert set(policy["allowed_fields"]) == (
        ALLOWED_FAILURE_FIELDS
    )

    assert set(policy["prohibited_fields"]) == (
        PROHIBITED_FAILURE_FIELDS
    )

    assert policy["exception_text_allowed"] is False
    assert policy["traceback_allowed"] is False
    assert policy["ordinary_log_payload_allowed"] is False


def test_failure_identity_is_deterministic_and_opaque():
    kwargs = {
        "source_system": "future_source",
        "topic": "cdc.future.public.account",
        "partition": 2,
        "offset": 41,
    }

    first = deterministic_cdc_failure_id(**kwargs)
    second = deterministic_cdc_failure_id(**kwargs)

    assert first == second
    assert len(first) == 64
    assert ":" not in first
    assert "future_source" not in first
    assert "account" not in first


def test_safe_failure_metadata_contains_only_allowlisted_fields():
    record = cdc_failure_metadata(
        source_system="future_source",
        topic="cdc.future.public.account",
        partition=2,
        offset=41,
        failure_category="RAW_STORAGE_FAILED",
        error=OSError("secret row value must not leak"),
        event_timestamp="2026-09-18T12:00:00Z",
    )

    assert set(record) == ALLOWED_FAILURE_FIELDS

    rendered = json.dumps(record)

    assert "secret row value must not leak" not in rendered

    for field in PROHIBITED_FAILURE_FIELDS:
        assert field not in record

    validate_safe_failure_metadata(record)


@pytest.mark.parametrize(
    "field",
    sorted(PROHIBITED_FAILURE_FIELDS),
)
def test_restricted_failure_fields_are_rejected(field):
    record = cdc_failure_metadata(
        source_system="future_source",
        topic="cdc.future.public.account",
        partition=2,
        offset=41,
        failure_category="CDC_FAILED",
        error=ValueError("restricted content"),
        event_timestamp="2026-09-18T12:00:00Z",
    )

    record[field] = "must-not-appear"

    with pytest.raises(ValueError):
        validate_safe_failure_metadata(record)


def test_unexpected_failure_metadata_field_is_rejected():
    record = cdc_failure_metadata(
        source_system="future_source",
        topic="cdc.future.public.account",
        partition=2,
        offset=41,
        failure_category="CDC_FAILED",
        error=ValueError("x"),
        event_timestamp="2026-09-18T12:00:00Z",
    )

    record["debug_message"] = "unsafe"

    with pytest.raises(ValueError):
        validate_safe_failure_metadata(record)


def test_quarantine_is_restricted_and_fail_stop():
    doc = load(OPS_CONTRACT)
    q = doc["quarantine"]

    assert q["required"] is True
    assert q["classification"] == "RESTRICTED"

    assert (
        q["ordinary_generated_event_dlq_reuse_allowed"]
        is False
    )

    assert (
        q["raw_source_payload_in_ordinary_dlq_allowed"]
        is False
    )

    assert (
        q["raw_source_key_in_ordinary_dlq_allowed"]
        is False
    )

    assert (
        q["quarantine_write_failure_action"]
        == "FAIL_STOP"
    )

    assert q["offset_commit_on_quarantine_failure"] is False


def test_exactly_once_claim_is_prohibited():
    doc = load(OPS_CONTRACT)
    replay = doc["raw_replay"]

    assert replay["deterministic_object_key_required"] is True
    assert replay["existing_object_replay_is_idempotent"] is True

    assert (
        replay["physical_exactly_once_claim_allowed"]
        is False
    )

    assert (
        replay[
            "content_collision_validation_required_before_production"
        ]
        is True
    )


def test_activation_requires_operational_preflight():
    doc = load(OPS_CONTRACT)
    preflight = doc["activation_preflight"]

    for value in preflight.values():
        assert value is True


def test_connector_contract_requires_observability_preflight():
    doc = load(CONNECTOR_CONTRACT)

    assert (
        doc["activation_controls"][
            "observability_preflight_required"
        ]
        is True
    )

    assert (
        doc["operational_requirements"][
            "dlq_or_quarantine_policy_required"
        ]
        is True
    )
