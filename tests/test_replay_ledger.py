from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from services.shared.replay_ledger import (
    ReplayLedger,
    ReplayRequest,
    ReplayStatus,
    canonical_replay_identity,
)


def request(**overrides):
    values = {
        "replay_batch_id": "synthetic-batch-001",
        "replay_request_id": "synthetic-request-001",
        "source_topic": "synthetic.source",
        "source_partition": 1,
        "source_offset": 42,
        "target_topic": "synthetic.target",
        "replay_reason_code": "SYNTHETIC_TEST",
        "requested_by_service_or_role": "test-role",
        "approved": True,
        "policy_resolved": True,
        "source_event_reference": "synthetic-event-reference",
    }
    values.update(overrides)
    return ReplayRequest(**values)


def test_canonical_identity_is_deterministic():
    a = canonical_replay_identity("source", 1, 42, "target")
    b = canonical_replay_identity("source", 1, 42, "target")
    assert a == b
    assert len(a) == 64


def test_canonical_identity_changes_with_coordinates():
    a = canonical_replay_identity("source", 1, 42, "target")
    b = canonical_replay_identity("source", 1, 43, "target")
    assert a != b


def test_successful_reservation(tmp_path):
    ledger = ReplayLedger(tmp_path / "ledger.db")
    identity = ledger.reserve(request())
    row = ledger.get(identity)

    assert row["replay_status"] == ReplayStatus.REQUESTED.value
    assert row["replay_identity"] == identity
    ledger.close()


def test_duplicate_reservation_suppressed(tmp_path):
    ledger = ReplayLedger(tmp_path / "ledger.db")
    ledger.reserve(request())

    with pytest.raises(RuntimeError, match="duplicate replay suppressed"):
        ledger.reserve(
            request(replay_request_id="synthetic-request-002")
        )

    ledger.close()


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("replay_request_id", "", "missing replay_request_id"),
        ("replay_batch_id", "", "missing replay_batch_id"),
        ("source_topic", "", "missing source_topic"),
        ("source_partition", None, "missing source_partition"),
        ("source_offset", None, "missing source_offset"),
        ("target_topic", "", "missing target_topic"),
    ],
)
def test_missing_coordinates_fail_closed(tmp_path, field, value, error):
    ledger = ReplayLedger(tmp_path / "ledger.db")

    with pytest.raises(ValueError, match=error):
        ledger.reserve(request(**{field: value}))

    ledger.close()


def test_unresolved_policy_fails_closed(tmp_path):
    ledger = ReplayLedger(tmp_path / "ledger.db")

    with pytest.raises(PermissionError, match="policy unresolved"):
        ledger.reserve(request(policy_resolved=False))

    ledger.close()


def test_missing_approval_fails_closed(tmp_path):
    ledger = ReplayLedger(tmp_path / "ledger.db")

    with pytest.raises(PermissionError, match="approval absent"):
        ledger.reserve(request(approved=False))

    ledger.close()


def test_explicit_database_path_required():
    with pytest.raises(ValueError, match="explicit database path required"):
        ReplayLedger("")


def test_unavailable_ledger_fails_closed(tmp_path):
    impossible = tmp_path / "missing-parent" / "ledger.db"

    with pytest.raises(RuntimeError, match="durable replay ledger unavailable"):
        ReplayLedger(impossible)


def test_allowed_state_transitions(tmp_path):
    ledger = ReplayLedger(tmp_path / "ledger.db")
    identity = ledger.reserve(request())

    ledger.transition(identity, ReplayStatus.APPROVED)
    ledger.transition(identity, ReplayStatus.IN_PROGRESS)
    ledger.transition(identity, ReplayStatus.SUCCEEDED, "OK")

    row = ledger.get(identity)
    assert row["replay_status"] == ReplayStatus.SUCCEEDED.value
    assert row["result_code"] == "OK"
    assert row["started_at"] is not None
    assert row["completed_at"] is not None

    ledger.close()


def test_invalid_transition_fails_closed(tmp_path):
    ledger = ReplayLedger(tmp_path / "ledger.db")
    identity = ledger.reserve(request())

    with pytest.raises(ValueError, match="invalid replay transition"):
        ledger.transition(identity, ReplayStatus.SUCCEEDED)

    assert ledger.get(identity)["replay_status"] == ReplayStatus.REQUESTED.value
    ledger.close()


def test_succeeded_is_terminal(tmp_path):
    ledger = ReplayLedger(tmp_path / "ledger.db")
    identity = ledger.reserve(request())

    ledger.transition(identity, ReplayStatus.APPROVED)
    ledger.transition(identity, ReplayStatus.IN_PROGRESS)
    ledger.transition(identity, ReplayStatus.SUCCEEDED)

    with pytest.raises(ValueError, match="invalid replay transition"):
        ledger.transition(identity, ReplayStatus.IN_PROGRESS)

    assert ledger.get(identity)["replay_status"] == ReplayStatus.SUCCEEDED.value
    ledger.close()


@pytest.mark.parametrize(
    "field",
    [
        "payload",
        "raw_payload",
        "encoded_payload",
        "message_value",
        "kafka_key",
        "beneficiary_id",
        "beneficiary_token",
        "nin",
        "account_number",
        "wallet_number",
        "exception",
        "exception_text",
        "traceback",
    ],
)
def test_prohibited_metadata_rejected(tmp_path, field):
    ledger = ReplayLedger(tmp_path / "ledger.db")

    with pytest.raises(ValueError, match="prohibited replay metadata"):
        ledger.reserve(request(), {field: "PROTECTED-SYNTHETIC-VALUE"})

    ledger.close()


def test_persistence_across_reopen(tmp_path):
    path = tmp_path / "ledger.db"

    first = ReplayLedger(path)
    identity = first.reserve(request())
    first.close()

    second = ReplayLedger(path)
    row = second.get(identity)

    assert row["replay_identity"] == identity
    assert row["replay_status"] == ReplayStatus.REQUESTED.value
    second.close()


def test_competing_reservation_only_one_succeeds(tmp_path):
    path = tmp_path / "ledger.db"

    def reserve_once(number):
        ledger = ReplayLedger(path)
        try:
            identity = ledger.reserve(
                request(
                    replay_request_id=f"synthetic-request-{number}"
                )
            )
            return ("reserved", identity)
        except RuntimeError:
            return ("suppressed", None)
        finally:
            ledger.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(reserve_once, (1, 2)))

    states = sorted(result[0] for result in results)
    assert states == ["reserved", "suppressed"]


def test_no_infrastructure_imports():
    source = (
        Path(__file__).resolve().parents[1]
        / "services/shared/replay_ledger.py"
    ).read_text()

    forbidden = (
        "confluent_kafka",
        "kafka-python",
        "boto3",
        "minio",
        "requests",
        "docker",
        "trino",
        "duckdb",
        "subprocess",
        "socket",
    )

    lowered = source.lower()
    for name in forbidden:
        assert name.lower() not in lowered


def test_no_replay_execution_api():
    source = (
        Path(__file__).resolve().parents[1]
        / "services/shared/replay_ledger.py"
    ).read_text()

    assert "producer.produce" not in source
    assert "consumer.seek" not in source
    assert "consumer.commit" not in source


def test_successful_reservation_is_audited(tmp_path):
    ledger = ReplayLedger(tmp_path / "ledger.db")
    identity = ledger.reserve(request())

    attempts = ledger.attempts(identity)

    assert len(attempts) == 1
    assert attempts[0]["attempt_status"] == ReplayStatus.REQUESTED.value
    assert attempts[0]["result_code"] == "RESERVED"
    ledger.close()


def test_duplicate_attempt_is_durably_audited(tmp_path):
    ledger = ReplayLedger(tmp_path / "ledger.db")
    identity = ledger.reserve(request())

    with pytest.raises(RuntimeError, match="duplicate replay suppressed"):
        ledger.reserve(
            request(
                replay_request_id="synthetic-request-duplicate",
                replay_batch_id="synthetic-batch-duplicate",
            )
        )

    attempts = ledger.attempts(identity)

    assert len(attempts) == 2
    assert attempts[0]["attempt_status"] == ReplayStatus.REQUESTED.value
    assert attempts[1]["attempt_status"] == ReplayStatus.SUPPRESSED_DUPLICATE.value
    assert attempts[1]["result_code"] == "PRIOR_RESERVATION_EXISTS"
    ledger.close()


def test_attempt_audit_persists_across_reopen(tmp_path):
    path = tmp_path / "ledger.db"

    first = ReplayLedger(path)
    identity = first.reserve(request())

    with pytest.raises(RuntimeError, match="duplicate replay suppressed"):
        first.reserve(
            request(replay_request_id="synthetic-request-second")
        )

    first.close()

    second = ReplayLedger(path)
    attempts = second.attempts(identity)

    assert len(attempts) == 2
    assert attempts[1]["attempt_status"] == ReplayStatus.SUPPRESSED_DUPLICATE.value
    second.close()
