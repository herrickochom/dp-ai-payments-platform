"""Phase 2.4A: source event identity and replay regression tests."""

import json
import tempfile
import unittest
from pathlib import Path

import pytest

from test_payment_event_separation import producer


class PaymentEventIdentityTests(unittest.TestCase):

    def write_json(self, relative_path, records):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)

        path = Path(temporary.name) / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(records), encoding="utf-8")
        return path

    def test_wendi_authoritative_event_id_is_preserved(self):
        path = self.write_json(
            "wendi/transactions/transactions.json",
            [{
                "event_id": "WENDI-EVT-LOAN-000014",
                "loan_id": "LOAN-000014",
                "event_type": "DISBURSEMENT",
                "event_timestamp": "2026-09-01T13:00:00+03:00",
                "amount": 250000,
                "currency": "UGX",
            }],
        )

        first = producer.parse_json_events(str(path))
        second = producer.parse_json_events(str(path))

        self.assertEqual(1, len(first))
        self.assertEqual(
            "WENDI-EVT-LOAN-000014",
            first[0]["event_id"],
        )
        self.assertEqual(
            first[0]["event_id"],
            second[0]["event_id"],
        )

    def test_pdmis_repayments_have_distinct_replay_stable_ids(self):
        path = self.write_json(
            "pdmis/repayments/repayments.json",
            [
                {
                    "repayment_event_id": "REPAY-000014-001",
                    "loan_id": "LOAN-000014",
                    "event_sequence": 1,
                    "event_type": "REPAYMENT",
                    "amount": 25000,
                },
                {
                    "repayment_event_id": "REPAY-000014-002",
                    "loan_id": "LOAN-000014",
                    "event_sequence": 2,
                    "event_type": "RECOVERY",
                    "amount": 10000,
                },
            ],
        )

        first = producer.parse_json_events(str(path))
        second = producer.parse_json_events(str(path))

        self.assertEqual(2, len(first))

        self.assertEqual(
            ["REPAY-000014-001", "REPAY-000014-002"],
            [event["event_id"] for event in first],
        )

        self.assertEqual(
            [event["event_id"] for event in first],
            [event["event_id"] for event in second],
        )

        self.assertEqual(
            ["LOAN-000014", "LOAN-000014"],
            [
                producer.select_business_key("pdmis.repayments", event)
                for event in first
            ],
        )


if __name__ == "__main__":
    unittest.main()


def test_agent_transaction_identity_is_replay_stable(tmp_path):
    source = tmp_path / "agent_network" / "agent_transactions" / "agent_transaction.xml"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text(
        "<AgentTransaction>"
        "<transaction_id>AGT-TX-LOAN-000014</transaction_id>"
        "<agent_id>AGT-000001</agent_id>"
        "<loan_id>LOAN-000014</loan_id>"
        "<transaction_timestamp>2026-09-01T13:00:00+03:00</transaction_timestamp>"
        "<amount>250000</amount>"
        "</AgentTransaction>",
        encoding="utf-8",
    )

    first = producer.parse_event(str(source))
    second = producer.parse_event(str(source))

    assert first is not None
    assert second is not None
    assert first["event_id"] == "AGT-TX-LOAN-000014"
    assert second["event_id"] == first["event_id"]
    assert first["message_id"] == "AGT-TX-LOAN-000014"


def test_agent_transaction_missing_identity_is_rejected(tmp_path):
    source = tmp_path / "agent_network" / "agent_transactions" / "agent_transaction.xml"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text(
        "<AgentTransaction>"
        "<agent_id>AGT-000001</agent_id>"
        "<loan_id>LOAN-000014</loan_id>"
        "<amount>250000</amount>"
        "</AgentTransaction>",
        encoding="utf-8",
    )

    assert producer.parse_event(str(source)) is None


def test_pdmis_snapshot_replay_and_observation_identity(tmp_path):
    source = tmp_path / "pdmis" / "loans" / "loans.json"
    source.parent.mkdir(parents=True, exist_ok=True)

    records = [
        {"loan_id": "LOAN-000014", "as_of_date": "2026-09-08",
         "loan_status": "APPROVED"},
        {"loan_id": "LOAN-000015", "as_of_date": "2026-09-08",
         "loan_status": "DISBURSED"},
    ]
    source.write_text(json.dumps(records), encoding="utf-8")

    first = producer.parse_json_events(str(source))
    second = producer.parse_json_events(str(source))

    assert len(first) == 2
    assert [e["event_id"] for e in first] == [
        e["event_id"] for e in second
    ]
    assert first[0]["event_id"] != first[1]["event_id"]
    assert producer.select_business_key("pdmis.loans", first[0]) == "LOAN-000014"

    records[0]["as_of_date"] = "2026-09-09"
    source.write_text(json.dumps(records), encoding="utf-8")
    changed = producer.parse_json_events(str(source))
    assert changed[0]["event_id"] != first[0]["event_id"]


def test_pdmis_beneficiary_snapshot_identity(tmp_path):
    source = tmp_path / "pdmis" / "beneficiaries" / "beneficiaries.json"
    source.parent.mkdir(parents=True, exist_ok=True)

    record = {
        "beneficiary_id": "BEN-00014",
        "updated_at": "2026-09-08T17:00:00+03:00",
        "is_active": True,
    }
    source.write_text(json.dumps([record]), encoding="utf-8")

    first = producer.parse_json_events(str(source))
    second = producer.parse_json_events(str(source))

    assert len(first) == 1
    assert first[0]["event_id"] == second[0]["event_id"]
    assert producer.select_business_key(
        "pdmis.beneficiaries", first[0]
    ) == "BEN-00014"


def test_pdmis_conflicting_snapshots_fail_closed(tmp_path):
    source = tmp_path / "pdmis" / "loans" / "loans.json"
    source.parent.mkdir(parents=True, exist_ok=True)

    records = [
        {"loan_id": "LOAN-000014", "as_of_date": "2026-09-08",
         "loan_status": "APPROVED"},
        {"loan_id": "LOAN-000014", "as_of_date": "2026-09-08",
         "loan_status": "DISBURSED"},
    ]
    source.write_text(json.dumps(records), encoding="utf-8")

    with pytest.raises(producer.SourceValidationError):
        producer.parse_json_events(str(source))


def test_pdmis_snapshot_missing_observation_date_fails_closed(tmp_path):
    source = tmp_path / "pdmis" / "loans" / "loans.json"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text(
        json.dumps([{"loan_id": "LOAN-000014"}]),
        encoding="utf-8",
    )

    with pytest.raises(producer.SourceValidationError):
        producer.parse_json_events(str(source))


def test_pdmis_remaining_entity_identities(tmp_path):
    cases = [
        (
            "households", "household_id", "HH-00014",
            {"updated_at": "2026-09-08T17:00:00+03:00"},
        ),
        (
            "saccos", "sacco_id", "SACCO-001",
            {"updated_at": "2026-09-08T17:00:00+03:00"},
        ),
        (
            "business_plans", "business_plan_id", "BP-00014",
            {"approval_status": "APPROVED"},
        ),
        (
            "special_groups", "group_code", "WOMEN",
            {"group_name": "Women"},
        ),
    ]

    for dataset, key, entity_id, attributes in cases:
        source = tmp_path / "pdmis" / dataset / f"{dataset}.json"
        source.parent.mkdir(parents=True, exist_ok=True)

        record = {key: entity_id, **attributes}
        source.write_text(json.dumps([record]), encoding="utf-8")

        first = producer.parse_json_events(str(source))
        second = producer.parse_json_events(str(source))

        assert len(first) == 1
        assert first[0]["event_id"] == second[0]["event_id"]
        assert producer.select_business_key(
            f"pdmis.{dataset}", first[0]
        ) == entity_id


def test_pdmis_content_version_changes_identity(tmp_path):
    for dataset, key, entity_id, field, before, after in [
        (
            "business_plans", "business_plan_id", "BP-00014",
            "approval_status", "PENDING", "APPROVED",
        ),
        (
            "special_groups", "group_code", "WOMEN",
            "group_name", "Women", "Women and Girls",
        ),
    ]:
        source = tmp_path / "pdmis" / dataset / f"{dataset}.json"
        source.parent.mkdir(parents=True, exist_ok=True)

        record = {key: entity_id, field: before}
        source.write_text(json.dumps([record]), encoding="utf-8")
        first = producer.parse_json_events(str(source))

        record[field] = after
        source.write_text(json.dumps([record]), encoding="utf-8")
        second = producer.parse_json_events(str(source))

        assert len(first) == len(second) == 1
        assert first[0]["event_id"] != second[0]["event_id"]


def test_pdmis_dated_snapshot_conflict_fails_closed(tmp_path):
    for dataset, key, entity_id in [
        ("households", "household_id", "HH-00014"),
        ("saccos", "sacco_id", "SACCO-001"),
    ]:
        source = tmp_path / "pdmis" / dataset / f"{dataset}.json"
        source.parent.mkdir(parents=True, exist_ok=True)

        records = [
            {
                key: entity_id,
                "updated_at": "2026-09-08T17:00:00+03:00",
                "is_active": True,
            },
            {
                key: entity_id,
                "updated_at": "2026-09-08T17:00:00+03:00",
                "is_active": False,
            },
        ]

        source.write_text(json.dumps(records), encoding="utf-8")
        with pytest.raises(producer.SourceValidationError):
            producer.parse_json_events(str(source))


@pytest.mark.parametrize(
    "relative_path,content",
    [
        ("pdmis/loans/loans.json", "{invalid"),
        ("pdmis/loans/loans.json", '[{"loan_id":"L1"},42]'),
        ("wendi/transactions/transactions.json",
         '[{"event_id":"VALID"},{"amount":10}]'),
        ("pdmis/repayments/repayments.json",
         '[{"repayment_event_id":"VALID"},{"amount":10}]'),
        ("unknown/source.json", '{"event_id":"X"}'),
    ],
)
def test_json_source_validation_rejects_entire_file(
    tmp_path, relative_path, content
):
    source = tmp_path / relative_path
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text(content, encoding="utf-8")

    with pytest.raises(producer.SourceValidationError):
        producer.parse_json_events(str(source))

def test_main_returns_failure_for_invalid_json_without_kafka_failure(
    tmp_path, monkeypatch
):
    source = tmp_path / "pdmis" / "loans" / "loans.json"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("{invalid", encoding="utf-8")

    class FakeKafkaProducer:
        def __init__(self, config):
            self.produced = []

        def produce(self, **kwargs):
            self.produced.append(kwargs)

        def poll(self, timeout):
            return 0

        def flush(self):
            return 0

    fake_producer = FakeKafkaProducer({})

    monkeypatch.setattr(producer.Config, "DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(producer, "load_avro_schema", lambda: "{}")
    monkeypatch.setattr(
        producer, "validate_kafka_security", lambda **kwargs: None
    )
    monkeypatch.setattr(
        producer, "validate_schema_registry_security",
        lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        producer, "SchemaRegistryClient", lambda config: object()
    )
    monkeypatch.setattr(
        producer, "AvroSerializer", lambda **kwargs: object()
    )
    monkeypatch.setattr(
        producer, "SerializingProducer", lambda config: fake_producer
    )

    assert producer.main() == 1
    assert fake_producer.produced == []
