"""Gate 3.6C offline replay-control demonstration tests."""

from pathlib import Path

from services.shared import replay_ledger_demo as demo


def test_demo_has_exactly_ten_synthetic_scenarios(capsys):
    rc = demo.main()
    output = capsys.readouterr().out

    assert rc == 0
    assert "TOTAL_SCENARIOS=10" in output
    assert "PASSED=10" in output
    assert "FAILED=0" in output


def test_demo_reports_no_live_execution(capsys):
    rc = demo.main()
    output = capsys.readouterr().out

    assert rc == 0
    assert "KAFKA_MESSAGES_REPLAYED=0" in output
    assert "OFFSETS_RESET=0" in output
    assert "LIVE_INFRASTRUCTURE_CONNECTIONS=0" in output
    assert "DURABLE_LIVE_LEDGER_CREATED=0" in output


def test_demo_exercises_duplicate_suppression(capsys):
    rc = demo.main()
    output = capsys.readouterr().out

    assert rc == 0
    assert (
        "duplicate reservation | "
        "expected=SUPPRESSED_DUPLICATE | "
        "actual=SUPPRESSED_DUPLICATE | PASS"
    ) in output


def test_demo_exercises_duplicate_audit(capsys):
    rc = demo.main()
    output = capsys.readouterr().out

    assert rc == 0
    assert (
        "duplicate audit | "
        "expected=SUPPRESSED_DUPLICATE | "
        "actual=SUPPRESSED_DUPLICATE | PASS"
    ) in output


def test_demo_exercises_fail_closed_approval(capsys):
    rc = demo.main()
    output = capsys.readouterr().out

    assert rc == 0
    assert "missing approval | expected=BLOCKED | actual=BLOCKED | PASS" in output


def test_demo_exercises_fail_closed_policy(capsys):
    rc = demo.main()
    output = capsys.readouterr().out

    assert rc == 0
    assert "unresolved policy | expected=BLOCKED | actual=BLOCKED | PASS" in output


def test_demo_exercises_privacy_block(capsys):
    rc = demo.main()
    output = capsys.readouterr().out

    assert rc == 0
    assert "protected metadata | expected=BLOCKED | actual=BLOCKED | PASS" in output


def test_demo_exercises_terminal_success_protection(capsys):
    rc = demo.main()
    output = capsys.readouterr().out

    assert rc == 0
    assert (
        "terminal success protection | "
        "expected=BLOCKED | actual=BLOCKED | PASS"
    ) in output


def test_demo_uses_existing_replay_ledger():
    source = Path(demo.__file__).read_text()

    assert "from services.shared.replay_ledger import" in source
    assert "ReplayLedger" in source


def test_demo_contains_no_live_infrastructure_capability():
    source = Path(demo.__file__).read_text().lower()

    forbidden = (
        "confluent_kafka",
        "boto3",
        "producer.produce",
        "consumer.seek",
        "consumer.commit",
        "docker compose",
        "subprocess",
        "requests.",
        "socket.",
        "minio.",
        "nessie",
        "trino",
        "duckdb",
    )

    for capability in forbidden:
        assert capability not in source
