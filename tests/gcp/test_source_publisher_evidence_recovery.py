import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PUBLISHER = ROOT / "services/payment-producer/source_publisher.py"


def source():
    return PUBLISHER.read_text(encoding="utf-8")


def test_publisher_compiles():
    ast.parse(source())


def test_historical_records_are_loaded():
    text = source()
    assert "prior_records = list(records)" in text
    assert 'prior_ids = {m["event_id"] for m in prior_records}' in text


def test_initial_evidence_is_not_overwritten():
    text = source()
    section = text.split("def run_live(args)", 1)[1]
    section = section.split("reg = SchemaRegistryClient", 1)[0]
    assert "ev.message_ids = list(prior_records)" in section
    assert "write_evidence(ev_path, ev)" not in section


def test_delivery_merge_is_preserved():
    text = source()
    assert "ev.message_ids = list(delivered)" in text
    assert text.count("if prior_records:") == 2


def test_corrupt_evidence_fails_closed():
    text = source()
    assert "historical evidence cannot be safely loaded" in text
    assert "message_ids must be a list" in text
    assert "invalid historical delivery record" in text
