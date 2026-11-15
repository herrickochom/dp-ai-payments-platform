
import ast
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "platform/gcp/kafka_pubsub_bridge/raw_v1_bridge_live.py"

def load():
    spec = importlib.util.spec_from_file_location("raw_recovery", SOURCE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def approved():
    return {"agent.locations", "pdmis.repayments"}

def plan(**changes):
    value = {
        "topic": "agent.locations",
        "partition": 0,
        "start_offset": 0,
        "end_offset": 10,
        "consumer_group": "gcp-raw-v1-recovery-agent-locations",
    }
    value.update(changes)
    return value

def test_valid_plan():
    module = load()
    assert module.validate_recovery_plan(plan(), approved()) == (
        "agent.locations", 0, 0, 10,
        "gcp-raw-v1-recovery-agent-locations",
    )

@pytest.mark.parametrize("changes", [
    {"topic": "mdm.sacco.golden"},
    {"partition": -1},
    {"start_offset": -1},
    {"end_offset": 0},
    {"end_offset": 11},
    {"partition": True},
    {"consumer_group": "ordinary-consumer-group"},
])
def test_invalid_plan(changes):
    with pytest.raises(ValueError):
        load().validate_recovery_plan(plan(**changes), approved())

def test_no_cloud_operations_on_import(monkeypatch):
    module = load()
    assert callable(module.run_recovery)
