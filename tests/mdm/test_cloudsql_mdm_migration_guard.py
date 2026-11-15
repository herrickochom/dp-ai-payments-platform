"""Offline migration safety tests; no database connection or cloud calls."""
import json
from pathlib import Path
import pytest
from services.shared.mdm.postgres_store import TABLES, persist_mdm

ROOT = Path(__file__).resolve().parents[2]

def test_full_replacement_denied_by_default(monkeypatch):
    for name in ("PDM_SOURCE_HOST", "PDM_SOURCE_DB", "PDM_SOURCE_USER", "PDM_SOURCE_PASSWORD"):
        monkeypatch.setenv(name, "offline-test")
    monkeypatch.delenv("PDM_MDM_FULL_REPLACE_APPROVED", raising=False)
    with pytest.raises(RuntimeError, match="Full MDM replacement DENIED"):
        persist_mdm({name: [] for name in TABLES})

def test_full_replacement_denied_without_cloudsql_target(monkeypatch):
    for name in ("PDM_SOURCE_HOST", "PDM_SOURCE_DB", "PDM_SOURCE_USER", "PDM_SOURCE_PASSWORD"):
        monkeypatch.setenv(name, "offline-test")
    monkeypatch.setenv("PDM_MDM_FULL_REPLACE_APPROVED", "I_APPROVE_FULL_MDM_REPLACEMENT")
    monkeypatch.delenv("PDM_MDM_TARGET", raising=False)
    with pytest.raises(RuntimeError, match="target must be cloudsql"):
        persist_mdm({name: [] for name in TABLES})

def test_cdc_remains_denied_and_sacco_only():
    contract=json.loads((ROOT/"platform/cdc/contracts/pdm_cdc_contract.json").read_text())
    connector=json.loads((ROOT/"platform/cdc/debezium/templates/pdm_mdm_sacco_connector.json").read_text())["config"]
    assert contract["activation_policy"]["default"] == "DENY"
    assert contract["cdc_sources"] == []
    assert connector["table.include.list"] == "mdm.sacco_master_sources"
    assert connector["publication.autocreate.mode"] == "disabled"

def test_ten_tables_preserved():
    assert len(TABLES) == 10
    assert "golden_beneficiaries_restricted" in TABLES
    assert "source_crosswalk" in TABLES
