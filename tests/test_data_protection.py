"""Gate 2 data-protection tests (non-destructive, fail closed)."""

from __future__ import annotations

import hashlib
import os
import re
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "shared"))

import data_protection as dp
from identity_resolver import IdentityAccessDenied, IdentityResolver


PART1 = ROOT / "platform/config/governance/data_classification_part1.yaml"
PART2 = ROOT / "platform/config/governance/data_classification_part2.yaml"
DBT = ROOT / "transform" / "dbt" / "models"


def load_part(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def sql_text(*rel: str) -> str:
    return DBT.joinpath(*rel).read_text()


def test_registry_valid_and_single_sourced():
    p1, p2 = load_part(PART1), load_part(PART2)

    assert set(p1["classifications"]) == {
        "PUBLIC",
        "INTERNAL",
        "CONFIDENTIAL",
        "RESTRICTED",
    }
    assert "DIRECT_IDENTIFIER" in p1["data_classes"]

    fields = {f["field"]: f for f in p1["fields"]}
    fields.update({f["field"]: f for f in p2["fields2"]})

    required = {
        "beneficiary_name",
        "nin",
        "phone",
        "email",
        "beneficiary_id",
        "x_beneficiary_sa",
        "x_wallet_reference",
        "debtor_account_id",
        "creditor_account_id",
        "account_id",
    }
    assert required <= set(fields)

    for name, f in fields.items():
        assert f["classification"] in p1["classifications"], name
        for key in (
            "permitted_layers",
            "tokenisation",
            "masking",
            "logging",
            "consumption",
        ):
            assert f[key], name

    assert p2["tokenisation_standard"]["algorithm"] == "HMAC-SHA256"
    assert p2["tokenisation_standard"]["missing_key"] == "fail-closed"


def test_restricted_fields_identified():
    fields = {f["field"]: f for f in load_part(PART1)["fields"]}

    for name in (
        "beneficiary_name",
        "nin",
        "phone",
        "beneficiary_id",
        "x_beneficiary_sa",
        "debtor_account_id",
        "creditor_account_id",
    ):
        assert fields[name]["classification"] == "RESTRICTED", name


def test_keyed_tokenisation_deterministic_and_distinct(monkeypatch):
    monkeypatch.setenv("DP_TOKEN_KEY", "gate2-test-key")
    monkeypatch.setenv("DP_TOKEN_KEY_VERSION", "v1")

    assert dp.keyed_token("BEN-000001") == dp.keyed_token("BEN-000001")
    assert dp.keyed_token("BEN-000001") != dp.keyed_token("BEN-000002")
    assert (
        dp.keyed_token("BEN-000001")
        != hashlib.sha256(b"BEN-000001").hexdigest()
    )


def test_missing_key_fails_closed(monkeypatch):
    monkeypatch.delenv("DP_TOKEN_KEY", raising=False)

    with pytest.raises(dp.TokenisationKeyMissing):
        dp.keyed_token("BEN-000001")


def test_masking_helpers():
    assert dp.mask_phone("256772123456").endswith("3456")
    assert dp.mask_national_id("CM12345678").endswith("678")
    assert dp.mask_email("jane.doe@example.ug") == "j***@example.ug"
    assert dp.mask_name("Jane Doe") == "***"


def test_logs_do_not_expose_protected_values():
    clean = dp.redact_for_log(
        "contact jane.doe@example.ug on +256772123456"
    )

    assert "jane.doe@example.ug" not in clean
    assert "+256772123456" not in clean
    assert "REDACTED" in clean


def test_payment_correlation_preserved_after_protection(monkeypatch):
    monkeypatch.setenv("DP_TOKEN_KEY", "gate2-test-key")

    pmn = {
        "correlation_id": "C-1",
        "uetr": "U-1",
        "x_beneficiary_sa": "SA-9",
    }
    plm = {
        "correlation_id": "C-1",
        "uetr": "U-1",
        "x_beneficiary_sa": "SA-9",
    }

    for key in ("correlation_id", "uetr"):
        assert pmn[key] == plm[key]

    assert (
        dp.keyed_token(pmn["x_beneficiary_sa"])
        == dp.keyed_token(plm["x_beneficiary_sa"])
    )
    assert "SA-9" not in (dp.keyed_token("SA-9") or "")


def test_phase6_pmn_plm_semantics_intact():
    pmn = sql_text(
        "bronze",
        "br_pdm_payments_pmn_lifecycle_events.sql",
    )
    plm = sql_text(
        "bronze",
        "br_pdm_payments_plm_lifecycle_events.sql",
    )
    uni = sql_text(
        "silver",
        "slv_pdm_payment_technical_events.sql",
    )

    # Bronze preserves the source-specific PMN/PLM semantics.
    assert "x_beneficiary_sa" in plm
    assert "x_wallet_reference" in plm
    assert "x_beneficiary_sa" not in pmn

    # Silver preserves technical correlation/provenance while enforcing
    # the Gate 2 privacy boundary. The direct PLM beneficiary identifier
    # must not propagate into the ordinary analytical Silver layer.
    assert "x_beneficiary_sa" not in uni
    assert "technical_source" in uni
    assert "correlation_id" in uni
    assert "uetr" in uni


def test_restricted_identity_fails_closed_without_authorisation():
    resolver = IdentityResolver(
        authorised_subjects={"governance-admin"}
    )

    with pytest.raises(IdentityAccessDenied):
        resolver.resolve(
            subject="random-analyst",
            token="v1:abc",
            purpose="curiosity",
        )

    assert resolver.audit_log
    assert resolver.audit_log[0]["decision"] == "DENY"


def _gap_guard(model_rel, pattern):
    text = sql_text(*model_rel)

    assert not re.search(pattern, text), (
        "ENFORCEMENT GAP "
        "(STOP: needs approved Iceberg replacement): "
        + "/".join(model_rel)
        + " still matches "
        + pattern
    )


def test_gap_silver_clear_identity_requires_vault():
    _gap_guard(
        ("silver", "slv_pdm_beneficiaries.sql"),
        r"(?i)\b(beneficiary_name|\bnin\b|date_of_birth)\b",
    )


def test_gap_gold_no_direct_identifiers_or_plain_sha256():
    _gap_guard(
        ("gold", "portfolio", "gld_dim_pdm_beneficiary.sql"),
        r"(?i)\bbeneficiary_name\b",
    )
    _gap_guard(
        ("gold", "portfolio", "gld_dim_pdm_beneficiary.sql"),
        r"sha256\s*\(",
    )


def test_gap_consumption_no_beneficiary_identity():
    _gap_guard(
        (
            "consumption",
            "leadership",
            "cns_pdm_beneficiary_insights.sql",
        ),
        r"(?i)\bbeneficiary_id\b",
    )
    _gap_guard(
        (
            "consumption",
            "operations",
            "cns_pdm_payment_operations.sql",
        ),
        r"(?i)\bbeneficiary_id\b",
    )
    _gap_guard(
        (
            "consumption",
            "investigators",
            "cns_pdm_ai_default_risk.sql",
        ),
        r"(?i)\bbeneficiary_id\b",
    )


def test_gap_trino_not_world_open():
    rules = (
        ROOT / "platform/trino/etc/rules.json"
    ).read_text()

    assert '"user": ".*"' not in rules, (
        "ENFORCEMENT GAP "
        "(STOP: approved Trino rules replacement needed)"
    )