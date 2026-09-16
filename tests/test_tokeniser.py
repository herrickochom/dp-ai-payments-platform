"""Gate 2 tokeniser tests: canonical HMAC contract, rotation, leakage guards."""
from __future__ import annotations

import hashlib
import hmac
import logging
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "shared"))

import data_protection as dp
from tokeniser_job import (
    TOKEN_CONTEXT,
    TokenisationVersionMissing,
    build_token_links,
    canonical_token_for,
    rows_from_bronze_records,
)

TEST_KEY = "gate2-offline-test-key-only"
TEST_VERSION = "vTest1"
OTHER_KEY = "gate2-offline-rotation-key-only"
OTHER_VERSION = "vTest2"

BENEFICIARIES = [f"BEN-{i:06d}" for i in range(1, 251)]


def _expected(beneficiary_id: str, key: str, version: str) -> str:
    digest = hmac.new(
        key.encode(),
        f"{TOKEN_CONTEXT}||{beneficiary_id}".encode(),
        hashlib.sha256,
    ).hexdigest()
    return f"{version}:{digest}"


@pytest.fixture()
def keyed_env(monkeypatch):
    monkeypatch.setenv("DP_TOKEN_KEY", TEST_KEY)
    monkeypatch.setenv("DP_TOKEN_KEY_VERSION", TEST_VERSION)
    return TEST_KEY, TEST_VERSION


def test_canonical_contract_shape_and_domain(keyed_env):
    token = canonical_token_for("BEN-000001", version=TEST_VERSION)
    assert re.fullmatch(r"vTest1:[0-9a-f]{64}", token)
    assert token == _expected("BEN-000001", TEST_KEY, TEST_VERSION)
    assert token != hashlib.sha256(b"BEN-000001").hexdigest()


def test_population_250_unique_no_collisions(keyed_env):
    rows = build_token_links(BENEFICIARIES, version=TEST_VERSION)
    tokens = [r.beneficiary_token for r in rows]
    assert len(rows) == 250
    assert len(set(BENEFICIARIES)) == 250
    assert len(set(tokens)) == 250
    assert all(t for t in tokens)
    assert len({r.beneficiary_key_internal for r in rows}) == 250


def test_determinism_and_distinctness(keyed_env):
    assert canonical_token_for("BEN-000007", version=TEST_VERSION) == canonical_token_for(
        "BEN-000007", version=TEST_VERSION
    )
    assert canonical_token_for("BEN-000007", version=TEST_VERSION) != canonical_token_for(
        "BEN-000008", version=TEST_VERSION
    )


def test_missing_key_fails_closed(monkeypatch):
    monkeypatch.delenv("DP_TOKEN_KEY", raising=False)
    monkeypatch.setenv("DP_TOKEN_KEY_VERSION", TEST_VERSION)
    with pytest.raises(dp.TokenisationKeyMissing):
        canonical_token_for("BEN-000001", version=TEST_VERSION)


def test_missing_version_fails_closed(keyed_env):
    with pytest.raises(TokenisationVersionMissing):
        canonical_token_for("BEN-000001", version="  ")


def test_rotation_coexistence_and_active_selection(monkeypatch):
    monkeypatch.setenv("DP_TOKEN_KEY", TEST_KEY)
    v1 = build_token_links(["BEN-000001"], version=TEST_VERSION)
    monkeypatch.setenv("DP_TOKEN_KEY", OTHER_KEY)
    v2 = build_token_links(["BEN-000001"], version=OTHER_VERSION)
    assert v1[0].beneficiary_token != v2[0].beneficiary_token
    assert v1[0].beneficiary_token.startswith(f"{TEST_VERSION}:")
    assert v2[0].beneficiary_token.startswith(f"{OTHER_VERSION}:")
    active = {r.token_version: r for r in v1 + v2}[OTHER_VERSION]
    assert active.is_active and active.token_version == OTHER_VERSION


def test_legacy_token_is_not_canonical(keyed_env):
    legacy = "TOKEN-" + hashlib.sha256(b"CM9000000100X").hexdigest()[:16].upper()
    canonical = canonical_token_for("BEN-000001", version=TEST_VERSION)
    assert legacy.startswith("TOKEN-")
    assert not canonical.startswith("TOKEN-")
    assert legacy != canonical


def test_no_clear_id_or_secret_in_logs(keyed_env, caplog):
    logger = logging.getLogger("gate2.tokeniser.test")
    with caplog.at_level(logging.INFO):
        token = canonical_token_for("BEN-000001", version=TEST_VERSION)
        logger.info("tokenised beneficiary batch", extra={"count": 1})
    rendered = caplog.text
    assert "BEN-000001" not in rendered
    assert TEST_KEY not in rendered
    assert token not in rendered


def test_bronze_row_extraction_rejects_missing_id():
    with pytest.raises(ValueError):
        rows_from_bronze_records([{"beneficiary_id": "BEN-000001"}, {"beneficiary_id": " "}])
    assert rows_from_bronze_records([{"beneficiary_id": "BEN-000001"}]) == ["BEN-000001"]
