from __future__ import annotations

import json
from pathlib import Path

from orchestration.transform_runtime.execution_policy import (
    EXECUTION_UNITS,
)


ROOT = Path(__file__).resolve().parents[3]
CONTRACT = (
    ROOT
    / "orchestration"
    / "transform_runtime"
    / "contracts"
    / "lakehouse_transform.json"
)


def load_contract() -> dict:
    return json.loads(CONTRACT.read_text())


def test_execution_model_allowlists_are_exhaustive_and_disjoint():
    contract = load_contract()

    manifest = {
        model["unique_id"]
        for model in contract["models"]
    }

    allowlists = [
        set(unit.model_allowlist)
        for unit in EXECUTION_UNITS.values()
    ]

    union = set().union(*allowlists)

    assert union == manifest
    assert sum(map(len, allowlists)) == len(union)


def test_execution_model_allowlist_cardinalities():
    assert len(
        EXECUTION_UNITS["raw_to_bronze"].model_allowlist
    ) == 48

    assert len(
        EXECUTION_UNITS["ml_derived_bronze"].model_allowlist
    ) == 1

    assert len(
        EXECUTION_UNITS["restricted_identity"].model_allowlist
    ) == 3

    assert len(
        EXECUTION_UNITS["ordinary_analytics"].model_allowlist
    ) == 73


def test_ml_bronze_is_not_owned_by_raw_to_bronze():
    model = "model.pdm_platform.br_pdm_ai_default_risk"

    assert model in (
        EXECUTION_UNITS[
            "ml_derived_bronze"
        ].model_allowlist
    )

    assert model not in (
        EXECUTION_UNITS[
            "raw_to_bronze"
        ].model_allowlist
    )


def test_restricted_identity_owns_only_silver_vault():
    contract = load_contract()

    schema_by_id = {
        model["unique_id"]: model["schema"]
        for model in contract["models"]
    }

    restricted = EXECUTION_UNITS[
        "restricted_identity"
    ].model_allowlist

    assert restricted

    assert {
        schema_by_id[model]
        for model in restricted
    } == {"silver_vault"}


def test_ownership_units_do_not_encode_job_prerequisites():
    for unit in EXECUTION_UNITS.values():
        assert not hasattr(unit, "prerequisites")


def test_execution_units_remain_selector_closed():
    for unit in EXECUTION_UNITS.values():
        assert unit.model_allowlist
        assert unit.allows_runtime_graph_expansion is False
        assert unit.allows_plus_selector is False
        assert unit.allows_tag_selector is False
        assert unit.allows_arbitrary_dbt_args is False
