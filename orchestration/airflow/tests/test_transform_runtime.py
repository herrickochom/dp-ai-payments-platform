from __future__ import annotations

import importlib
import inspect
import json
from pathlib import Path

import pytest

import orchestration.transform_runtime.execution_plan as execution_plan
from orchestration.transform_runtime.execution_plan import (
    APPROVED_AUTHORITIES,
    EXECUTION_BATCHES,
    PROTECTED_EXTERNAL_PREREQUISITES,
    TOKEN_LINK,
    UGANDA_DISTRICT_GEOJSON,
    ExecutionPlanError,
    validate_execution_batch,
)
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


def test_transform_contract_has_single_canonical_owner():
    duplicate = (
        ROOT
        / "orchestration"
        / "job_runner"
        / "contracts"
        / "lakehouse_transform.json"
    )

    assert CONTRACT.is_file()
    assert not duplicate.exists()


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


def test_execution_plan_has_exact_immutable_batch_registry():
    assert set(EXECUTION_BATCHES) == {
        "C4_ML_01",
        "C4_RAW_02",
        "C4_RID_FOUNDATION_03",
        "C4_ORD_FOUNDATION_04",
        "C4_ORD_BRIDGE_05",
        "C4_ORD_PRE_ALERT_06",
        "C4_RID_ALERTS_07",
        "C4_ORD_PRE_SIGNAL_08",
        "C4_RID_SIGNALS_09",
        "C4_ORD_DOWNSTREAM_10",
    }

    with pytest.raises(TypeError):
        EXECUTION_BATCHES["unknown"] = object()


def test_execution_batches_have_approved_authority_and_models():
    for batch_id, batch in EXECUTION_BATCHES.items():
        assert batch.batch_id == batch_id
        assert batch.authority in APPROVED_AUTHORITIES
        assert batch.model_allowlist
        assert batch.contract_waves


def test_execution_batch_cardinalities_and_prerequisites_are_exact():
    expected = {
        "C4_ML_01": (1, frozenset()),
        "C4_RAW_02": (48, frozenset()),
        "C4_RID_FOUNDATION_03": (
            1,
            frozenset({"C4_RAW_02"}),
        ),
        "C4_ORD_FOUNDATION_04": (
            17,
            frozenset({"C4_RAW_02"}),
        ),
        "C4_ORD_BRIDGE_05": (
            18,
            frozenset({
                "C4_ML_01",
                "C4_RAW_02",
                "C4_RID_FOUNDATION_03",
                "C4_ORD_FOUNDATION_04",
            }),
        ),
        "C4_ORD_PRE_ALERT_06": (
            8,
            frozenset({
                "C4_ORD_FOUNDATION_04",
                "C4_ORD_BRIDGE_05",
            }),
        ),
        "C4_RID_ALERTS_07": (
            1,
            frozenset({
                "C4_RID_FOUNDATION_03",
                "C4_ORD_FOUNDATION_04",
                "C4_ORD_BRIDGE_05",
            }),
        ),
        "C4_ORD_PRE_SIGNAL_08": (
            6,
            frozenset({
                "C4_ORD_FOUNDATION_04",
                "C4_ORD_BRIDGE_05",
                "C4_ORD_PRE_ALERT_06",
            }),
        ),
        "C4_RID_SIGNALS_09": (
            1,
            frozenset({"C4_RID_ALERTS_07"}),
        ),
        "C4_ORD_DOWNSTREAM_10": (
            24,
            frozenset({
                "C4_ORD_FOUNDATION_04",
                "C4_ORD_BRIDGE_05",
                "C4_ORD_PRE_ALERT_06",
                "C4_ORD_PRE_SIGNAL_08",
                "C4_RID_SIGNALS_09",
            }),
        ),
    }

    assert {
        batch_id: (
            len(batch.model_allowlist),
            batch.prerequisite_batches,
        )
        for batch_id, batch in EXECUTION_BATCHES.items()
    } == expected


def test_execution_batch_models_are_exhaustive_and_disjoint():
    contract = load_contract()
    manifest = {
        model["unique_id"]
        for model in contract["models"]
    }
    allowlists = [
        set(batch.model_allowlist)
        for batch in EXECUTION_BATCHES.values()
    ]
    union = set().union(*allowlists)

    assert len(union) == 125
    assert sum(map(len, allowlists)) == 125
    assert union == manifest


def test_every_batch_model_matches_its_ownership_authority():
    for batch in EXECUTION_BATCHES.values():
        unit = EXECUTION_UNITS[batch.ownership_unit]

        assert batch.authority == unit.authority
        assert batch.model_allowlist <= unit.model_allowlist


def test_execution_batch_prerequisites_exist_and_are_acyclic():
    remaining = set(EXECUTION_BATCHES)
    completed = set()

    for batch_id, batch in EXECUTION_BATCHES.items():
        assert batch_id not in batch.prerequisite_batches
        assert batch.prerequisite_batches <= set(EXECUTION_BATCHES)

    while remaining:
        ready = {
            batch_id
            for batch_id in remaining
            if EXECUTION_BATCHES[
                batch_id
            ].prerequisite_batches <= completed
        }

        assert ready
        completed.update(ready)
        remaining.difference_update(ready)

    assert completed == set(EXECUTION_BATCHES)


def test_contract_model_dependencies_are_closed_by_batches():
    contract = load_contract()
    model_to_batch = {
        model: batch_id
        for batch_id, batch in EXECUTION_BATCHES.items()
        for model in batch.model_allowlist
    }
    model_to_wave = {
        model: wave["wave"]
        for wave in contract["topological_waves"]
        for model in wave["models"]
    }

    for model in contract["models"]:
        child = model["unique_id"]
        child_batch_id = model_to_batch[child]
        child_batch = EXECUTION_BATCHES[child_batch_id]

        assert model_to_wave[child] in child_batch.contract_waves

        for parent in model["parents"]:
            parent_batch_id = model_to_batch[parent]

            if parent_batch_id == child_batch_id:
                assert model_to_wave[parent] <= model_to_wave[child]
            else:
                assert (
                    parent_batch_id
                    in child_batch.prerequisite_batches
                )


def test_raw_batch_preserves_same_runtime_boundary():
    contract = load_contract()
    schema_by_id = {
        model["unique_id"]: model["schema"]
        for model in contract["models"]
    }
    batch = EXECUTION_BATCHES["C4_RAW_02"]
    schemas = [
        schema_by_id[model]
        for model in batch.model_allowlist
    ]

    assert len(batch.model_allowlist) == 48
    assert schemas.count("staging") == 23
    assert schemas.count("bronze") == 25
    assert batch.same_runtime_required is True


def test_ml_model_occurs_only_in_ml_batch():
    model = "model.pdm_platform.br_pdm_ai_default_risk"
    owners = {
        batch_id
        for batch_id, batch in EXECUTION_BATCHES.items()
        if model in batch.model_allowlist
    }

    assert owners == {"C4_ML_01"}
    assert (
        EXECUTION_BATCHES["C4_ML_01"].authority
        == "ml_prediction_transform"
    )


def test_restricted_identity_models_have_exact_batches():
    expected = {
        "model.pdm_platform.vlt_pdm_beneficiary_identity": (
            "C4_RID_FOUNDATION_03"
        ),
        "model.pdm_platform.vlt_pdm_beneficiary_identity_alerts": (
            "C4_RID_ALERTS_07"
        ),
        "model.pdm_platform.vlt_pdm_beneficiary_identity_signals": (
            "C4_RID_SIGNALS_09"
        ),
    }

    for model, expected_batch in expected.items():
        actual = {
            batch_id
            for batch_id, batch in EXECUTION_BATCHES.items()
            if model in batch.model_allowlist
        }

        assert actual == {expected_batch}
        assert (
            EXECUTION_BATCHES[expected_batch].authority
            == "restricted_identity_transform"
        )


def test_restricted_consumption_outputs_keep_ordinary_authority():
    models = {
        "model.pdm_platform.cns_pdm_lifecycle_exception_cases",
        "model.pdm_platform.cns_pdm_end_to_end_traceability",
        "model.pdm_platform.cns_pdm_fraud_risk_insights",
    }

    for model in models:
        batches = [
            batch
            for batch in EXECUTION_BATCHES.values()
            if model in batch.model_allowlist
        ]

        assert len(batches) == 1
        assert batches[0].authority == "ordinary_transform"


def test_protected_external_prerequisites_are_read_only_inputs():
    contract = load_contract()
    declared = {
        prerequisite["unique_id"]
        for prerequisite in contract["external_prerequisites"]
        if prerequisite["mutation_allowed"] is False
    }
    executable_models = set().union(*(
        batch.model_allowlist
        for batch in EXECUTION_BATCHES.values()
    ))

    assert PROTECTED_EXTERNAL_PREREQUISITES == declared
    assert PROTECTED_EXTERNAL_PREREQUISITES.isdisjoint(
        executable_models
    )
    assert EXECUTION_BATCHES[
        "C4_RID_FOUNDATION_03"
    ].external_prerequisites == frozenset({TOKEN_LINK})
    assert EXECUTION_BATCHES[
        "C4_ORD_PRE_SIGNAL_08"
    ].external_prerequisites == frozenset({
        UGANDA_DISTRICT_GEOJSON,
    })
    assert EXECUTION_BATCHES[
        "C4_ORD_DOWNSTREAM_10"
    ].external_prerequisites == frozenset({
        UGANDA_DISTRICT_GEOJSON,
    })


def test_write_batches_require_post_write_tests():
    for batch in EXECUTION_BATCHES.values():
        if batch.write_capable:
            assert batch.post_write_tests_required is True


def test_execution_batches_remain_selector_closed():
    for batch in EXECUTION_BATCHES.values():
        assert batch.allows_runtime_graph_expansion is False
        assert batch.allows_plus_selector is False
        assert batch.allows_tag_selector is False
        assert batch.allows_arbitrary_dbt_args is False


def test_validate_execution_batch_accepts_exact_ready_batch():
    batch = EXECUTION_BATCHES["C4_RID_FOUNDATION_03"]

    assert validate_execution_batch(
        batch.batch_id,
        authority=batch.authority,
        requested_models=batch.model_allowlist,
        completed_batches=frozenset({"C4_RAW_02"}),
    ) is batch


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"runtime_graph_expansion": True}, "graph expansion"),
        ({"plus_selector": True}, "plus selectors"),
        ({"tag_selector": True}, "tag selectors"),
        ({"arbitrary_dbt_args": True}, "dbt arguments"),
    ],
)
def test_validate_execution_batch_rejects_expansion(
    overrides,
    message,
):
    batch = EXECUTION_BATCHES["C4_ML_01"]

    with pytest.raises(ExecutionPlanError, match=message):
        validate_execution_batch(
            batch.batch_id,
            authority=batch.authority,
            requested_models=batch.model_allowlist,
            completed_batches=frozenset(),
            **overrides,
        )


@pytest.mark.parametrize(
    ("batch_id", "authority", "models", "completed", "message"),
    [
        (
            "UNKNOWN",
            "ordinary_transform",
            frozenset(),
            frozenset(),
            "not allowlisted",
        ),
        (
            "C4_ML_01",
            "ordinary_transform",
            frozenset({
                "model.pdm_platform.br_pdm_ai_default_risk",
            }),
            frozenset(),
            "authority",
        ),
        (
            "C4_ML_01",
            "ml_prediction_transform",
            frozenset({"model.pdm_platform.unapproved"}),
            frozenset(),
            "bounded batch allowlist",
        ),
        (
            "C4_RID_FOUNDATION_03",
            "restricted_identity_transform",
            frozenset({
                "model.pdm_platform.vlt_pdm_beneficiary_identity",
            }),
            frozenset(),
            "prerequisites",
        ),
    ],
)
def test_validate_execution_batch_rejects_invalid_request(
    batch_id,
    authority,
    models,
    completed,
    message,
):
    with pytest.raises(ExecutionPlanError, match=message):
        validate_execution_batch(
            batch_id,
            authority=authority,
            requested_models=models,
            completed_batches=completed,
        )


def test_transform_execution_defaults_disabled(monkeypatch):
    monkeypatch.delenv(
        "LAKEHOUSE_TRANSFORM_EXECUTION_ENABLED",
        raising=False,
    )

    import orchestration.transform_runtime.api as api

    api = importlib.reload(api)

    assert api.EXECUTION_ENABLED is False


def test_execution_plan_introduces_no_executor():
    source = inspect.getsource(execution_plan)

    assert "subprocess" not in source
    assert not hasattr(execution_plan, "execute")
