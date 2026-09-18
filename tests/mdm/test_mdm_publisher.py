import importlib.util
from pathlib import Path

import pytest


PATH = Path(
    "services/mdm-publisher/mdm_publisher.py"
)

SPEC = importlib.util.spec_from_file_location(
    "mdm_publisher",
    PATH,
)

publisher = importlib.util.module_from_spec(
    SPEC
)

assert SPEC.loader is not None
SPEC.loader.exec_module(publisher)


def test_registry_validates():
    counts = publisher.validate_registry()

    assert len(counts) == 6
    assert sum(counts.values()) == 728


def test_ordinary_plan_has_only_ordinary_topics():
    plan = publisher.build_publish_plan(
        "ordinary"
    )

    topics = {
        row["topic"]
        for row in plan
    }

    assert topics == {
        "mdm.sacco.golden",
        "mdm.agent.golden",
        "mdm.geography.golden",
    }

    assert len(plan) == 114


def test_restricted_plan_is_separate():
    plan = publisher.build_publish_plan(
        "restricted_identity"
    )

    topics = {
        row["topic"]
        for row in plan
    }

    assert topics == {
        "mdm.beneficiary.golden.restricted",
        "mdm.beneficiary.identity-alert.restricted",
        "mdm.crosswalk.restricted",
    }

    assert len(plan) == 614


def test_cross_authority_publish_fails():
    registry = publisher.load_registry()
    policies = publisher.topic_policy()

    spec = next(
        row
        for row in registry["records"]
        if row["topic"]
        == "mdm.beneficiary.golden.restricted"
    )

    with pytest.raises(PermissionError):
        publisher.validate_authority(
            spec,
            policies[spec["topic"]],
            "ordinary",
        )


def test_execute_fails_closed_by_default(
    monkeypatch,
):
    monkeypatch.delenv(
        "MDM_KAFKA_PUBLISH_ENABLED",
        raising=False,
    )

    with pytest.raises(
        RuntimeError,
        match="publishing is disabled",
    ):
        publisher.execute_publish(
            "ordinary"
        )
