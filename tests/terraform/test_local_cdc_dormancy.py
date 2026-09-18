import json
from pathlib import Path

from services.shared.cdc.activation import (
    current_activation_summary,
)


ROOT = Path(__file__).resolve().parents[2]

CONTRACT = (
    ROOT
    / "infra"
    / "terraform"
    / "targets"
    / "local"
    / "contracts"
    / "cdc_dormancy.json"
)


def load_contract():
    return json.loads(CONTRACT.read_text())


def test_cdc_dormancy_contract_exists():
    assert CONTRACT.is_file()


def test_cdc_dormancy_contract_uses_existing_api():
    c = load_contract()

    assert (
        c["source_api"]
        == "services.shared.cdc.activation."
        "current_activation_summary"
    )


def test_current_cdc_summary_shape():
    summary = current_activation_summary()

    assert set(summary) == {
        "cdc_capable",
        "cdc_activated",
        "allowlist_count",
        "contract_status",
    }

    assert isinstance(summary["cdc_capable"], list)
    assert isinstance(summary["cdc_activated"], list)
    assert isinstance(summary["allowlist_count"], int)
    assert isinstance(summary["contract_status"], str)


def test_cdc_is_dormant_fail_closed():
    summary = current_activation_summary()

    assert summary["cdc_capable"] == []
    assert summary["cdc_activated"] == []
    assert summary["allowlist_count"] == 0
    assert (
        summary["contract_status"]
        == "fail_closed_not_activated"
    )


def test_local_deployment_cannot_activate_cdc():
    c = load_contract()
    policy = c["policy"]

    assert (
        policy[
            "cdc_activation_allowed_by_local_deployment"
        ]
        is False
    )

    assert (
        policy[
            "cdc_activation_requires_separate_governed_process"
        ]
        is True
    )

    assert (
        policy[
            "terraform_local_preflight_must_fail_if_cdc_is_activated"
        ]
        is True
    )
