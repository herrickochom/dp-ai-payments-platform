import pytest

from services.shared.cdc.activation import (
    CDCActivationDenied,
    current_activation_summary,
    validate_activation,
    validate_source_for_cdc,
)


@pytest.mark.parametrize(
    "source_system",
    [
        "agent_network",
        "cpo",
        "icmn",
        "mobile_networks",
        "pdmis",
        "wendi",
    ],
)
def test_generated_sources_cannot_activate_cdc(
    source_system,
):
    with pytest.raises(CDCActivationDenied):
        validate_source_for_cdc(
            source_system
        )


def test_pdmis_cannot_activate_as_cdc():
    with pytest.raises(CDCActivationDenied):
        validate_activation("pdmis")


def test_platform_postgres_not_source():
    with pytest.raises(CDCActivationDenied):
        validate_activation(
            "platform_postgres"
        )


def test_unknown_source_fails_closed():
    with pytest.raises(CDCActivationDenied):
        validate_activation(
            "not-a-source"
        )


def test_current_cdc_state_is_closed():
    summary = current_activation_summary()

    assert summary["cdc_capable"] == []
    assert summary["cdc_activated"] == []
    assert summary["allowlist_count"] == 0
    assert (
        summary["contract_status"]
        == "fail_closed_not_activated"
    )
