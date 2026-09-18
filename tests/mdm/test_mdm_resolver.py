from services.shared.mdm.resolver import (
    crosswalk_record,
    resolve_beneficiaries,
    resolve_simple_domain,
    stable_golden_id,
)


def beneficiary(
    source_id: str,
    nin: str,
    verified: bool = True,
) -> dict:
    return {
        "source_system": "pdmis",
        "source_entity": "beneficiaries",
        "source_record_id": source_id,
        "nin": nin,
        "nin_verified": verified,
    }


def test_golden_id_is_deterministic():
    first = stable_golden_id(
        "beneficiary",
        "pdmis",
        "BEN-0001",
    )

    second = stable_golden_id(
        "beneficiary",
        "pdmis",
        "BEN-0001",
    )

    assert first == second
    assert first.startswith("MDMBEN-")


def test_different_sources_do_not_share_source_identity():
    first = stable_golden_id(
        "beneficiary",
        "pdmis",
        "123",
    )

    second = stable_golden_id(
        "beneficiary",
        "another_system",
        "123",
    )

    assert first != second


def test_unique_verified_nin_is_accepted():
    resolutions, alerts = resolve_beneficiaries(
        [beneficiary("BEN-1", "NIN-A")]
    )

    assert len(resolutions) == 1
    assert resolutions[0].match_status == "MATCHED"
    assert resolutions[0].match_rule == "verified_nin_exact"
    assert alerts == []


def test_shared_verified_nin_is_not_merged():
    resolutions, alerts = resolve_beneficiaries(
        [
            beneficiary("BEN-1", "NIN-A"),
            beneficiary("BEN-2", "NIN-A"),
        ]
    )

    assert len(resolutions) == 2
    assert all(
        row.match_status == "QUARANTINED"
        for row in resolutions
    )

    assert len(
        {
            row.golden_record_id
            for row in resolutions
        }
    ) == 2

    assert len(alerts) == 2


def test_unverified_nin_does_not_drive_identity_match():
    resolutions, alerts = resolve_beneficiaries(
        [beneficiary("BEN-1", "NIN-A", False)]
    )

    assert resolutions[0].match_status == "MATCHED"
    assert (
        resolutions[0].match_rule
        == "source_record_provenance"
    )

    assert alerts == []


def test_crosswalk_contains_no_direct_identity():
    resolutions, _ = resolve_beneficiaries(
        [beneficiary("BEN-1", "NIN-A")]
    )

    row = crosswalk_record(
        resolutions[0],
        valid_from="2026-09-18T00:00:00Z",
    )

    forbidden = {
        "nin",
        "name",
        "phone",
        "alternative_phone",
        "email",
        "date_of_birth",
    }

    assert forbidden.isdisjoint(row)


def test_simple_domain_resolution():
    rows = [
        {
            "source_system": "pdmis",
            "source_entity": "saccos",
            "source_record_id": "SACCO-1",
        }
    ]

    result = resolve_simple_domain(
        "sacco",
        rows,
    )

    assert len(result) == 1
    assert result[0].match_status == "MATCHED"
    assert result[0].golden_record_id.startswith(
        "MDMSAC-"
    )


def test_missing_source_key_fails_closed():
    try:
        resolve_simple_domain(
            "agent",
            [
                {
                    "source_system": "agent_network",
                    "source_entity": "agent_profiles",
                }
            ],
        )
    except ValueError as exc:
        assert "source_record_id" in str(exc)
    else:
        raise AssertionError(
            "Missing source key did not fail closed"
        )
