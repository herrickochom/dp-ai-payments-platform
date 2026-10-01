"""JOB F2/F3 - FAIL-FIRST PROOFS: BI publication least privilege.

F1 secured WHO may request a privileged operation. F2 secures the authority of
the BI service identity that performs the publication downstream.

Proven invariants:

* automated publication authenticates as a DEDICATED publisher identity and
  never as the Superset administrator, with no silent fallback;
* publication fails closed when the publisher credential is absent, even when
  administrator credentials are present;
* a caller cannot supply, replace or select the Superset credential;
* publication is bounded by the SERVER-OWNED approved BI allowlist, so the
  bi_publisher role plus an INTERNAL classification is not sufficient;
* the RESTRICTED classification gate still applies unchanged.

Deterministic and offline. No live Superset, Metabase or Trino.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
AGENT_API_DIR = ROOT / "services" / "agent-api"

for _path in (ROOT / "tests", ROOT, AGENT_API_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))


from agent_authority_fixtures import publication_authority
from authority import Authority
from bi_adapter import (
    AdapterError,
    AdapterPermissionDenied,
    PublicationRequest,
    SupersetAdapter,
)
from bi_policy import (
    APPROVED_BI_TABLES,
    BI_CATALOG,
    BI_SCHEMA,
    IDENTITY_BEARING_DATASETS,
    check_bi_publication,
    is_bi_approved,
)

APPROVED_DATASET = (
    f"{BI_CATALOG}.{BI_SCHEMA}.cns_pdm_local_government_performance"
)
INTERNAL_SILVER = "iceberg.silver.slv_pdm_loans"
RESTRICTED_DATASET = "iceberg.silver.slv_pdm_beneficiaries"
IDENTITY_BEARING = (
    f"{BI_CATALOG}.{BI_SCHEMA}.cns_pdm_beneficiary_insights"
)

PUBLISHER_SETTINGS = {
    "superset_publisher_username": "agent_publisher",
    "superset_publisher_password": "publisher-secret-not-real",
}


class NetworkTrapSession:
    """Any Superset call fails the test: publication must stop beforehand."""

    def __getattr__(self, name):
        raise AssertionError(f"Superset network call attempted: {name}")


def _dashboard_for(dataset: str):
    return SimpleNamespace(
        visualizations=[
            SimpleNamespace(
                data_source=SimpleNamespace(id="ds_f2", dataset=dataset),
                encoding=[SimpleNamespace(field="district")],
            )
        ],
        filters=[],
    )


def _settings(**overrides):
    """Settings with NO publisher credential by default (fail-closed)."""

    base = {
        "superset_url": "https://superset.invalid",
        "superset_username": "admin",
        "superset_password": "administrator-secret-not-real",
        "superset_publisher_username": "",
        "superset_publisher_password": None,
        "superset_database_name": "PDM Trino",
    }
    base.update(overrides)

    return SimpleNamespace(**base)


# ---------------------------------------------------------------------------
# F2-1 - dedicated publisher identity, no admin fallback
# ---------------------------------------------------------------------------


def test_publication_fails_closed_without_publisher_credentials():
    adapter = SupersetAdapter(_settings(), session=NetworkTrapSession())

    with pytest.raises(AdapterError) as excinfo:
        adapter.publish(
            _dashboard_for(APPROVED_DATASET),
            publication_authority(),
        )

    assert "SUPERSET_PUBLISHER" in str(excinfo.value)


def test_admin_credential_present_does_not_authorise_publication():
    """Administrator credentials must NOT silently back publication."""

    adapter = SupersetAdapter(
        _settings(
            superset_username="admin",
            superset_password="administrator-secret",
        ),
        session=NetworkTrapSession(),
    )

    assert adapter.settings.superset_password

    with pytest.raises(AdapterError) as excinfo:
        adapter.publish(
            _dashboard_for(APPROVED_DATASET),
            publication_authority(),
        )

    assert "SUPERSET_PUBLISHER" in str(excinfo.value), (
        "SECURITY: publication must fail closed rather than fall back to the "
        "Superset administrator credential"
    )


def test_publisher_credentials_are_read_from_the_publisher_settings():
    adapter = SupersetAdapter(
        _settings(**PUBLISHER_SETTINGS),
        session=NetworkTrapSession(),
    )

    adapter._require_publisher_identity()

    assert adapter.settings.superset_publisher_username == (
        "agent_publisher"
    )
    assert adapter.settings.superset_username == "admin"


def test_caller_cannot_supply_a_superset_credential():
    """A request body has no channel to reach the Superset credential."""

    #
    # The caller-facing publication model has NO field that could carry a
    # Superset username or password into the adapter.
    #
    fields = set(PublicationRequest.model_fields)

    assert not fields & {"superset_username", "superset_password"}
    assert fields == {"dashboard", "publish", "permissions"}


# ---------------------------------------------------------------------------
# F2-2 - server-owned BI publication allowlist
# ---------------------------------------------------------------------------


def test_approved_bi_dataset_is_publication_eligible():
    assert is_bi_approved(APPROVED_DATASET)
    assert check_bi_publication(APPROVED_DATASET) is None


@pytest.mark.parametrize(
    "dataset",
    [
        INTERNAL_SILVER,
        RESTRICTED_DATASET,
        IDENTITY_BEARING,
        "iceberg.consumption.cns_pdm_not_on_the_allowlist",
        "iceberg.gold.gld_fct_pdm_loans",
        "not-a-dataset",
    ],
)
def test_non_approved_dataset_is_not_publication_eligible(dataset):
    assert not is_bi_approved(dataset)
    assert check_bi_publication(dataset) is not None


def test_authorised_publisher_cannot_publish_internal_silver_dataset():
    """F2-2 regression: role + INTERNAL classification is not enough."""

    adapter = SupersetAdapter(
        _settings(**PUBLISHER_SETTINGS),
        session=NetworkTrapSession(),
    )

    with pytest.raises(AdapterPermissionDenied) as excinfo:
        adapter.publish(
            _dashboard_for(INTERNAL_SILVER),
            publication_authority(),
        )

    assert "approved BI allowlist" in str(excinfo.value)


def test_authorised_publisher_cannot_publish_identity_bearing_dataset():
    """An identity-bearing consumption dataset is refused before any write.

    Two independent server-owned controls refuse it: the trusted
    classification gate (these datasets are not classified for BI publication)
    and the approved BI allowlist. Whichever fires first, publication is
    denied and the Superset adapter performs no write. The allowlist refusal
    is asserted independently in
    `test_non_approved_dataset_is_not_publication_eligible`.
    """

    adapter = SupersetAdapter(
        _settings(**PUBLISHER_SETTINGS),
        session=NetworkTrapSession(),
    )

    with pytest.raises(AdapterPermissionDenied) as excinfo:
        adapter.publish(
            _dashboard_for(IDENTITY_BEARING),
            publication_authority(),
        )

    message = str(excinfo.value)
    assert "cns_pdm_beneficiary_insights" in message

    #
    # The allowlist independently refuses the same dataset.
    #
    refusal = check_bi_publication(IDENTITY_BEARING)
    assert refusal is not None
    assert "beneficiary identity" in refusal.reason


def test_restricted_dataset_is_still_refused():
    """The pre-existing RESTRICTED gate must still fire.

    The BI allowlist is an ADDITIONAL deny layer that now runs first, so the
    refusal reason differs; the guarantee - a RESTRICTED dataset is never
    published - is unchanged. The classification gate itself is covered by the
    Phase 5/Phase 9 publication governance tests.
    """

    adapter = SupersetAdapter(
        _settings(**PUBLISHER_SETTINGS),
        session=NetworkTrapSession(),
    )

    with pytest.raises(AdapterPermissionDenied) as excinfo:
        adapter.publish(
            _dashboard_for(RESTRICTED_DATASET),
            publication_authority(),
        )

    assert RESTRICTED_DATASET in str(excinfo.value)


def test_publication_requires_server_authority_before_dataset_policy():
    unauthorised = Authority(
        subject_id="someone",
        capabilities=frozenset(),
        roles=(),
        authenticated=True,
    )

    adapter = SupersetAdapter(
        _settings(**PUBLISHER_SETTINGS),
        session=NetworkTrapSession(),
    )

    with pytest.raises(AdapterPermissionDenied):
        adapter.publish(
            _dashboard_for(APPROVED_DATASET),
            unauthorised,
        )


def test_allowlist_is_server_owned_and_not_influenced_by_caller():
    before = set(APPROVED_BI_TABLES)

    check_bi_publication("iceberg.silver.slv_pdm_loans")

    assert set(APPROVED_BI_TABLES) == before
    assert not is_bi_approved(INTERNAL_SILVER)


def test_identity_bearing_and_approved_sets_are_disjoint():
    assert not (APPROVED_BI_TABLES & IDENTITY_BEARING_DATASETS)
