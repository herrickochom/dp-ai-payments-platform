"""JOB F1 - FAIL-FIRST PROOFS: end-to-end trusted identity over HTTP.

Exercises the real FastAPI application through an in-process ASGI client with
a deterministic TEST-ONLY signing key.  No live IdP, JWKS endpoint, Superset or
Trino is contacted.

Mandatory closure proofs:

* a forged ``x-subject-id`` header authenticates nothing;
* a missing or invalid credential yields 401;
* a valid token authenticates but does NOT by itself authorise;
* token role/permission claims and caller-body role/permission claims are
  inert;
* a verified, server-bound publisher reaches the publication path, while a
  verified low-privilege subject does not;
* an anonymous caller stays fail-closed.
"""

from __future__ import annotations

import pytest

from f1_identity_fixtures import (  # noqa: F401  (path bootstrap)
    OTHER_PRIVATE_KEY,
    TEST_AUDIENCE,
    TEST_ISSUER,
    StaticSigningKeyResolver,
    bearer,
    make_token,
)

import api
from agent_asgi_client import LocalClient
from orchestrator import Orchestrator
from authentication import TokenVerifier
from config import Settings


BINDINGS = {
    "f1-analyst": frozenset({"programme_analyst"}),
    "f1-publisher": frozenset({"bi_publisher"}),
}

class OfflineGateway:
    """Fake Trino gateway so no live platform is contacted."""

    def execute(self, sql):
        lowered = sql.lower()

        if "information_schema.columns" in lowered and "where table_schema" in lowered:
            return (
                ["column_name", "data_type", "ordinal_position"],
                [["district", "varchar", 1],
                 ["principal_repayment_rate", "double", 2]],
                "fake-describe",
            )

        if "information_schema.columns" in lowered:
            return (
                ["table_schema", "table_name", "column_name", "data_type"],
                [["consumption", "cns_pdm_local_government_performance",
                  "district", "varchar"]],
                "fake-search",
            )

        return ["district"], [["Kabale"]], "fake-data"

    def health(self):
        return {"status": "healthy", "query_id": "fake-health"}


MALICIOUS_CLAIMS = {
    "roles": ["governance_administrator"],
    "permissions": ["can_view_restricted_data"],
    "can_publish_bi_assets": True,
}


@pytest.fixture
def authenticated_client(monkeypatch):
    """A client with trusted authentication enabled and TEST-ONLY keys."""

    monkeypatch.setattr(
        api,
        "settings",
        Settings(
            auth_enabled=True,
            auth_issuer=TEST_ISSUER,
            auth_audience=TEST_AUDIENCE,
            auth_jwks_url="https://idp.test.invalid/jwks.json",
        ),
    )
    monkeypatch.setattr(
        api,
        "token_verifier",
        TokenVerifier(
            issuer=TEST_ISSUER,
            audience=TEST_AUDIENCE,
            signing_keys=StaticSigningKeyResolver(),
        ),
    )
    monkeypatch.setattr(api, "role_authorities", BINDINGS)
    monkeypatch.setattr(api.orchestrator, "gateway", OfflineGateway())

    return LocalClient(api.app)


# ---------------------------------------------------------------------------
# Credential requirements
# ---------------------------------------------------------------------------


def test_missing_credential_is_rejected(authenticated_client):
    response = authenticated_client.post(
        "/agents/query",
        json={"objective": "What tables contain repayment information?"},
    )

    assert response.status_code == 401
    assert response.json()["error"]["category"] == "AuthenticationRequired"


def test_forged_x_subject_id_header_authenticates_nothing(
    authenticated_client,
):
    """The retired header must not stand in for a verified identity."""

    response = authenticated_client.post(
        "/agents/query",
        headers={"x-subject-id": "governance-admin"},
        json={"objective": "What tables contain repayment information?"},
    )

    assert response.status_code == 401, (
        "SECURITY: a bare x-subject-id header must never authenticate."
    )


def test_invalid_signature_is_rejected(authenticated_client):
    response = authenticated_client.post(
        "/agents/query",
        headers={
            "Authorization": bearer(make_token(key=OTHER_PRIVATE_KEY))
        },
        json={"objective": "What tables contain repayment information?"},
    )

    assert response.status_code == 401


def test_valid_token_authenticates_the_request(authenticated_client):
    response = authenticated_client.post(
        "/agents/query",
        headers={
            "Authorization": bearer(make_token(subject="f1-analyst"))
        },
        json={"objective": "What tables contain repayment information?"},
    )

    #
    # Discovery is the anonymous-safe surface, so a verified caller reaches it.
    # Reaching it proves the token authenticated; it does NOT prove authority.
    #
    assert response.status_code == 200, response.text
# ---------------------------------------------------------------------------
# Authenticated is NOT authorised
# ---------------------------------------------------------------------------


def test_valid_token_with_admin_claims_cannot_publish(authenticated_client):
    """A signed token claiming publication authority must still be refused."""

    response = authenticated_client.post(
        "/publish/superset",
        headers={
            "Authorization": bearer(
                make_token(
                    subject="f1-analyst",
                    extra_claims=MALICIOUS_CLAIMS,
                )
            )
        },
        json=_public_publication_request(),
    )

    assert response.status_code == 403, (
        "SECURITY: token role/permission claims must never grant publication "
        "authority; only the server-owned binding may."
    )
    assert response.json()["error"]["category"] == (
        "PublicationAuthorityRequired"
    )


def test_caller_body_publish_flag_alone_is_refused(authenticated_client):
    """``can_publish_bi_assets=true`` in the body grants nothing."""

    publication = _public_publication_request()
    publication["permissions"]["can_publish_bi_assets"] = True

    response = authenticated_client.post(
        "/publish/superset",
        headers={
            "Authorization": bearer(make_token(subject="f1-analyst"))
        },
        json=publication,
    )

    assert response.status_code == 403


def test_anonymous_publication_remains_fail_closed(authenticated_client):
    response = authenticated_client.post(
        "/publish/superset",
        json=_public_publication_request(),
    )

    assert response.status_code == 401


def test_unbound_verified_subject_cannot_publish(authenticated_client):
    """A valid token for an unbound subject is authenticated but unprivileged."""

    publication = _public_publication_request()
    publication["permissions"]["can_publish_bi_assets"] = True

    response = authenticated_client.post(
        "/publish/superset",
        headers={
            "Authorization": bearer(make_token(subject="f1-unknown-user"))
        },
        json=publication,
    )

    assert response.status_code == 403


def test_bound_publisher_passes_the_authority_gate(authenticated_client):
    """A verified, server-bound publisher gets past the authority gate.

    The request then fails on Superset configuration, not on authority, which
    proves the publication AUTHORITY decision succeeded.
    """

    response = authenticated_client.post(
        "/publish/superset",
        headers={
            "Authorization": bearer(make_token(subject="f1-publisher"))
        },
        json=_public_publication_request(),
    )

    assert response.status_code == 502, (
        "SECURITY: a server-bound publisher must clear the authority gate; "
        f"got {response.status_code} {response.text[:200]}"
    )
    assert response.json()["error"]["category"] == "AdapterError", (
        "the request must fail on Superset configuration, not on authority"
    )


# ---------------------------------------------------------------------------
# Anonymous posture when authentication is disabled
# ---------------------------------------------------------------------------


def test_anonymous_stays_fail_closed_when_auth_disabled(monkeypatch):
    """The A3/A4 posture is preserved when authentication is off."""

    monkeypatch.setattr(api, "settings", Settings(auth_enabled=False))
    monkeypatch.setattr(api.orchestrator, "gateway", OfflineGateway())

    client = LocalClient(api.app)

    publication = _public_publication_request()
    publication["permissions"]["can_publish_bi_assets"] = True

    response = client.post("/publish/superset", json=publication)
# ---------------------------------------------------------------------------
# Payload helper
# ---------------------------------------------------------------------------


def _public_publication_request():
    from bi_adapter import PublicationRequest
    from builder_models import (
        DashboardSpec,
        DataSourceSpec,
        Encoding,
        LayoutItem,
        ResolvedField,
        VisualizationSpec,
    )

    return PublicationRequest(
        dashboard=DashboardSpec(
            id="dashboard_f1",
            title="F1 publication authority proof",
            visualizations=[
                VisualizationSpec(
                    id="kpi_f1",
                    type="kpi",
                    title="Repayment performance",
                    data_source=DataSourceSpec(
                        id="ds_f1",
                        dataset=(
                            "iceberg.consumption."
                            "cns_pdm_local_government_performance"
                        ),
                        fields=[
                            ResolvedField(
                                name="district",
                                data_type="varchar",
                                role="dimension",
                            )
                        ],
                    ),
                    encoding=[
                        Encoding(
                            channel="x",
                            field="district",
                            role="dimension",
                        )
                    ],
                )
            ],
            layout=[
                LayoutItem(
                    visualization_id="kpi_f1",
                    x=0,
                    y=0,
                    width=6,
                    height=4,
                )
            ],
        ),
        publish=True,
    ).model_dump(mode="json")

    assert response.status_code == 403, (
        "SECURITY: disabling authentication must not restore privileged "
        "publication authority."
    )


def test_disabled_auth_allows_discovery_but_denies_governed_agents(
    monkeypatch,
):
    monkeypatch.setattr(api, "settings", Settings(auth_enabled=False))
    monkeypatch.setattr(api.orchestrator, "gateway", OfflineGateway())

    client = LocalClient(api.app)

    discovery = client.post(
        "/agents/query",
        json={
            "objective": "What tables contain repayment information?",
        },
    )
    assert discovery.status_code == 200

    governed = client.post(
        "/agents/query",
        json={"objective": "What is repayment performance by district?"},
    )
    # /agents/query maps a failed request to 503.
    assert governed.status_code == 503
    assert governed.json()["error"]["category"] == (
        "GovernanceContextRequired"
    )


def test_health_probe_is_reachable_without_credentials(authenticated_client):
    """The container healthcheck must keep working when auth is enabled."""

    assert authenticated_client.get("/agents/health").status_code in (
        200,
        503,
    )
    assert authenticated_client.get("/health/live").status_code == 200