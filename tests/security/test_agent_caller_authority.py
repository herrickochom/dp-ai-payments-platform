"""JOB A2.2 - FAIL-FIRST PROOF: caller-supplied agent authority.

A1 finding under test
---------------------
``AgentRequest.permissions`` (and every other request model that embeds
``Permissions``) is caller-supplied request-body data.  The agent capability
checks in ``agents.py``, ``phase3_agents.py``, ``phase4_agents.py``,
``builders.py``, ``data_quality.py`` and ``bi_adapter.py`` read those flags
directly, and ``api.py`` forwards ``request.permissions`` into them unchanged.

Nothing on the request path derives authority from a server-verified identity:
``authentication_hook`` only copies an unauthenticated ``x-subject-id`` header
(or the literal default ``local-pilot-user``) onto ``request.state.subject_id``,
and that value is never used to build ``Permissions``.

Security property under test
----------------------------
A caller must not be able to grant itself authority.

These tests are intentionally FAIL-FIRST: they assert the security property
the platform must have, not the behaviour it currently has.  A failure here is
recorded as::

    A2_2_CALLER_AUTHORITY=VULNERABILITY_PROVEN

They are deterministic and offline.  No live Superset, Trino, container or
network access is required or performed.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
AGENT_API_DIR = ROOT / "services" / "agent-api"

for _path in (ROOT / "tests", AGENT_API_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))


import api
from agent_asgi_client import LocalClient
from bi_adapter import (
    AdapterPermissionDenied,
    PublicationRequest,
    SupersetAdapter,
)
from builder_models import (
    DashboardSpec,
    DataSourceSpec,
    Encoding,
    LayoutItem,
    ResolvedField,
    VisualizationSpec,
)
from config import Settings
from models import Permissions


#
# Exactly what a caller puts in the JSON request body.  No credential, no
# verified identity and no server-side grant accompany these values.
#
CALLER_AUTHORITY_PAYLOAD = {
    "can_discover_metadata": True,
    "can_execute_read_queries": True,
    "can_build_visualizations": True,
    "can_build_dashboards": True,
    "can_publish_bi_assets": True,
    "max_rows": 100000,
}


class NetworkTrapSession:
    """Any Superset network attempt is a proof the authority gate was passed."""

    def __getattr__(self, name):
        raise AssertionError(
            f"Superset network call attempted before authority rejection: {name}"
        )


def _public_dashboard():
    return DashboardSpec(
        id="dashboard_a2_2",
        title="A2.2 caller authority proof",
        visualizations=[
            VisualizationSpec(
                id="kpi_a2_2",
                type="kpi",
                title="Repayment performance",
                data_source=DataSourceSpec(
                    id="ds_a2_2",
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
                visualization_id="kpi_a2_2",
                x=0,
                y=0,
                width=6,
                height=4,
            )
        ],
    )


# ---------------------------------------------------------------------------
# A2.2 - component proof: the publication gate trusts the caller's own body
# ---------------------------------------------------------------------------


def test_a2_2_publish_gate_must_not_accept_caller_requested_authority():
    """``can_publish_bi_assets`` must be server-derived, not body-supplied.

    ``SupersetAdapter.publish`` is the only authority check on this path and it
    reads ``permissions.can_publish_bi_assets`` straight from the caller.

    ``superset_password`` is deliberately left unset so that, once the gate is
    passed, publication stops deterministically and offline.  That stop point
    is itself the evidence that the caller's self-granted flag was honoured.
    """

    assert Permissions().can_publish_bi_assets is False, (
        "platform default must remain deny"
    )

    #
    # Caller-requested capability, not server-authorised capability.
    #
    caller_permissions = Permissions.model_validate(CALLER_AUTHORITY_PAYLOAD)

    adapter = SupersetAdapter(
        settings=SimpleNamespace(superset_password=None),
        session=NetworkTrapSession(),
    )

    with pytest.raises(AdapterPermissionDenied):
        adapter.publish(
            _public_dashboard(),
            caller_permissions,
        )


# ---------------------------------------------------------------------------
# A2.2 - reachability proof: the same body reaches the public endpoint
# ---------------------------------------------------------------------------


def test_a2_2_publish_endpoint_must_not_honour_anonymous_body_authority(
    monkeypatch,
):
    """POST /publish/superset with no credential and no verified identity.

    The only authority presented is the request body.  A hardened service must
    reject this with 401/403 before any publication decision is made.
    """

    monkeypatch.setattr(
        api,
        "settings",
        Settings(superset_password=None),
    )

    publication = PublicationRequest(
        dashboard=_public_dashboard(),
        publish=True,
        permissions=Permissions.model_validate(CALLER_AUTHORITY_PAYLOAD),
    )

    client = LocalClient(api.app)

    #
    # No Authorization header, no x-subject-id header, no cookie.
    #
    response = client.post(
        "/publish/superset",
        json=publication.model_dump(mode="json"),
    )

    observed = {
        "status_code": response.status_code,
        "error_category": response.json().get("error", {}).get("category"),
    }

    assert response.status_code in {401, 403}, (
        "SECURITY: publication authority must be server-authorised; an "
        "anonymous caller must not be able to grant itself the capability "
        "to publish BI assets by putting it in the request body.\n"
        f"observed={observed}\n"
        "required={'status_code': '401 or 403'}"
    )
