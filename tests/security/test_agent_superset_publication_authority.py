"""JOB A2.7 - FAIL-FIRST PROOF: Superset publication authority.

A2.2 proved that caller-controlled capability reaches ``SupersetAdapter``.  This
proof closes the consequential half of that path: whether an anonymous,
untrusted caller can reach the adapter's AUTHENTICATED Superset operation,
performed with server-configured service authority, and cross the Superset
network boundary.

Path under test::

    anonymous HTTP request
        -> /publish/superset
        -> request Permissions
        -> publication permission check
        -> classification / publication gate
        -> SupersetAdapter.publish
        -> Superset network boundary

Only the HTTP transport is replaced.  The permission gate, the classification
gate and ``SupersetAdapter.publish`` itself are the real production code.

Security property under test
----------------------------
Anonymous / caller-controlled publication must terminate with 401 or 403
*before* ``SupersetAdapter.publish``.

This test is intentionally FAIL-FIRST.  A failure is recorded as::

    A2_7_SUPERSET_PUBLICATION=VULNERABILITY_PROVEN

Deterministic and offline.  No live Superset request is ever issued, and no
configured credential is read, recorded or printed: the service credential is
represented by a synthetic non-secret marker, and the network trap records
booleans only.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
AGENT_API_DIR = ROOT / "services" / "agent-api"

for _path in (ROOT / "tests", AGENT_API_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))


import api
from agent_asgi_client import LocalClient
from bi_adapter import PublicationRequest, SupersetAdapter
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
# Synthetic, non-secret stand-in for "a service credential is configured on
# the server".  It is never a real credential and is only ever compared, never
# printed.
#
SERVER_CREDENTIAL_MARKER = "a2-synthetic-service-credential-marker"

LOGIN_PATH = "/api/v1/security/login"


class SupersetNetworkTrap:
    """Terminal HTTP boundary. Records booleans, never payloads."""

    def __init__(self):
        self.login_reached = False
        self.used_server_configured_credential = False

    def _record_login(self, kwargs):
        body = kwargs.get("json") or {}

        self.used_server_configured_credential = (
            body.get("password") == SERVER_CREDENTIAL_MARKER
        )

        raise AssertionError(
            "Superset network boundary reached by anonymous caller"
        )

    def post(self, url, **kwargs):
        if LOGIN_PATH in url:
            self.login_reached = True
            return self._record_login(kwargs)
        raise AssertionError("unexpected Superset network call")

    def get(self, url, **kwargs):
        raise AssertionError("unexpected Superset network call")

    def request(self, method, url, **kwargs):
        raise AssertionError("unexpected Superset network call")


NETWORK_TRAP = SupersetNetworkTrap()
def _public_dashboard():
    """A PUBLIC dataset, so the classification gate passes and publication is
    attempted.  The defect under test is publication AUTHORITY, not data
    classification."""

    return DashboardSpec(
        id="dashboard_a2_7",
        title="A2.7 publication authority proof",
        visualizations=[
            VisualizationSpec(
                id="kpi_a2_7",
                type="kpi",
                title="Repayment performance",
                data_source=DataSourceSpec(
                    id="ds_a2_7",
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
                visualization_id="kpi_a2_7",
                x=0,
                y=0,
                width=6,
                height=4,
            )
        ],
    )
def test_anonymous_caller_cannot_reach_superset_publication(monkeypatch):
    """An anonymous body-supplied capability must not reach the Superset login."""

    publish_calls = []
    original_publish = SupersetAdapter.publish

    class TrappedSupersetAdapter(SupersetAdapter):
        """Identical to production except the HTTP transport is trapped."""

        def __init__(self, settings):
            super().__init__(settings, session=NETWORK_TRAP)

    def spy_publish(self, dashboard, permissions):
        publish_calls.append(
            {"can_publish_bi_assets": permissions.can_publish_bi_assets}
        )
        return original_publish(self, dashboard, permissions)

    monkeypatch.setattr(api, "SupersetAdapter", TrappedSupersetAdapter)
    monkeypatch.setattr(SupersetAdapter, "publish", spy_publish)
    monkeypatch.setattr(
        api,
        "settings",
        Settings(superset_password=SERVER_CREDENTIAL_MARKER),
    )

    #
    # The ONLY authority this caller presents is its own request body.
    #
    caller_payload = PublicationRequest(
        dashboard=_public_dashboard(),
        publish=True,
        permissions=Permissions.model_validate(
            {"can_publish_bi_assets": True}
        ),
    ).model_dump(mode="json")

    client = LocalClient(api.app)

    #
    # No Authorization header, no x-subject-id header, no cookie, and no
    # approval object anywhere on this request model.
    #
    response = client.post("/publish/superset", json=caller_payload)

    observed = {
        "anonymous_request": True,
        "caller_can_publish": bool(
            caller_payload["permissions"]["can_publish_bi_assets"]
        ),
        "verified_server_role": False,
        "independent_approval": "approval" in PublicationRequest.model_fields,
        "caller_payload_contained_credential": (
            SERVER_CREDENTIAL_MARKER in json.dumps(caller_payload)
        ),
        "adapter_publish_reached": len(publish_calls) > 0,
        "superset_network_boundary_reached": NETWORK_TRAP.login_reached,
        "login_used_server_configured_credential": (
            NETWORK_TRAP.used_server_configured_credential
        ),
        "status_code": response.status_code,
    }

    assert response.status_code in {401, 403}, (
        "SECURITY: anonymous / caller-controlled publication must terminate "
        "with 401 or 403 before SupersetAdapter.publish. A caller-supplied "
        "can_publish_bi_assets flag, with no verified identity, no "
        "server-derived publication role and no independent approval, "
        "reached the adapter and crossed the Superset network boundary "
        "using server-configured service authority.\n"
        f"observed={observed}\n"
        "required={'status_code': '401 or 403'}"
    )