"""Shared authority and governance fixtures for agent tests.

Every helper here builds TRUSTED, SERVER-SIDE inputs only:

* governance context of the kind the platform resolves from an identity;
* a platform-owned classification resolver, so synthetic test datasets can be
  governed without weakening the fail-closed behaviour;
* a server-held approval registry;
* server-issued publication authority.

Nothing in this module is constructed from request payload data.  Using these
fixtures therefore exercises the real authority boundary rather than bypassing
it.
"""

from __future__ import annotations

from approvals import ApprovalRecord, InMemoryApprovalRegistry
from authority import publication_authority as _server_publication_authority
from classification import (
    DATASET_POLICIES,
    DatasetPolicy,
    DataClassification,
    TrustedClassificationResolver,
)
from governance_models import (
    ApprovalContext,
    GovernanceRequest,
    IdentityContext,
    ResourceContext,
)


BENEFICIARY_DATASET = "iceberg.silver.slv_pdm_beneficiaries"
LOANS_DATASET = "iceberg.silver.slv_pdm_loans"


#
# Server-side subject -> authorised roles.
#
# Roles are a TRUSTED server input.  A request may claim roles, but only the
# intersection of the claim and this registry survives
# (`Orchestrator._authoritative_governance`).  With no registry entry a claim
# grants nothing.
#
ROLE_AUTHORITIES: dict[str, tuple[str, ...]] = {
    "analyst-1": ("programme_analyst",),
    "case-officer-1": ("investigator",),
    "publisher-1": ("bi_publisher",),
}


def authority_resolver(extra_roles=None):
    """Server authority resolver wired with the test role registry."""

    from authority import ServerAuthorityResolver

    role_authorities = dict(ROLE_AUTHORITIES)
    role_authorities.update(extra_roles or {})

    return ServerAuthorityResolver(
        role_authorities=role_authorities,
    )


def governance_context(
    *,
    subject_id="analyst-1",
    roles=("programme_analyst",),
    purpose="programme_monitoring",
    dataset="iceberg.consumption.cns_pdm_local_government_performance",
    action="read",
):
    """Governance context in the exact shape ``Orchestrator`` resolves."""

    return {
        "governance": {
            "identity": {
                "subject_id": subject_id,
                "roles": list(roles),
                "purpose": purpose,
            },
            "action": action,
            "resource": {
                "dataset": dataset,
                "classification": "PUBLIC",
            },
        },
    }


def governance_request(
    *,
    subject_id="analyst-1",
    roles=("programme_analyst",),
    purpose="programme_monitoring",
    dataset="iceberg.consumption.cns_pdm_local_government_performance",
    action="read",
    approval_id=None,
):
    """A directly constructed GovernanceRequest for ToolRegistry tests."""

    approval = (
        ApprovalContext(approval_id=approval_id, status="approved")
        if approval_id
        else None
    )

    return GovernanceRequest(
        identity=IdentityContext(
            subject_id=subject_id,
            roles=list(roles),
            purpose=purpose,
        ),
        action=action,
        approval=approval,
        resource=ResourceContext(dataset=dataset),
    )


def approval_registry(
    *,
    approval_id="trusted-approval-1",
    subject_id,
    action="read",
    dataset,
    status="approved",
):
    """A server-side approval record bound to one subject/action/resource."""

    return InMemoryApprovalRegistry(
        [
            ApprovalRecord(
                approval_id=approval_id,
                subject_id=subject_id,
                action=action,
                dataset=dataset,
                status=status,
                approver="governance-administrator",
            )
        ]
    )


def publication_authority(
    subject_id="publisher-1",
    *,
    roles=("bi_publisher",),
    authenticated=True,
):
    """Server-issued publication authority for adapter-level tests."""

    return _server_publication_authority(
        subject_id,
        roles,
        authenticated=authenticated,
    )


def platform_classifier(
    classifications=None,
    *,
    field_policies=None,
):
    """Platform-owned classification covering synthetic test datasets.

    ``classifications`` maps a dataset name to a `DataClassification`.  Any
    dataset not listed still fails closed through the normal trusted registry.
    """

    policies = dict(DATASET_POLICIES)

    for dataset, classification in (classifications or {}).items():
        policies[dataset] = DatasetPolicy(
            dataset=dataset,
            classification=classification,
        )

    return TrustedClassificationResolver(
        dataset_policies=policies,
        field_policies=field_policies,
    )
# ---------------------------------------------------------------------------
# Example server-owned role bindings (F1) - SYNTHETIC PLACEHOLDER ONLY.
#
# Shape of the file referenced by AGENT_AUTH_ROLE_BINDINGS_FILE.  The subjects
# below are placeholders, not production identities: replace them with the `sub`
# values your identity provider actually issues.
#
# An unknown subject is NOT an error.  It authenticates and receives no roles,
# which keeps authentication and authorisation as separate decisions.
#
# subjects:
#   <analyst-subject-id>:
#     roles:
#       - programme_analyst
#   <publisher-subject-id>:
#     roles:
#       - bi_publisher
