"""Server-derived authority boundary for the agent platform.

Security invariant enforced here::

    effective_capability(s) is a SUBSET OF server_authorised_capability(s)

Caller-controlled request data is a REQUEST, never a GRANT.

``AgentRequest.permissions`` (and every other caller-supplied capability model)
is therefore only ever able to NARROW what the server has already authorised.
It can never widen authority beyond what the server derived for the subject.

Authority sources, in decreasing trust:

``server_configuration``
    Capabilities the deployment explicitly grants to the agent request path.
    This is the ceiling for every request that is not governed by an identity.

``identity_roles``
    Capabilities derived from the governance roles of a resolved identity.

``unauthenticated``
    No capabilities at all.  Used when no trusted identity is available, so
    that privileged operations fail closed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from governance import ROLE_PERMISSIONS
from models import Permissions


#
# The capability ceiling for the non-governed agent request path.
#
# `can_publish_bi_assets` is deliberately ABSENT: publication is a privileged
# operation and is never granted by default to a request that carries no
# verified identity.  See `publication_authority`.
#
DEFAULT_AGENT_CAPABILITIES: frozenset[str] = frozenset(
    {
        "can_discover_metadata",
        "can_execute_read_queries",
        "can_build_visualizations",
        "can_build_dashboards",
        "can_run_data_quality_checks",
        "can_generate_insights",
    }
)

#
# Capabilities that may never be satisfied by an unverified caller.
#
PRIVILEGED_CAPABILITIES: frozenset[str] = frozenset(
    {
        "can_publish_bi_assets",
        "can_view_data_quality_samples",
    }
)


@dataclass(frozen=True)
class Authority:
    """A server-issued statement of what a subject may do.

    ``Authority`` is deliberately NOT a Pydantic model and is never
    constructed from request payload data.  It is only ever produced by
    `ServerAuthorityResolver` or `publication_authority`, both of which run on
    the server side of the boundary.
    """

    subject_id: str
    capabilities: frozenset[str]
    roles: tuple[str, ...] = ()
    source: str = "unauthenticated"
    authenticated: bool = False

    def permits(
        self,
        capability: str,
        *,
        require_authenticated: bool = False,
    ) -> bool:
        if capability not in self.capabilities:
            return False

        if require_authenticated and not self.authenticated:
            return False

        return True

    def permits_all(self, capabilities, *, require_authenticated: bool = False) -> bool:
        return all(
            self.permits(
                capability,
                require_authenticated=require_authenticated,
            )
            for capability in capabilities
        )


def unauthenticated() -> Authority:
    """No trusted identity is available.  Grants nothing."""

    return Authority(
        subject_id="",
        capabilities=frozenset(),
        roles=(),
        source="unauthenticated",
        authenticated=False,
    )


class ServerAuthorityResolver:
    """Single place where request authority becomes server authority.

    Authority is derived from the INTERSECTION of what the request claims and
    what the server authorises for that subject.  A request may narrow
    authority; it may never widen it.
    """

    def __init__(
        self,
        server_capabilities=DEFAULT_AGENT_CAPABILITIES,
        max_rows: int = 1000,
        role_authorities=None,
    ):
        self.server_capabilities = frozenset(server_capabilities)
        self.max_rows = max_rows
        #
        # Server-side subject -> authorised roles.  Populated by a trusted
        # identity source only.  It is empty by default, so a request that
        # merely CLAIMS a role gains nothing (A2.3 family).
        #
        self.role_authorities = {
            str(subject): frozenset(roles)
            for subject, roles in (role_authorities or {}).items()
        }

    def authorised_roles(self, identity: Any | None) -> tuple[str, ...]:
        """Roles the server authorises for this subject, intersected with the claim."""

        if identity is None:
            return ()

        subject = getattr(identity, "subject_id", "") or ""
        claimed = frozenset(
            getattr(identity, "roles", ()) or ()
        )

        return tuple(
            sorted(self.role_authorities.get(subject, frozenset()) & claimed)
        )

    def roles_for_subject(self, subject_id: str) -> tuple[str, ...]:
        """Server-authorised roles for an already-verified subject.

        A verified subject IS the claim, so there is nothing to intersect with:
        the binding alone decides.  An unbound subject yields no roles, which
        keeps authentication and authorisation as separate decisions.
        """

        return tuple(sorted(self.role_authorities.get(subject_id, frozenset())))

    def resolve_verified(self, subject_id: str) -> Authority:
        """Authority for a cryptographically verified subject.

        Roles come exclusively from the server-owned binding.  No token claim
        and no request-body value can influence the result.
        """

        roles = self.roles_for_subject(subject_id)

        #
        # Capabilities are DERIVED from the server-bound roles, never from a
        # token claim or a request-body field.
        #
        return Authority(
            subject_id=subject_id,
            capabilities=capabilities_for_roles(roles),
            roles=roles,
            source="trusted_identity",
            authenticated=True,
        )

    def resolve(
        self,
        identity: Any | None = None,
        *,
        authenticated: bool = False,
    ) -> Authority:
        """Resolve server authority for a request.

        The result is always a SUBSET of the server capability ceiling.  A
        resolved identity can only narrow it further.
        """

        subject_id = getattr(identity, "subject_id", "") or ""
        roles = self.authorised_roles(identity)

        granted = set(self.server_capabilities)

        if identity is not None:
            granted &= agent_capabilities_for_roles(roles)

            #
            # Privileged capabilities are never granted through the
            # non-authenticated agent request path, even to a role that would
            # otherwise hold them.
            #
            granted -= PRIVILEGED_CAPABILITIES

        return Authority(
            subject_id=subject_id,
            capabilities=frozenset(granted),
            roles=roles,
            source=(
                "identity_roles"
                if identity is not None
                else "server_configuration"
            ),
            authenticated=bool(authenticated),
        )


def publication_authority(
    subject_id: str,
    roles=(),
    *,
    authenticated: bool,
) -> Authority:
    """Server-derived publication authority for a resolved subject.

    ``authenticated`` is supplied by a TRUSTED identity source only.  The
    unverified ``x-subject-id`` header is not such a source, so privileged
    publication fails closed while no trusted identity provider is wired in.
    """

    granted = capabilities_for_roles(roles)

    if not authenticated:
        granted = frozenset()

    return Authority(
        subject_id=subject_id,
        capabilities=granted,
        roles=tuple(roles or ()),
        source=(
            "trusted_identity" if authenticated else "unauthenticated"
        ),
        authenticated=bool(authenticated),
    )


def narrow_permissions(
    requested: Permissions,
    authority: Authority,
    *,
    server_max_rows: int = 1000,
) -> Permissions:
    """Intersect caller-requested capabilities with server authority.

    The caller may narrow authority; it may never widen it.  Every boolean
    capability is the AND of the request and the server grant, and the row
    ceiling is the minimum of both.
    """

    granted = authority.capabilities

    fields: dict[str, Any] = {}

    for name in Permissions.model_fields:
        value = getattr(requested, name)

        if isinstance(value, bool):
            fields[name] = bool(value) and (name in granted)
        elif isinstance(value, int):
            fields[name] = max(1, min(value, server_max_rows))
        else:
            fields[name] = value

    return Permissions(**fields)
def capabilities_for_roles(roles) -> frozenset[str]:
    """Derive governance-namespace capabilities from trusted roles only."""

    granted: set[str] = set()

    for role in roles or ():
        granted.update(ROLE_PERMISSIONS.get(role, set()))

    return frozenset(granted)


#
# Translation from the governance permission namespace to the agent capability
# namespace used by `models.Permissions`.  Without this mapping a governed
# identity would lose every agent capability purely because the two vocabularies
# use different names.
#
AGENT_CAPABILITY_BY_GOVERNANCE_PERMISSION: dict[str, str] = {
    "can_discover_metadata": "can_discover_metadata",
    "can_run_read_queries": "can_execute_read_queries",
    "can_build_visualizations": "can_build_visualizations",
    "can_build_dashboards": "can_build_dashboards",
    "can_run_data_quality_checks": "can_run_data_quality_checks",
    "can_generate_insights": "can_generate_insights",
    "can_view_data_quality_samples": "can_view_data_quality_samples",
}


def agent_capabilities_for_roles(roles) -> frozenset[str]:
    """Derive agent-namespace capabilities from trusted roles only."""

    granted: set[str] = set()

    for permission in capabilities_for_roles(roles):
        capability = AGENT_CAPABILITY_BY_GOVERNANCE_PERMISSION.get(
            permission
        )

        if capability:
            granted.add(capability)

    return frozenset(granted)