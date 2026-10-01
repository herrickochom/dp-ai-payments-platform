"""JOB F1 - FAIL-FIRST PROOFS: trusted identity -> server authority binding.

Authentication and authorisation are separate decisions, and these tests prove
they stay separate:

    verified JWT  ->  TrustedIdentity  ->  server subject-role binding
                  ->  ServerAuthorityResolver  ->  A3 governance

The central invariant under test::

    authenticated  !=  authorised

A valid token proves WHO.  Only the server-owned binding decides WHAT.  Token
role/permission claims and caller-body role/permission claims must both be
inert.
"""

from __future__ import annotations

import pytest
import yaml

from f1_identity_fixtures import (  # noqa: F401  (path bootstrap)
    TEST_AUDIENCE,
    TEST_ISSUER,
    StaticSigningKeyResolver,
    bearer,
    make_token,
)

from agent_asgi_client import LocalClient
from authority import Authority, ServerAuthorityResolver
from authentication import TokenVerifier
from config import Settings
from role_bindings import RoleBindingError, load_role_bindings


BINDINGS = {
    "f1-analyst": frozenset({"programme_analyst"}),
    "f1-publisher": frozenset({"bi_publisher"}),
}

MALICIOUS_CLAIMS = {
    "roles": ["governance_administrator"],
    "groups": ["platform_admins"],
    "permissions": ["can_view_restricted_data"],
    "can_publish_bi_assets": True,
    "scope": "admin:all",
}


# ---------------------------------------------------------------------------
# Role-binding configuration
# ---------------------------------------------------------------------------


def test_role_bindings_load_from_server_owned_file(tmp_path):
    bindings_file = tmp_path / "roles.yaml"
    bindings_file.write_text(
        yaml.safe_dump(
            {
                "subjects": {
                    "f1-analyst": {"roles": ["programme_analyst"]},
                    "f1-publisher": {"roles": ["bi_publisher"]},
                }
            }
        ),
        encoding="utf-8",
    )

    assert load_role_bindings(bindings_file) == BINDINGS


def test_no_role_bindings_configured_means_no_authority():
    assert load_role_bindings(None) == {}
    assert load_role_bindings("") == {}


def test_unknown_platform_role_is_a_configuration_error(tmp_path):
    bindings_file = tmp_path / "roles.yaml"
    bindings_file.write_text(
        yaml.safe_dump(
            {"subjects": {"f1-x": {"roles": ["superuser"]}}}
        ),
        encoding="utf-8",
    )

    with pytest.raises(RoleBindingError):
        load_role_bindings(bindings_file)


def test_missing_role_bindings_file_fails_closed(tmp_path):
    with pytest.raises(RoleBindingError):
        load_role_bindings(tmp_path / "absent.yaml")


def test_malformed_role_bindings_fails_closed(tmp_path):
    bindings_file = tmp_path / "roles.yaml"
    bindings_file.write_text("subjects: [not, a, mapping]", encoding="utf-8")

    with pytest.raises(RoleBindingError):
        load_role_bindings(bindings_file)


# ---------------------------------------------------------------------------
# Authentication is not authorisation
# ---------------------------------------------------------------------------


def _verifier():
    return TokenVerifier(
        issuer=TEST_ISSUER,
        audience=TEST_AUDIENCE,
        signing_keys=StaticSigningKeyResolver(),
    )


def test_valid_token_with_malicious_claims_authenticates_but_grants_nothing():
    """A correctly signed token claiming admin authority must be inert."""

    identity = _verifier().verify(
        bearer(
            make_token(
                subject="f1-analyst",
                extra_claims=MALICIOUS_CLAIMS,
            )
        )
    )

    assert identity.authenticated is True
    assert identity.subject_id == "f1-analyst"

    #
    # Nothing from the token survives into authority: only the subject is
    # carried forward, and roles come from the server binding.
    #
    authority = ServerAuthorityResolver(
        role_authorities=BINDINGS
    ).resolve_verified(identity.subject_id)

    assert authority.roles == ("programme_analyst",)
    assert "governance_administrator" not in authority.roles
    assert "can_publish_bi_assets" not in authority.capabilities


def test_valid_unknown_subject_is_authenticated_but_unprivileged():
    identity = _verifier().verify(
        bearer(make_token(subject="f1-unknown-user"))
    )

    authority = ServerAuthorityResolver(
        role_authorities=BINDINGS
    ).resolve_verified(identity.subject_id)

    assert identity.authenticated is True
    assert authority.roles == ()
    assert authority.capabilities == frozenset()
    assert not authority.permits(
        "can_publish_bi_assets",
        require_authenticated=True,
    )


def test_mapped_subject_receives_only_its_server_bound_authority():
    identity = _verifier().verify(
        bearer(make_token(subject="f1-publisher"))
    )

    authority = ServerAuthorityResolver(
        role_authorities=BINDINGS
    ).resolve_verified(identity.subject_id)

    assert authority.roles == ("bi_publisher",)
    assert authority.permits(
        "can_publish_bi_assets",
        require_authenticated=True,
    )


def test_trusted_identity_carries_no_credential_or_claim_material():
    """The identity object must not retain the token or its authority claims."""

    identity = _verifier().verify(
        bearer(
            make_token(
                subject="f1-analyst",
                extra_claims=MALICIOUS_CLAIMS,
            )
        )
    )

    exposed = {
        name: getattr(identity, name)
        for name in identity.__dataclass_fields__
    }

    for forbidden in ("token", "claims", "roles", "permissions"):
        assert forbidden not in exposed

    assert set(exposed) == {
        "subject_id",
        "issuer",
        "authentication_method",
        "authenticated",
    }

    #
    # Audit output is restricted to verified, non-secret identity fields.
    #
    assert set(identity.audit_fields()) == {
        "subject_id",
        "issuer",
        "authentication_method",
    }