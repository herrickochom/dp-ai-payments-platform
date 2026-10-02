"""Regression guard for the approved ``agent_publisher`` permission contract.

The contract is exactly six permissions:

    Dashboard:can_read, Dashboard:can_write,
    Chart:can_read,      Chart:can_write,
    Dataset:can_read,    SecurityRestApi:can_read

``SecurityRestApi:can_read`` exists solely because Flask-AppBuilder demands a
CSRF token on every mutating request and the only supported source is
``GET /api/v1/security/csrf_token/``. Runtime verification against Superset
6.1.0 / Flask-AppBuilder 5.0.2 showed that ``can_read`` on that view menu
exposes only the CSRF token endpoint, while role listing and guest-token
issuance are gated behind their own permissions.

**Version sensitivity.** That finding is not inferred from version numbers; it
was measured. ``test_security_surface_probes_expect_authorization_denial``
pins the runtime expectations that would break if a future upgrade caused
``can_read`` to expose further security-administration methods: the probe
classifies each response, so a widened surface would surface as ``MISMATCH``
against ``AUTHZ_DENIED`` rather than passing silently. Re-run
``platform/superset/authority_probe.py`` after any Superset or FAB upgrade and
treat a mismatch on these controls as a security regression.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

#: The approved contract. Any drift fails here before it reaches the runtime.
APPROVED_PERMISSIONS: frozenset[tuple[str, str]] = frozenset(
    {
        ("Dashboard", "can_read"),
        ("Dashboard", "can_write"),
        ("Chart", "can_read"),
        ("Chart", "can_write"),
        ("Dataset", "can_read"),
        ("SecurityRestApi", "can_read"),
    }
)

#: Permissions whose absence from the role is itself a control.
FORBIDDEN_SENSITIVE: tuple[tuple[str, str], ...] = (
    ("SecurityRestApi", "can_grant_guest_token"),
    ("SecurityRestApi", "can_list_roles"),
    ("SecurityRestApi", "can_write"),
    ("SQLLab", "can_read"),
    ("SQLLab", "can_write"),
    ("user", "can_read"),
    ("user", "can_write"),
    ("Database", "can_read"),
    ("Database", "can_write"),
    ("Dataset", "all_datasource_access"),
    ("Chart", "can_export"),
    ("Dashboard", "can_export"),
    ("all_query_access", "can_read"),
)


def _load(name: str, relative: str):
    """Load a repository module by path (``platform`` shadows the stdlib)."""
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _policy():
    return _load("publisher_policy", "platform/superset/publisher_policy.py")


def _probe():
    return _load("authority_probe", "platform/superset/authority_probe.py")


def test_required_permissions_are_exactly_the_approved_six():
    granted = frozenset(_policy().required_permissions())
    assert granted == APPROVED_PERMISSIONS, (
        "SECURITY: the agent_publisher contract drifted from the approved six "
        f"permissions. Missing={sorted(APPROVED_PERMISSIONS - granted)} "
        f"Unexpected={sorted(granted - APPROVED_PERMISSIONS)}"
    )
    assert len(_policy().required_permissions()) == 6, (
        "SECURITY: duplicate entries in the required permission set"
    )


def test_sensitive_permissions_are_never_granted():
    policy = _policy()
    granted = set(policy.required_permissions())
    for pair in FORBIDDEN_SENSITIVE:
        assert pair not in granted, (
            f"SECURITY: {pair[0]}:{pair[1]} must never be granted to the publisher"
        )
        assert policy.is_forbidden(*pair), (
            f"SECURITY: {pair[0]}:{pair[1]} is not classified as forbidden"
        )


def test_policy_self_check_accepts_the_contract():
    policy = _policy()
    policy.assert_no_forbidden(set(policy.required_permissions()))


def test_security_surface_probes_expect_authorization_denial():
    """Version-sensitive guard against ``SecurityRestApi:can_read`` widening.

    These are the runtime expectations that prove the CSRF permission confers
    nothing beyond the token. If a Superset or Flask-AppBuilder upgrade caused
    ``can_read`` to expose further methods, the probe would classify the
    response as something other than ``AUTHZ_DENIED`` and report MISMATCH.
    """
    probe = _probe()
    expectations = {name: expected for name, _m, _p, _d, expected in probe.CONTROLS}

    must_be_denied = (
        "SQLLAB_execute",
        "SQLLAB_history",
        "DATABASE_list",
        "DATABASE_read",
        "DATABASE_mutate",
        "DATABASE_create",
        "USER_ADMIN_list_users",
        "ROLE_ADMIN_list_roles",
        "GUEST_TOKEN_create",
        "EXPORT_chart_csv",
    )
    for control in must_be_denied:
        assert control in expectations, f"{control} is no longer probed"
        assert expectations[control] == probe.AUTHZ_DENIED, (
            f"SECURITY: {control} must be expected to be authorization-denied; "
            "relaxing this expectation would hide a widened CSRF permission"
        )


def test_probe_classifies_csrf_separately_from_authorization():
    """A CSRF rejection must never be classified as an authorization denial."""
    probe = _probe()
    assert probe.classify(400, "The CSRF token is missing.") == probe.CSRF_FAILURE
    assert probe.classify(403, '{"message":"Forbidden"}') == probe.AUTHZ_DENIED
    assert probe.classify(401, "Unauthorized") == probe.AUTH_FAILURE
    assert probe.classify(201, "{}") == probe.SUCCESS