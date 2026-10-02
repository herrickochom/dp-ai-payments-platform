"""Permission policy for the least-privilege ``agent_publisher`` identity.

Purpose
-------
The agent publishes governed BI assets into Superset through the REST API.
That is a narrow capability: create or update a dashboard or chart, and read
back what it wrote. It is emphatically *not* database administration, SQL Lab
admission, or user/role administration.

This module holds the policy only, so it is importable and testable without a
Superset installation. ``provision_agent_publisher`` applies it.

Design constraints
------------------
* **Permission names are discovered, not assumed.** The allow-list is
  validated against the permission-view rows that actually exist in the
  installed Superset metadata database at run time. If a named permission is
  absent, provisioning fails closed rather than silently substituting another,
  so a Superset upgrade cannot quietly widen or narrow the role.
* **No wildcard.** ``all_database_access``, ``all_datasource_access``,
  ``all_query_access``, ``copyrole``, ``can_write``, ``can_export``,
  ``can_import_`` and the SQL Lab permissions are never granted.
* **Least privilege by default.** Anything not explicitly required is absent.
"""

from __future__ import annotations

#: The dedicated identity and role share this name.
PUBLISHER = "agent_publisher"

#: Minimal permission set for the bounded publication workflow.
#:
#: ``can_write`` on Dashboard and Chart is the write path. ``can_read`` lets the
#: publisher resolve an existing asset to update rather than duplicating it,
#: and ``can_read`` on Dataset lets it resolve a dataset it is attaching.
#: ``can_export`` is deliberately NOT granted: it is a data-egress path.
#:
#: ``can_list`` is absent on purpose: it does not exist on Chart or Dashboard
#: in Superset 6.1.0, and provisioning validates this set against the installed
#: inventory rather than substituting a different permission when one is
#: missing. The REST listing the workflow needs is covered by ``can_read`` on
#: the Dashboard and Chart view menus.
#:
#: ``SecurityRestApi:can_read`` is required because the only supported way to
#: obtain the CSRF token that FAB demands on every mutating request is
#: ``GET /api/v1/security/csrf_token/``, which is gated by
#: ``@permission_name("read")`` on that view menu. Without it every publication
#: mutation fails CSRF before authorization is reached.
#:
#: Runtime verified against Superset 6.1.0 / Flask AppBuilder 5.0.2 as granting
#: access to the CSRF token endpoint without granting the tested identity,
#: role, permission, database or dashboard administration surfaces. That
#: assertion is version-scoped and must be re-proved after any upgrade; see
#: ``tests/security/test_publisher_authority_contract.py``.
REQUIRED_PERMISSIONS: tuple[tuple[str, str], ...] = (
    ("Dashboard", "can_read"),
    ("Dashboard", "can_write"),
    ("Chart", "can_read"),
    ("Chart", "can_write"),
    ("Dataset", "can_read"),
    ("SecurityRestApi", "can_read"),
)

#: The only view menus the publisher may hold ``can_write`` on. This is the
#: entire write scope of the role: it publishes BI assets and nothing else.
WRITABLE_VIEWS: frozenset[str] = frozenset({"Dashboard", "Chart"})

#: Roles the publisher must never hold, because each confers broad authority.
FORBIDDEN_ROLES: frozenset[str] = frozenset(
    {"Admin", "Alpha", "Public", "Gamma", "sql_lab"}
)

#: Permission names that must never be granted to this role.
FORBIDDEN_ACTIONS: frozenset[str] = frozenset(
    {
        # Identity and role administration.
        "can_list_roles",
        "can_list_role_permissions",
        "can_add_role_permissions",
        "can_update_role_users",
        "can_update_role_groups",
        "copyrole",
        "resetpasswords",
        "resetmypassword",
        "userinfoedit",
        "can_userinfo",
        # Administration of any object class is forbidden on any view menu
        # EXCEPT the two the publisher must write: Dashboard and Chart.
        # ``can_write`` is therefore scoped rather than banned outright.
        "can_write",
        "can_delete",
        "can_add",
        "can_import_",
        "can_edit",
        # SQL Lab / ad-hoc query against any database.
        "can_sqllab",
        "can_execute_sql_query",
        "can_query",
        "can_get_results",
        "can_post",
        "can_sqllab_history",
        "can_view_query",
        # Whole-database and whole-datasource access.
        "all_database_access",
        "all_datasource_access",
        "all_query_access",
        # Data egress.
        "can_export",
        "can_export_csv",
        "can_export_streaming_csv",
        "can_download",
        "can_get",
        "can_csv",
        # Guest / public sharing.
        "can_grant_guest_token",
        "can_share_dashboard",
        "can_share_chart",
        "can_set_embedded",
        "can_get_embedded",
        "can_delete_embedded",
        # Open the management surfaces themselves.
        "can_list",
        "can_show",
        "can_info",
    }
)

#: Permission actions that grant reading of sensitive management surfaces.
#:
#: ``SecurityRestApi`` is deliberately absent. Runtime verification against
#: Superset 6.1.0 / Flask AppBuilder 5.0.2 showed that ``can_read`` on that
#: view menu exposes only the CSRF token endpoint, while role listing and guest
#: token issuance are gated behind their own distinct permissions
#: (``can_list_roles`` and ``can_grant_guest_token``), both of which remain
#: forbidden below. This is version-scoped: ``can_read`` on the view menu must
#: be re-proved after any Superset or FAB upgrade.
FORBIDDEN_VIEWS: frozenset[str] = frozenset(
    {
        "user",
        "SQLLab",
        "SavedQuery",
        "Query",
        "Log",
        # Database administration and the Flask-AppBuilder identity/permission
        # management views. The publisher reaches neither; naming them here
        # means a future change to the required set cannot quietly add them.
        "Database",
        "ab_user",
        "ab_role",
        "ab_group",
        "ab_permission",
        "ab_permission_view",
        "Role",
        "Group",
        # Superset's wildcard-access view menus. These are the broadest grants
        # the platform can express and must never reach a machine publisher.
        "all_database_access",
        "all_datasource_access",
        "all_query_access",
    }
)


class PublisherPolicyError(Exception):
    """The requested publisher authority is not acceptable."""


def required_permissions() -> tuple[tuple[str, str], ...]:
    """Return the minimum permission set for the publication workflow."""
    return REQUIRED_PERMISSIONS


def forbidden_permissions() -> frozenset[tuple[str, str]]:
    """Return every (view_menu, action) pair that must never be granted."""
    pairs = {(action, "") for action in FORBIDDEN_ACTIONS}
    pairs |= {(view, "can_read") for view in FORBIDDEN_VIEWS}
    pairs |= {(view, "can_write") for view in FORBIDDEN_VIEWS}
    return frozenset(pairs)


def missing_against_inventory(
    required: tuple[tuple[str, str], ...], available: set[tuple[str, str]]
) -> list[tuple[str, str]]:
    """Return required permissions absent from the installed inventory."""
    return [pair for pair in required if pair not in available]


def is_forbidden(view_menu: str, action: str) -> bool:
    """Whether granting this permission would confer prohibited authority."""
    # The publisher must write dashboards and charts; ``can_write`` is scoped to
    # those two view menus and forbidden everywhere else.
    if action == "can_write":
        return view_menu not in WRITABLE_VIEWS

    if action in FORBIDDEN_ACTIONS:
        return True
    if view_menu in FORBIDDEN_VIEWS and action in {
        "can_read",
        "can_write",
        "can_list",
        "can_show",
    }:
        return True
    return False


def assert_no_forbidden(granted: set[tuple[str, str]]) -> None:
    """Fail closed if any granted permission carries prohibited authority.

    Guards two failure modes: a prohibited permission appearing in the role at
    all, and a role that somehow carries none of its required permissions,
    which would make publication impossible and indicates a broken policy.
    """
    for view_menu, action in sorted(granted):
        if is_forbidden(view_menu, action):
            raise PublisherPolicyError(
                f"refusing prohibited permission: {view_menu}:{action}"
            )
    if not set(granted) & set(REQUIRED_PERMISSIONS):
        raise PublisherPolicyError(
            "publisher role would carry none of its required permissions"
        )