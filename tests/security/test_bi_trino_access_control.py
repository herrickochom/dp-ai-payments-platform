"""JOB F2/F3 - FAIL-FIRST PROOFS: BI Trino access control and dashboard policy.

Trino file-based access control is the established mechanism in this platform
(``access-control.name=file`` -> ``rules.json``). It is deny-by-default, so the
tests evaluate the committed rule file directly and deterministically rather
than contacting a live coordinator.

Proven invariants:

* the BI service identities ``superset_bi`` / ``metabase_bi`` may SELECT only
  the server-owned approved BI tables;
* they are DENIED every identity-bearing consumption dataset;
* they are DENIED bronze, silver, silver_vault and gold outright;
* transform (``dbt``), developer (``hochom``) and the agent-api one-table
  restriction are preserved unchanged;
* SQL Lab is disabled on the controlled BI connection.

No live Trino, Superset or Metabase is contacted.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
AGENT_API_DIR = ROOT / "services" / "agent-api"

for _path in (ROOT, AGENT_API_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))


from bi_policy import (
    APPROVED_BI_TABLES,
    BI_CATALOG,
    BI_SCHEMA,
    IDENTITY_BEARING_DATASETS,
    METABASE_BI_TRINO_USER,
    SUPERSET_BI_TRINO_USER,
)


RULES_PATH = ROOT / "platform" / "trino" / "etc" / "rules.json"
ACCESS_CONTROL_PATH = (
    ROOT / "platform" / "trino" / "etc" / "access-control.properties"
)
MANIFEST_PATH = (
    ROOT / "platform" / "superset" / "imports"
    / "pdm_executive_datasources.yaml"
)
COMPOSE_PATH = ROOT / "docker-compose.yaml"

BI_IDENTITIES = (SUPERSET_BI_TRINO_USER, METABASE_BI_TRINO_USER)

DENIED_SCHEMAS = ("bronze", "silver", "silver_vault", "gold")


@pytest.fixture(scope="module")
def rules():
    return json.loads(RULES_PATH.read_text(encoding="utf-8"))


def _matches(rule_value: str, candidate: str) -> bool:
    """Trino file access control treats a rule value as a regular expression."""

    return re.fullmatch(rule_value, candidate) is not None


def can_select(rules, user: str, catalog: str, schema: str, table: str) -> bool:
    """Evaluate SELECT authority the way Trino's file ruleset would.

    A privilege is granted only when a matching table rule lists it. No match
    means DENY, which is Trino's default-deny behaviour.
    """

    if not any(
        _matches(rule["user"], user)
        and _matches(rule["catalog"], catalog)
        and rule["allow"] != "none"
        for rule in rules["catalogs"]
    ):
        return False

    for rule in rules["tables"]:
        if not _matches(rule["user"], user):
            continue
        if not _matches(rule["catalog"], catalog):
            continue
        if not _matches(rule["schema"], schema):
            continue
        if not _matches(rule["table"], table):
            continue
        return "SELECT" in rule.get("privileges", [])

    return False


# ---------------------------------------------------------------------------
# Positive: approved BI tables
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("user", BI_IDENTITIES)
def test_bi_identity_can_select_approved_bi_tables(rules, user):
    for table in sorted(APPROVED_BI_TABLES):
        assert can_select(rules, user, BI_CATALOG, BI_SCHEMA, table), (
            f"{user} must be able to read approved BI table {table}"
        )


# ---------------------------------------------------------------------------
# Negative: identity-bearing datasets
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("user", BI_IDENTITIES)
@pytest.mark.parametrize("table", sorted(IDENTITY_BEARING_DATASETS))
def test_bi_identity_is_denied_identity_bearing_tables(rules, user, table):
    assert not can_select(rules, user, BI_CATALOG, BI_SCHEMA, table), (
        f"SECURITY: {user} must not be able to read identity-bearing dataset "
        f"{table}"
    )


# ---------------------------------------------------------------------------
# Negative: raw and non-BI schemas
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("user", BI_IDENTITIES)
@pytest.mark.parametrize("schema", DENIED_SCHEMAS)
def test_bi_identity_is_denied_non_bi_schemas(rules, user, schema):
    assert not can_select(
        rules, user, BI_CATALOG, schema, "anything_at_all"
    ), f"SECURITY: {user} must not reach {BI_CATALOG}.{schema}"


@pytest.mark.parametrize("user", BI_IDENTITIES)
def test_bi_identity_is_denied_wildcard_bi_access(rules, user):
    """No `.*` table rule may remain for a canonical BI identity."""

    for rule in rules["tables"]:
        if _matches(rule["user"], user):
            assert rule["table"] != ".*", (
                f"SECURITY: {user} must not hold blanket table access"
            )


@pytest.mark.parametrize("user", BI_IDENTITIES)
def test_bi_identity_has_no_write_privileges(rules, user):
    for rule in rules["tables"]:
        if _matches(rule["user"], user):
            assert set(rule["privileges"]) <= {"SELECT"}


@pytest.mark.parametrize("user", BI_IDENTITIES)
def test_bi_identity_is_denied_other_catalogs(rules, user):
    assert not can_select(rules, user, "system", "metadata", "query")
# ---------------------------------------------------------------------------
# Preserved authority
# ---------------------------------------------------------------------------


def test_transform_authority_is_preserved(rules):
    assert can_select(rules, "dbt", BI_CATALOG, "silver", "slv_pdm_loans")
    assert can_select(rules, "dbt", "system", "metadata", "query")


def test_agent_api_single_table_restriction_is_preserved(rules):
    assert can_select(
        rules,
        "agent-api",
        BI_CATALOG,
        BI_SCHEMA,
        "cns_pdm_executive_overview",
    )
    assert not can_select(
        rules,
        "agent-api",
        BI_CATALOG,
        BI_SCHEMA,
        "cns_pdm_social_impact",
    )


def test_developer_authority_is_preserved(rules):
    assert can_select(rules, "hochom", BI_CATALOG, "gold", "anything")


# ---------------------------------------------------------------------------
# Policy coherence and configuration
# ---------------------------------------------------------------------------


def test_rules_file_matches_the_server_owned_bi_policy(rules):
    """The committed rules must not drift from bi_policy.py."""

    for user in BI_IDENTITIES:
        granted = {
            rule["table"]
            for rule in rules["tables"]
            if rule["user"] == user
            and rule["catalog"] == BI_CATALOG
            and rule["schema"] == BI_SCHEMA
            and rule.get("privileges") == ["SELECT"]
        }
        assert granted == set(APPROVED_BI_TABLES), (
            f"{user} table grants have drifted from the approved BI policy"
        )


def test_access_control_is_the_established_file_mechanism():
    properties = ACCESS_CONTROL_PATH.read_text(encoding="utf-8")

    assert "access-control.name=file" in properties
    assert "rules.json" in properties


def test_sql_lab_is_disabled_on_the_controlled_bi_connection():
    manifest = yaml.safe_load(MANIFEST_PATH.read_text(encoding="utf-8"))
    database = manifest["databases"][0]

    assert database["expose_in_sqllab"] is False, (
        "SECURITY: the automated BI connection must not expose SQL Lab"
    )
    assert database["allow_dml"] is False
    assert database["allow_ctas"] is False


def _superset_init_sqllab_assignments():
    """Parse every SQL Lab assignment in the repository-controlled BI connection.

    The superset-init command is an embedded heredoc inside a compose command
    string. Rather than matching one exact substring (which silently misses
    whitespace variants such as ``expose_in_sqllab = True``), the parsed source
    is normalised and every assignment is evaluated.

    Returns ``[(branch_label, assigned_value), ...]`` where the value is the
    literal text assigned, upper-cased.
    """

    compose = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    shell = compose["services"]["superset-init"]["command"][-1]

    assignments = []
    branch = "unknown"

    for line in shell.splitlines():
        normalised = " ".join(line.split())

        if normalised.startswith("database = Database("):
            branch = "create"
        elif normalised.startswith("if database is None:"):
            branch = "create"
        elif normalised.startswith("else:"):
            branch = "update"

        match = re.search(
            r"(?:database\.)?expose_in_sqllab\s*[=:]\s*(\S+?),?$",
            normalised,
        )

        if match:
            assignments.append((branch, match.group(1).upper()))

    return assignments


def test_superset_database_create_path_disables_sql_lab():
    """F3-2 create branch must independently declare SQL Lab off.

    ``superset db upgrade`` plus this script provisions a fresh Superset; on
    first run the database row does not exist and the CREATE branch executes.
    """

    create_values = [
        value
        for branch, value in _superset_init_sqllab_assignments()
        if branch == "create"
    ]

    assert create_values, (
        "SECURITY: the Superset database CREATE path must declare "
        "expose_in_sqllab explicitly"
    )

    for value in create_values:
        assert value == "FALSE", (
            f"SECURITY: CREATE path enables SQL Lab ({value})"
        )


def test_superset_database_update_path_disables_sql_lab():
    """F3-2 update branch must independently declare SQL Lab off.

    ``superset-init`` re-runs on every deploy, so once the database row exists
    the UPDATE branch is what actually executes. An earlier revision left this
    branch at ``True``, which silently re-enabled SQL Lab on every redeploy.
    """

    update_values = [
        value
        for branch, value in _superset_init_sqllab_assignments()
        if branch == "update"
    ]

    assert update_values, (
        "SECURITY: the Superset database UPDATE path must declare "
        "expose_in_sqllab explicitly, otherwise a redeploy can re-enable "
        "SQL Lab"
    )

    for value in update_values:
        assert value == "FALSE", (
            f"SECURITY: UPDATE path re-enables SQL Lab ({value})"
        )


def test_manifest_and_superset_init_agree_on_sql_lab():
    """The manifest and the init script must not disagree about SQL Lab."""

    manifest_value = yaml.safe_load(
        MANIFEST_PATH.read_text(encoding="utf-8")
    )["databases"][0]["expose_in_sqllab"]

    assert manifest_value is False

    assert {
        value for _, value in _superset_init_sqllab_assignments()
    } == {"FALSE"}


def test_compose_disables_sql_lab_and_separates_publisher_identity():
    compose = COMPOSE_PATH.read_text(encoding="utf-8")

    #
    # Every SQL Lab assignment in the controlled BI connection must be False,
    # regardless of formatting. This deliberately replaces an earlier
    # substring check that could not see `expose_in_sqllab = True`.
    #
    assert _superset_init_sqllab_assignments(), (
        "no SQL Lab assignment was found in superset-init; the controlled "
        "BI connection must declare one explicitly"
    )

    for branch, value in _superset_init_sqllab_assignments():
        assert value == "FALSE", (
            "SECURITY: SQL Lab must be disabled on the automated BI "
            f"connection, but the {branch} path sets expose_in_sqllab={value}"
        )

    #
    # The publication adapter must no longer be wired to the admin credential.
    #
    assert "SUPERSET_PUBLISHER_USERNAME" in compose
    assert "SUPERSET_PUBLISHER_PASSWORD" in compose

    agent_api_block = compose.split("  agent-api:")[1].split(
        "\n  metabase:"
    )[0]
    assert "SUPERSET_ADMIN_USERNAME" not in agent_api_block
    assert "SUPERSET_ADMIN_PASSWORD" not in agent_api_block


def test_bi_manifest_uses_the_canonical_identity_and_no_credential():
    manifest = yaml.safe_load(MANIFEST_PATH.read_text(encoding="utf-8"))
    uri = manifest["databases"][0]["sqlalchemy_uri"]

    #
    # F8: the manifest must authenticate as the canonical BI identity and must
    # never carry a literal credential. A `${...}` placeholder is required
    # because the password is injected at runtime from the secret source.
    #
    assert f"trino://{SUPERSET_BI_TRINO_USER}" in uri
    assert "pdm@" not in uri
    assert "${" in uri, (
        "SECURITY: the BI manifest must not embed a literal Trino password; "
        "it must reference a runtime-injected secret variable"
    )

    tables = {t["table_name"] for t in manifest["databases"][0]["tables"]}
    assert tables == set(APPROVED_BI_TABLES)
    assert not tables & IDENTITY_BEARING_DATASETS


def test_no_placeholder_credential_was_added_to_the_rule_configuration():
    """Passwords belong in password.db / the secret provider, never in rules."""

    raw = RULES_PATH.read_text(encoding="utf-8").upper()

    for marker in ("PASSWORD", "SECRET", "PRIVATE KEY"):
        assert marker not in raw


# ---------------------------------------------------------------------------
# F3-4 - repository-controlled dashboard exposure policy
# ---------------------------------------------------------------------------


def test_publication_never_marks_a_dashboard_published_or_public():
    """Repository-controlled publication must not expose assets anonymously."""

    source = (AGENT_API_DIR / "bi_adapter.py").read_text(encoding="utf-8")

    assert '"published": False' in source
    assert '"published": True' not in source
    assert '"owners": []' in source


def test_trino_requires_authentication_before_authorisation():
    """F5 critical: the coordinator must not trust an unauthenticated identity.

    `http-server.authentication.allow-insecure-over-http=true` made Trino skip
    authentication on the plaintext port and accept the caller's `X-Trino-User`
    verbatim, so any reachable client could assert `hochom` and inherit its
    `allow: all` + OWNERSHIP grants on the whole Iceberg warehouse. This test
    pins the fail-closed property so it cannot be silently reintroduced.
    """

    config = (
        ROOT / "platform" / "trino" / "etc" / "config.properties"
    ).read_text(encoding="utf-8")

    assert "http-server.authentication.type=PASSWORD" in config
    assert "http-server.authentication.allow-insecure-over-http=false" in config
    assert "http-server.authentication.allow-insecure-over-http=true" not in config


def test_trino_coordinator_is_not_published_on_all_interfaces():
    """F8: the coordinator must not be reachable from arbitrary networks."""

    compose = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))

    for port in compose["services"]["trino"]["ports"]:
        # compose short syntax is [HOST:]CONTAINER[/PROTOCOL]
        host_binding = str(port).split(":")[0]
        assert host_binding in {"127.0.0.1", "localhost"}, (
            "SECURITY: the Trino coordinator must be bound to loopback only, "
            f"found {port}"
        )
