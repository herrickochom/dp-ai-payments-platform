"""Regression tests for Trino password authentication and the Superset BI link.

Two independent root causes are pinned here.

**Password store.** ``io.trino.plugin.password.file.EncryptionUtil.
getHashingAlgorithm`` rejects any BCrypt entry whose cost factor is below 8
and raises ``HashedPasswordException("Minimum cost of BCrypt password must be
8")``. The failure is not reported as a rejected credential; it propagates out
of ``PasswordAuthenticator.authenticate`` and is surfaced to the client as

    HTTP 500 java.lang.RuntimeException: Authentication error

``htpasswd`` produces cost-5 hashes by default, and ``htpasswd -C <cost>``
*without* ``-B`` silently emits an Apache MD5 (``$apr1$``) hash instead, which
Trino does not support at all. Both failure modes look identical from the
outside, so the provisioning contract is pinned here.

**BI connection shape.** Three layers in Superset have to agree, and each has
been the site of a production failure:

* ``dbs.sqlalchemy_uri`` must use the ``trino://`` dialect scheme. The URL
  scheme resolves to a SQLAlchemy plugin; ``https://`` resolves to
  ``sqlalchemy.dialects:https``, which does not exist.

* ``dbs.encrypted_extra`` is consumed by ``TrinoEngineSpec.
  update_params_from_encrypted_extra``, which **overrides** the base class.
  The override recognises exactly two keys, ``auth_method`` and
  ``auth_params``; if ``auth_method`` is absent it returns without touching
  ``params``, silently discarding everything else. On this image, the only way
  to get ``http_scheme="https"`` into ``connect_args`` is to declare a Trino
  auth method here.

* The CA bundle path cannot come from ``encrypted_extra`` — the override does
  not read a ``verify`` value. It goes on the URI query string, JSON-quoted,
  because the dialect calls ``json.loads()`` on it.

These tests never read a live password database: ``password.db`` is
gitignored, holds real credentials, and is not present in CI.
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]

CATALOG_PROPERTIES = ROOT / "platform" / "trino" / "etc" / "catalog" / "iceberg.properties"
COMPOSE_PATH = ROOT / "docker-compose.yaml"
PASSWORD_DB = ROOT / "platform" / "trino" / "etc" / "password.db"
REGISTRATION_MODULE = ROOT / "platform" / "superset" / "register_bi_connection.py"

#: Minimum cost enforced by Trino's file password authenticator.
TRINO_MINIMUM_BCRYPT_COST = 8

BCRYPT_ENTRY = re.compile(r"^([^:\s]+):\$2([abxy])\$(\d\d)\$")


# ---------------------------------------------------------------------------
# Password store contract
# ---------------------------------------------------------------------------


def test_trino_enforces_a_minimum_bcrypt_cost() -> None:
    """Documents the runtime constraint that caused the outage."""
    assert TRINO_MINIMUM_BCRYPT_COST == 8


def is_usable_by_trino(entry: str) -> bool:
    """Mirror of Trino's ``PasswordStore`` acceptance rule.

    An entry is usable when it is a BCrypt hash (``$2a$``/``$2b$``/``$2x$``/
    ``$2y$``) whose cost is at least 8, or a PBKDF2 hash, which Trino only
    recognises by the presence of a colon in the stored value.
    """
    match = BCRYPT_ENTRY.match(entry)
    if match is not None:
        return int(match.group(3)) >= TRINO_MINIMUM_BCRYPT_COST
    return ":" in entry.split(":", 1)[1]


@pytest.mark.parametrize(
    ("entry", "usable"),
    [
        # htpasswd's default cost, which is what silently broke the BI identities
        ("superset_bi:$2y$05$" + "a" * 53, False),
        # still below the enforced minimum
        ("superset_bi:$2y$07$" + "a" * 53, False),
        # `htpasswd -C <cost>` without -B emits Apache MD5, which Trino rejects
        ("superset_bi:$apr1$" + "a" * 8, False),
        # compliant entries, as the file now holds
        ("superset_bi:$2y$10$" + "a" * 53, True),
        ("agent-api:$2y$12$" + "a" * 53, True),
        ("superset_bi:$2b$10$" + "a" * 53, True),
    ],
)
def test_non_compliant_password_entries_are_detected(entry: str, usable: bool) -> None:
    """Entries Trino cannot verify must be classified as unusable.

    This is the check that was missing when the two BI identities were
    provisioned: both were written at cost 5, and because the minimum is
    enforced by raising inside the authenticator, every login failed with
    HTTP 500 rather than a clean 401.
    """
    assert is_usable_by_trino(entry) is usable


def test_no_password_database_is_committed() -> None:
    """The credential store must never enter version control."""
    tracked = subprocess.run(
        ["git", "ls-files", "--error-unmatch", str(PASSWORD_DB.relative_to(ROOT))],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert tracked.returncode != 0, (
        "SECURITY: the Trino password database must not be tracked by git"
    )


# ---------------------------------------------------------------------------
# Catalog credential isolation
# ---------------------------------------------------------------------------


def test_iceberg_catalog_has_no_literal_s3_credential() -> None:
    """F9: the catalog credential must be injected, never committed.

    A literal ``s3.aws-secret-key`` was committed to
    ``platform/trino/etc/catalog/iceberg.properties``, exposing the catalog
    principal to anyone with repository read access.
    """
    properties = CATALOG_PROPERTIES.read_text(encoding="utf-8")

    for key in ("s3.aws-access-key", "s3.aws-secret-key"):
        match = re.search(rf"^{key}=(.*)$", properties, re.M)
        assert match is not None, f"{key} must still be configured"
        value = match.group(1).strip()
        assert value.startswith("${ENV:"), (
            f"SECURITY: {key} contains a literal value; it must be supplied "
            "via ${ENV:...} so the secret stays outside version control"
        )


def test_compose_requires_the_catalog_credential() -> None:
    """The injected catalog credential must fail closed when absent.

    The Trino catalog must consume the same canonical variables that
    ``platform/minio/provision_object_store_identities.sh`` provisions for the
    ``nessie_catalog`` identity. Introducing a second copy of the credential
    under a private name was tried and reverted: it duplicated a live secret
    and created a second place for it to drift.
    """
    compose = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    environment = compose["services"]["trino"]["environment"]

    for key in ("NESSIE_S3_ACCESS_KEY", "NESSIE_S3_SECRET_KEY"):
        assert key in environment, f"{key} must be passed to the Trino service"
        assert f"${{{key}:?" in environment[key], (
            f"SECURITY: {key} must be required, not defaulted, so Trino cannot "
            "start with an implicit credential"
        )

    duplicates = [name for name in environment if "CATALOG_S3" in name]
    assert not duplicates, (
        "SECURITY: the catalog credential must be supplied only through the "
        f"canonical NESSIE_S3_* variables, not duplicated: {duplicates}"
    )


# ---------------------------------------------------------------------------
# MinIO identity provisioning
# ---------------------------------------------------------------------------


def _load_provisioner():
    """Load the provisioning script by path.

    ``platform`` is a standard-library module name, so the repository's
    ``platform/`` directory cannot be imported as a package from the repo
    root. The script is a standalone entrypoint, so it is loaded directly.
    """
    import importlib.util

    path = ROOT / "platform" / "minio" / "provision_object_store_identities.py"
    spec = importlib.util.spec_from_file_location(
        "provision_object_store_identities", path
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_rotation_is_limited_to_the_approved_identities() -> None:
    """A rotation must not be able to target an arbitrary principal.

    ``--rotate`` is the only path that rewrites a stored secret, so it is
    bounded twice: the identity must be one of the eight approved ones, and
    the target user must already exist with its policy attached. Anything else
    would let a caller either invent a principal or silently create one.
    """
    provisioner = _load_provisioner()

    approved = {row[0] for row in provisioner.IDENTITIES}
    assert approved == {
        "raw_ingest",
        "cdc_quarantine",
        "platform_raw_read",
        "nessie_catalog",
        "trino_iceberg",
        "ordinary_transform",
        "restricted_identity_transform",
        "ml_prediction_transform",
    }

    with pytest.raises(provisioner.ProvisionError):
        provisioner.rotate({}, "not_an_identity")

    with pytest.raises(provisioner.ProvisionError):
        provisioner.rotate({}, "")


def test_rotation_target_is_scoped_to_one_identity() -> None:
    """``rotate`` must act on exactly the identity it was given.

    The blast radius of an exposure response is one principal. If rotation
    wrote another identity's secret, an operator intending to re-key the
    Nessie catalog could unknowingly re-key a transform identity as well.
    """
    provisioner = _load_provisioner()

    rows = {row[0]: row for row in provisioner.IDENTITIES}
    _, _, access_name, secret_name = rows["nessie_catalog"]
    assert (access_name, secret_name) == (
        "NESSIE_S3_ACCESS_KEY",
        "NESSIE_S3_SECRET_KEY",
    )

    _, _, trino_access, trino_secret = rows["trino_iceberg"]
    assert trino_secret != secret_name
    assert trino_access != access_name

    with pytest.raises(provisioner.ProvisionError):
        provisioner.rotate({}, "nessie_catalog")


# ---------------------------------------------------------------------------
# BI connection shape
# ---------------------------------------------------------------------------


def _load_bi_registration():
    """Load the BI registration module by path.

    ``platform`` is a standard-library module name, so the repository's
    ``platform/`` directory is not importable as a package from the repo root.
    """
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "register_bi_connection", REGISTRATION_MODULE
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _executable_source(path: Path) -> str:
    """Return the module source with comments and docstrings removed.

    The rationale for these guards is documented in prose that necessarily
    names the insecure URI that was removed. Checking raw text would therefore
    match the documentation rather than the behaviour, so only executable code
    is inspected.
    """
    import ast

    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(
            node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
        ):
            body = node.body
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                node.body = body[1:] or [ast.Pass()]
    return ast.unparse(tree)


def _query_value(query: str, key: str) -> str | None:
    """Return the raw value for ``key`` in a URI query string, or ``None``."""
    for pair in query.split("&"):
        if not pair:
            continue
        k, _, v = pair.partition("=")
        if k == key:
            return v
    return None


def test_registration_has_no_plaintext_or_anonymous_fallback() -> None:
    """The registration step must not embed an insecure default URI.

    The old inline heredoc carried
    ``trino://trino@trino:8080/iceberg/consumption`` as a fallback, so an
    absent environment value silently produced a plaintext anonymous
    connection. Registration now lives in a version-controlled module with no
    such default.
    """
    module = _load_bi_registration()
    code = _executable_source(REGISTRATION_MODULE)

    # The module may construct a ``trino://`` URI (it does so through
    # ``STORED_SCHEME``) but must not embed an anonymous or plaintext
    # literal such as ``trino://trino@``.
    assert "trino://trino@" not in code, (
        "SECURITY: BI registration must not contain an anonymous trino:// URI"
    )

    compose = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    command = " ".join(compose["services"]["superset-init"]["command"])
    assert "trino://trino@" not in command
    assert "register_bi_connection.py" in command, (
        "SECURITY: superset-init must invoke the version-controlled, fail-closed "
        "registration module rather than an inline heredoc"
    )

    assert module.BI_TRINO_USER == "superset_bi"
    assert module.STORED_SCHEME == "trino"
    # The auth method is the mechanism by which the Trino engine spec sets
    # ``connect_args["http_scheme"] = "https"``. If this constant changes,
    # every connection through this module must be re-verified.
    assert module.TRINO_AUTH_METHOD == "basic"


def test_bi_connection_uri_cannot_be_plaintext() -> None:
    """The registered BI connection must be authenticated TLS.

    Three separate defects surfaced during runtime provisioning and each is
    pinned here:

    * the connection defaulted to a plaintext listener, which Trino rejects
      with "TLS/SSL is required for authentication" once
      ``allow-insecure-over-http`` is false;
    * the credential is percent-encoded in the URI, because a generated secret
      may contain ``/`` or ``+`` and an unencoded ``/`` silently truncates the
      authority (``Port could not be cast to integer value``);
    * the certificate must be mounted into the service that runs the query,
      not only into the one-shot initialiser.
    """
    compose = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))

    assert ":?" in compose["services"]["superset-init"]["environment"][
        "TRINO_SQLALCHEMY_URI"
    ], (
        "SECURITY: superset-init must require an explicit BI URI rather than "
        "defaulting to an unauthenticated connection"
    )

    for service in ("superset-init", "superset"):
        assert any(
            "trino-public.crt" in str(volume)
            for volume in compose["services"][service]["volumes"]
        ), (
            f"SECURITY: {service} needs the coordinator certificate mounted so "
            "the driver can verify it"
        )


@pytest.mark.parametrize(
    ("uri", "reason"),
    [
        (None, "required"),
        ("", "required"),
        ("http://superset_bi:pw@trino:8080/iceberg/consumption", "must use one of"),
        # wrong identity
        ("https://trino:pw@trino:8443/iceberg/consumption", "must authenticate as"),
        # correct identity, no credential
        (
            "https://superset_bi@trino:8443/iceberg/consumption",
            "must carry a credential",
        ),
        # correct identity and credential, missing verify
        ("https://superset_bi:pw@trino:8443/iceberg/consumption", "must verify"),
        # verification explicitly disabled
        (
            "https://superset_bi:pw@trino:8443/iceberg/consumption?verify=False",
            "must not disable certificate verification",
        ),
    ],
)
def test_bi_connection_registration_refuses_insecure_forms(uri, reason: str) -> None:
    """Every insecure or incomplete configuration must abort registration."""
    module = _load_bi_registration()

    with pytest.raises(module.BiConnectionRejected) as excinfo:
        module.build_bi_connection_uri(uri)
    assert reason in str(excinfo.value)


def test_bi_connection_credential_is_uri_safe_by_construction() -> None:
    """A credential with reserved characters must round-trip safely.

    The generated secret is base64 and can contain ``+``. The URI is rebuilt
    with the credential decoded then re-encoded, so the value the driver uses
    is the original secret regardless of how the string was assembled.
    """
    module = _load_bi_registration()

    raw = "https://superset_bi:ab%2Fcd+ef@trino:8443/iceberg/consumption?verify=/ca.pem"
    normalised = module.build_bi_connection_uri(raw)

    parts = urlsplit(normalised)
    assert parts.scheme == "trino"
    assert parts.hostname == "trino"
    assert parts.port == 8443
    assert parts.username == "superset_bi"
    assert unquote(parts.password) == "ab/cd+ef"
    assert _query_value(parts.query, "verify") is not None


def test_bi_connection_rejects_unencoded_credential_delimiter() -> None:
    """An unencoded "/" in the credential truncates the authority.

    This is unrecoverable by re-encoding: the tail of the credential has
    already been parsed as the path. It surfaced at runtime as
    ``Port could not be cast to integer value``, so it must be rejected as
    malformed rather than silently accepted.
    """
    module = _load_bi_registration()

    with pytest.raises(module.BiConnectionRejected) as excinfo:
        module.build_bi_connection_uri(
            "https://superset_bi:ab+cd/ef@trino:8443/iceberg/consumption?verify=/ca.pem"
        )
    assert "URI-encoded" in str(excinfo.value)


def test_bi_connection_rejection_messages_never_contain_the_credential() -> None:
    """A rejection must name the defect without echoing the secret."""
    module = _load_bi_registration()

    secret = "sup3rs3cr3t/with+reserved"
    with pytest.raises(module.BiConnectionRejected) as excinfo:
        module.build_bi_connection_uri(f"trino://superset_bi:{secret}@trino:8080/x")
    message = str(excinfo.value)

    assert "sup3rs3cr3t" not in message
    assert secret not in message


def test_bi_connection_stored_uri_uses_trino_scheme_with_bare_host() -> None:
    """The stored URI must use ``trino://`` with a bare host.

    ``https://`` as a scheme resolves to ``sqlalchemy.dialects:https``, which
    does not exist. ``https://`` inside the host field is rejected by
    ``make_url`` and Superset stores a placeholder. Both failures have been
    observed in production. The host must be bare; the HTTP scheme is
    delivered separately, through ``encrypted_extra``.
    """
    module = _load_bi_registration()

    raw = "https://superset_bi:pw@trino:8443/iceberg/consumption?verify=/ca.pem"
    normalised = module.build_bi_connection_uri(raw)
    parts = urlsplit(normalised)

    assert parts.scheme == "trino", (
        "stored URI must use the trino dialect scheme, not https"
    )
    assert parts.hostname == "trino", (
        "stored URI must carry a bare hostname; the HTTP scheme is not "
        "expressed in the host field"
    )
    assert "://" not in (parts.hostname or "")


def test_bi_connection_encrypted_extra_declares_trino_auth_method() -> None:
    """``encrypted_extra`` must declare a Trino auth method.

    ``TrinoEngineSpec.update_params_from_encrypted_extra`` overrides the base
    class. It recognises exactly two keys, ``auth_method`` and ``auth_params``.
    If ``auth_method`` is absent, the method returns without touching
    ``params`` — the DBAPI receives no HTTPS transport instruction, and the
    connection fails with::

        TrinoAuthError: TLS/SSL is required for authentication

    The failure surfaces three layers away from the cause. Declaring
    ``auth_method="basic"`` causes the override to set
    ``connect_args["http_scheme"] = "https"`` and to attach a
    ``BasicAuthentication`` instance. The base-class ``connect_args`` shape is
    silently ignored on this image.
    """
    module = _load_bi_registration()

    raw = "https://superset_bi:pw@trino:8443/iceberg/consumption?verify=/ca.pem"
    payload = json.loads(module.build_encrypted_extra(raw))

    assert payload == {"auth_method": "basic", "auth_params": {}}
    # The base-class shape is silently ignored by the Trino override.
    assert "connect_args" not in payload
    assert "engine_params" not in payload


def test_bi_connection_uri_carries_json_quoted_verify() -> None:
    """The CA bundle path must be on the URI as a JSON-encoded string.

    ``TrinoEngineSpec.update_params_from_encrypted_extra`` sets ``http_scheme``
    but never reads a ``verify`` value. The Trino SQLAlchemy dialect reads
    ``verify`` from the URL query string and calls ``json.loads()`` on it, so
    the path must be JSON-quoted when stored.

    Operators may hand the input URI in any of three forms (bare path,
    JSON-quoted, percent-encoded JSON). All are normalised to the JSON-quoted
    form on the stored URI.
    """
    module = _load_bi_registration()

    for raw in (
        'https://superset_bi:pw@trino:8443/iceberg/consumption?verify="/etc/ssl/certs/trino-public.crt"',
        "https://superset_bi:pw@trino:8443/iceberg/consumption?verify=%22%2Fetc%2Fssl%2Fcerts%2Ftrino-public.crt%22",
        "https://superset_bi:pw@trino:8443/iceberg/consumption?verify=/etc/ssl/certs/trino-public.crt",
    ):
        uri = module.build_bi_connection_uri(raw)
        parts = urlsplit(uri)

        raw_verify = _query_value(parts.query, "verify")
        assert raw_verify is not None, "verify query parameter must be present"

        # The stored value must be a JSON string literal that json.loads()
        # accepts, because that is what the dialect does with it.
        decoded = unquote(raw_verify)
        assert json.loads(decoded) == "/etc/ssl/certs/trino-public.crt"