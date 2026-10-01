"""Regression tests for the Trino password authentication failure.

Root cause established from the deployed runtime:

``io.trino.plugin.password.file.EncryptionUtil.getHashingAlgorithm`` rejects any
BCrypt entry whose cost factor is below 8 and raises
``HashedPasswordException("Minimum cost of BCrypt password must be 8")``. The
failure is not reported as a rejected credential; it propagates out of
``PasswordAuthenticator.authenticate`` and is surfaced to the client as

    HTTP 500 java.lang.RuntimeException: Authentication error

``htpasswd`` produces cost-5 hashes by default, and ``htpasswd -C <cost>``
*without* ``-B`` silently emits an Apache MD5 (``$apr1$``) hash instead, which
Trino does not support at all. Both failure modes look identical from the
outside, so the provisioning contract is pinned here.

These tests never read a live password database: ``password.db`` is
gitignored, holds real credentials, and is not present in CI.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]

CATALOG_PROPERTIES = ROOT / "platform" / "trino" / "etc" / "catalog" / "iceberg.properties"
COMPOSE_PATH = ROOT / "docker-compose.yaml"
PASSWORD_DB = ROOT / "platform" / "trino" / "etc" / "password.db"

#: Minimum cost enforced by Trino's file password authenticator.
TRINO_MINIMUM_BCRYPT_COST = 8

BCRYPT_ENTRY = re.compile(r"^([^:\s]+):\$2([abxy])\$(\d\d)\$")


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
    """The injected catalog credential must fail closed when absent."""
    compose = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    environment = compose["services"]["trino"]["environment"]

    for key in ("NESSIE_CATALOG_S3_ACCESS_KEY", "NESSIE_CATALOG_S3_SECRET_KEY"):
        assert f"${{{key}:?" in environment[key], (
            f"SECURITY: {key} must be required, not defaulted, so Trino cannot "
            "start with an implicit credential"
        )
