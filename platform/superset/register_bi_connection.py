"""Register the Superset Trino (BI) database connection, fail-closed.

This replaces an inline shell heredoc in ``docker-compose.yaml`` that carried
``trino://trino@trino:8080/iceberg/consumption`` as a fallback. Two problems
with that fallback, both observed at runtime:

* it is plaintext and anonymous. Once the coordinator requires authentication
  before authorisation (``allow-insecure-over-http=false``), such a connection
  cannot authenticate at all, and it failed with
  ``TrinoAuthError: TLS/SSL is required for authentication``.
* an absent ``TRINO_SQLALCHEMY_URI`` silently produced it instead of failing.

Every rejection here is explicit. Nothing is logged except the database id and
name, so the credential never reaches logs, test output or the container
stdout.
"""

from __future__ import annotations

import os
import sys
from urllib.parse import quote, unquote, urlsplit, urlunsplit

#: The identity the BI connection must authenticate as.
BI_TRINO_USER = "superset_bi"

#: Only the coordinator's TLS listener may carry a credential.
REQUIRED_SCHEME = "https"

#: Trino's TLS listener. Used when the URI omits an explicit port.
DEFAULT_TLS_PORT = 8443


class BiConnectionRejected(Exception):
    """The supplied BI connection is not acceptable; registration must abort."""


def build_bi_connection_uri(raw: str | None) -> str:
    """Validate and normalise the BI connection URI.

    Returns a URI that is safe to construct by anyone: the credential is
    percent-encoded here rather than trusted to already be encoded. A generated
    secret may contain ``/`` or ``+``; an unencoded ``/`` silently truncates the
    authority and produced ``Port could not be cast to integer value``.

    Raises :class:`BiConnectionRejected` for a missing value, a plaintext
    scheme, a missing or wrong identity, or a missing/disabled certificate
    verification.
    """
    if not raw:
        raise BiConnectionRejected("TRINO_SQLALCHEMY_URI is required")

    parts = urlsplit(raw)

    if parts.scheme != REQUIRED_SCHEME:
        raise BiConnectionRejected(
            f"BI connection must use {REQUIRED_SCHEME}; refusing plaintext"
        )
    if not parts.hostname:
        raise BiConnectionRejected("BI connection must name a coordinator host")

    # A credential containing an unencoded "/" truncates the authority, so the
    # remainder of the credential lands in the path. That is unrecoverable by
    # re-encoding, and at runtime it surfaced as
    # "Port could not be cast to integer value". Reject it as malformed rather
    # than silently registering a connection that cannot authenticate.
    if "@" in parts.path or ":" in parts.path:
        raise BiConnectionRejected(
            "BI connection credential is not URI-encoded; it must be "
            "percent-encoded so reserved characters cannot truncate the authority"
        )

    if parts.username != BI_TRINO_USER or not parts.password:
        raise BiConnectionRejected(
            f"BI connection must authenticate as {BI_TRINO_USER} with a credential"
        )

    query = parts.query
    if "verify=" not in query:
        raise BiConnectionRejected(
            "BI connection must verify the coordinator certificate (verify=)"
        )
    if "verify=False" in query:
        raise BiConnectionRejected(
            "BI connection must not disable certificate verification (verify=False)"
        )

    port = parts.port or DEFAULT_TLS_PORT
    # Decode then re-encode so the credential is URI-safe by construction. The
    # password itself is never logged or returned in any diagnostic.
    encoded_password = quote(unquote(parts.password), safe="")
    netloc = f"{BI_TRINO_USER}:{encoded_password}@{parts.hostname}:{port}"
    return urlunsplit((REQUIRED_SCHEME, netloc, parts.path, query, ""))


def main() -> int:
    """Create or update the BI database connection. Idempotent."""
    from superset.app import create_app

    raw_uri = os.getenv("TRINO_SQLALCHEMY_URI")
    try:
        sqlalchemy_uri = build_bi_connection_uri(raw_uri)
    except BiConnectionRejected as exc:
        # The message names the defect only; no credential is echoed.
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    app = create_app()
    with app.app_context():
        from superset import db
        from superset.models.core import Database

        database_name = os.getenv("TRINO_DATABASE_NAME", "PDM Trino")
        database = (
            db.session.query(Database)
            .filter(Database.database_name == database_name)
            .first()
        )
        if database is None:
            database = Database(
                database_name=database_name,
                sqlalchemy_uri=sqlalchemy_uri,
                # F3-2: the CREATE branch declares the least-privilege posture
                # explicitly rather than relying on a model default.
                expose_in_sqllab=False,
                allow_ctas=False,
                allow_cvas=False,
                allow_dml=False,
                allow_run_async=False,
                allow_file_upload=False,
            )
            db.session.add(database)
            action = "Created"
        else:
            database.sqlalchemy_uri = sqlalchemy_uri
            # F3-2: the UPDATE branch re-asserts the same posture, so a row that
            # was previously permissive cannot survive a redeploy.
            database.expose_in_sqllab = False
            database.allow_ctas = False
            database.allow_cvas = False
            database.allow_dml = False
            database.allow_run_async = False
            database.allow_file_upload = False
            action = "Updated"

        db.session.commit()

        # Identifier and display name only; never the URI or its credential.
        print(
            f"{action} Superset Trino connection: "
            f"id={database.id}, name={database.database_name}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())