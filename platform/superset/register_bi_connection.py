"""Register the Superset Trino (BI) database connection, fail-closed.

Three separate layers in Superset have to agree on what a working Trino
connection looks like. Each layer has a different idea, and each defect has
been observed in production with a distinct symptom.

**Layer 1 — the SQLAlchemy dialect scheme (``dbs.sqlalchemy_uri``).**
SQLAlchemy resolves the URL scheme to a dialect plugin. ``https://`` maps to
``sqlalchemy.dialects:https``, which does not exist:

    NoSuchModuleError: Can't load plugin: sqlalchemy.dialects:https

The URL must use the ``trino://`` scheme. The HTTP transport is not expressed
by the URL scheme.

**Layer 2 — the HTTP transport (``dbs.encrypted_extra``).**
Superset's ``BaseEngineSpec.update_params_from_encrypted_extra`` performs
``params.update(json.loads(encrypted_extra))`` — a shallow merge. That
contract is what the docs describe, and it is what ``TrinoEngineSpec`` used to
do. The Trino engine spec on this image **overrides** it with a method that
recognises exactly two keys, ``auth_method`` and ``auth_params``::

    auth_method = encrypted_extra.pop("auth_method", None)
    auth_params = encrypted_extra.pop("auth_params", {})
    if not auth_method:
        return
    connect_args = params.setdefault("connect_args", {})
    connect_args["http_scheme"] = "https"
    ...
    connect_args["auth"] = trino_auth(**auth_params)

If ``auth_method`` is absent, the override returns without touching
``params``. Every other key in ``encrypted_extra`` is discarded. A
``connect_args`` payload written under the base-class contract is therefore a
silent no-op on this image, and the DBAPI receives no HTTPS instruction::

    TrinoAuthError: TLS/SSL is required for authentication.
    To use HTTPS, specify 'https://' in the host URL ... or, if the host URL
    has no scheme, pass http_scheme='https'.

The failure surfaces in the dashboard builder, three layers away from the
cause. Declaring ``auth_method="basic"`` triggers the override, which sets
``http_scheme="https"`` on ``connect_args`` as a side effect.

**Layer 3 — the CA bundle (``dbs.sqlalchemy_uri`` query string).**
``TrinoEngineSpec.update_params_from_encrypted_extra`` sets ``http_scheme``
but does not read a ``verify`` value. The dialect reads ``verify`` from the
URL query string instead, and calls ``json.loads()`` on the value, so the path
must be JSON-quoted::

    ?verify=%22%2Fetc%2Fssl%2Fcerts%2Ftrino-public.crt%22

An unquoted path produces ``json.decoder.JSONDecodeError: Expecting value``.

The canonical stored shape, verified end-to-end against Trino 477 and the
dialect shipped in this image, is therefore:

* ``dbs.sqlalchemy_uri``::

    trino://superset_bi:<pw>@trino:8443/iceberg/consumption?verify=%22...%22

  Scheme ``trino``, bare host ``trino`` (not ``https://trino``), password
  percent-encoded, ``verify`` JSON-quoted.

* ``dbs.encrypted_extra``::

    {"auth_method":"basic","auth_params":{}}

  This exists to trigger the Trino engine spec's ``http_scheme="https"``
  side effect. The credential itself lives in the URI, not here.

This module reads an operator-supplied ``TRINO_SQLALCHEMY_URI``, validates it,
normalises both fields into the shape above, and verifies the connection
through Superset's own engine construction before reporting success. Every
rejection is explicit and names the defect. Nothing is logged that contains
the credential.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Final
from urllib.parse import quote, unquote, urlsplit

#: The Superset identity that owns the BI connection.
BI_TRINO_USER: Final[str] = "superset_bi"

#: SQLAlchemy dialect scheme the stored URI must carry. ``trino``, not
#: ``https``; SQLAlchemy resolves the scheme to a dialect plugin.
STORED_SCHEME: Final[str] = "trino"

#: Schemes accepted on the operator-supplied input URI. All are normalised to
#: :data:`STORED_SCHEME`. The HTTP transport is delivered separately, through
#: ``encrypted_extra``.
ACCEPTED_INPUT_SCHEMES: Final[frozenset[str]] = frozenset(
    {"https", "trino", "trino+https"}
)

#: Trino's TLS listener port, used when the input URI omits an explicit port.
DEFAULT_TLS_PORT: Final[int] = 8443

#: Query-parameter key that carries the CA bundle path.
VERIFY_KEY: Final[str] = "verify"

#: Default CA bundle path inside the Superset container.
DEFAULT_VERIFY_PATH: Final[str] = "/etc/ssl/certs/trino-public.crt"

#: Authentication method recognised by ``TrinoEngineSpec``. Its presence is
#: what causes the engine spec to set ``connect_args["http_scheme"]="https"``.
#: The credential itself is read from the URI's userinfo by
#: ``BasicAuthentication``.
TRINO_AUTH_METHOD: Final[str] = "basic"


class BiConnectionRejected(Exception):
    """The supplied BI connection is not acceptable; registration must abort."""


# ---------------------------------------------------------------------------
# URI parsing and normalisation
# ---------------------------------------------------------------------------


def _extract_query_value(query: str, key: str) -> str | None:
    """Return the raw (still-percent-encoded) value for ``key`` in ``query``.

    Trino uses simple ``key=value`` pairs; a straightforward split is
    sufficient and avoids ``parse_qs`` semantics, which silently drop blank
    values and reorder duplicate keys.
    """
    for pair in query.split("&"):
        if not pair:
            continue
        if "=" not in pair:
            if pair == key:
                return ""
            continue
        k, v = pair.split("=", 1)
        if k == key:
            return v
    return None


def _normalise_verify_path(raw_value: str | None) -> str:
    """Return a bare filesystem path for the CA bundle.

    Accepts any of::

        /etc/ssl/certs/trino-public.crt              # bare path
        "/etc/ssl/certs/trino-public.crt"            # JSON string literal
        %22%2Fetc%2Fssl%2Ftrino-public.crt%22        # percent-encoded JSON

    Returns the bare path. JSON encoding is applied when the value is written
    into the URL query; the dialect's ``json.loads()`` call expects the
    encoded form.
    """
    if raw_value is None:
        return DEFAULT_VERIFY_PATH
    decoded = unquote(raw_value).strip()
    if decoded.startswith('"') and decoded.endswith('"') and len(decoded) >= 2:
        decoded = decoded[1:-1]
    return decoded or DEFAULT_VERIFY_PATH


def build_bi_connection_uri(raw: str | None) -> str:
    """Validate and normalise an operator-supplied connection URI.

    Returns the URI to write into ``dbs.sqlalchemy_uri``::

        trino://superset_bi:<pw>@trino:8443/iceberg/consumption?verify=%22...%22

    The host is bare (``trino``, not ``https://trino``). ``http_scheme`` is
    *not* placed in the URI; it is delivered through ``encrypted_extra``, via
    the Trino engine spec's handling of ``auth_method``.
    """
    if not raw:
        raise BiConnectionRejected("TRINO_SQLALCHEMY_URI is required")

    parts = urlsplit(raw.strip())

    if parts.scheme.lower() not in ACCEPTED_INPUT_SCHEMES:
        raise BiConnectionRejected(
            "BI connection source URI must use one of "
            f"{sorted(ACCEPTED_INPUT_SCHEMES)}; got {parts.scheme!r}"
        )

    if not parts.hostname:
        raise BiConnectionRejected("BI connection must name a coordinator host")

    # If the credential contains an unencoded reserved character (typically
    # "/" or "@" in a randomly generated secret) the authority is truncated
    # and the remainder lands in the path. That state is unrecoverable here
    # because we cannot tell where the credential ended, so reject it rather
    # than register a connection that cannot authenticate.
    if "@" in (parts.path or "") or ":" in (parts.path or ""):
        raise BiConnectionRejected(
            "BI connection credential is not URI-encoded; it must be "
            "percent-encoded so reserved characters cannot truncate the authority"
        )

    if parts.username != BI_TRINO_USER:
        raise BiConnectionRejected(
            f"BI connection must authenticate as {BI_TRINO_USER!r}"
        )

    if not parts.password:
        raise BiConnectionRejected(
            "BI connection must carry a credential for the configured identity"
        )

    verify_raw = _extract_query_value(parts.query, VERIFY_KEY)
    if verify_raw is None:
        raise BiConnectionRejected(
            "BI connection must verify the coordinator certificate "
            f"({VERIFY_KEY}=)"
        )
    verify_decoded = unquote(verify_raw).strip().strip('"')
    if verify_decoded.lower() == "false":
        raise BiConnectionRejected(
            "BI connection must not disable certificate verification"
        )

    port = parts.port or DEFAULT_TLS_PORT

    # Decode-then-encode round trip: operators may hand us a credential that
    # is already percent-encoded, already raw, or a mix. This produces a
    # single canonical encoding that the SQLAlchemy URL parser will decode
    # back to the original credential bytes.
    encoded_password = quote(unquote(parts.password), safe="")
    verify_path = _normalise_verify_path(verify_raw)
    encoded_verify = quote(json.dumps(verify_path), safe="")

    host = parts.hostname
    path = parts.path or ""
    netloc = f"{BI_TRINO_USER}:{encoded_password}@{host}:{port}"

    return f"{STORED_SCHEME}://{netloc}{path}?{VERIFY_KEY}={encoded_verify}"


def build_encrypted_extra(raw: str | None) -> str:
    """Return the JSON string for ``dbs.encrypted_extra``.

    On this Superset image, ``TrinoEngineSpec`` overrides
    ``BaseEngineSpec.update_params_from_encrypted_extra``. It does **not**
    perform the base class's ``params.update(json.loads(encrypted_extra))``.
    It recognises exactly two keys, ``auth_method`` and ``auth_params``, and
    returns immediately if ``auth_method`` is absent::

        auth_method = encrypted_extra.pop("auth_method", None)
        auth_params = encrypted_extra.pop("auth_params", {})
        if not auth_method:
            return
        connect_args = params.setdefault("connect_args", {})
        connect_args["http_scheme"] = "https"
        ...
        connect_args["auth"] = trino_auth(**auth_params)

    Declaring ``auth_method="basic"`` causes the override to set
    ``connect_args["http_scheme"] = "https"`` as a side effect and to attach a
    ``BasicAuthentication`` instance. ``BasicAuthentication`` reads the user
    and password from the URI's userinfo, so ``auth_params`` is empty.

    Writing the base-class shape here — ``{"connect_args": {"http_scheme":
    "https", ...}}`` — is a silent no-op on this image. The DBAPI receives no
    HTTPS instruction and the connection fails with ``TrinoAuthError:
    TLS/SSL is required for authentication``. The failure surfaces three
    layers away from the cause, which is why the shape is pinned by a test.

    ``raw`` is accepted for symmetry with :func:`build_bi_connection_uri` and
    to allow the URI to influence the payload if a future version needs it.
    Nothing in the current contract requires it.
    """
    del raw  # see docstring: signature kept for symmetry, value not consumed
    return json.dumps(
        {
            "auth_method": TRINO_AUTH_METHOD,
            "auth_params": {},
        },
        separators=(",", ":"),
    )


# ---------------------------------------------------------------------------
# Superset integration
# ---------------------------------------------------------------------------


def _persist_connection(
    sqlalchemy_uri: str,
    encrypted_extra: str,
) -> tuple[str, int, str]:
    """Create or update the Superset database row. Returns (action, id, name)."""
    from superset.app import create_app

    database_name = os.getenv("TRINO_DATABASE_NAME", "PDM Trino")

    app = create_app()
    with app.app_context():
        from superset import db
        from superset.models.core import Database

        database = (
            db.session.query(Database)
            .filter(Database.database_name == database_name)
            .first()
        )

        if database is None:
            database = Database(
                database_name=database_name,
                sqlalchemy_uri=sqlalchemy_uri,
                encrypted_extra=encrypted_extra,
                # Least-privilege posture is asserted explicitly here rather
                # than relying on model defaults, which have changed between
                # Superset releases.
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
            database.encrypted_extra = encrypted_extra
            database.expose_in_sqllab = False
            database.allow_ctas = False
            database.allow_cvas = False
            database.allow_dml = False
            database.allow_run_async = False
            database.allow_file_upload = False
            action = "Updated"

        db.session.commit()
        return action, database.id, database.database_name


def _verify_connection(database_name: str) -> None:
    """Open a real connection through Superset's own engine construction.

    Do not hand-roll the engine here. ``Database._get_sqla_engine`` performs
    the ``encrypted_extra`` merge, calls ``adjust_engine_params``, applies the
    ``DB_CONNECTION_MUTATOR``, and passes the result to ``create_engine``.
    Rebuilding that chain by hand risks verifying a path the dashboards will
    not take. Using ``get_sqla_engine`` means the trial is exactly what a
    chart render will do.
    """
    from superset.app import create_app

    app = create_app()
    with app.app_context():
        from superset import db
        from superset.models.core import Database

        database = (
            db.session.query(Database)
            .filter(Database.database_name == database_name)
            .one()
        )

        # Use the schema the dashboards will query, so the trial exercises the
        # same database/schema path.
        with database.get_sqla_engine(schema="consumption") as engine:
            with engine.connect() as connection:
                connection.exec_driver_sql("SELECT 1").scalar()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    """Create or update the BI database connection. Idempotent.

    Exit codes::

        0  success; the connection is registered and a trial query succeeded
        2  input was rejected; nothing was written
        3  input was accepted but the trial connection failed

    Failure messages name the defect only. No credential is ever printed.
    """
    raw_uri = os.getenv("TRINO_SQLALCHEMY_URI", "")

    try:
        sqlalchemy_uri = build_bi_connection_uri(raw_uri)
        encrypted_extra = build_encrypted_extra(raw_uri)
    except BiConnectionRejected as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    try:
        action, database_id, database_name = _persist_connection(
            sqlalchemy_uri, encrypted_extra
        )
    except Exception as exc:  # noqa: BLE001 — surfaced verbatim for operators
        print(
            f"ERROR: failed to persist BI connection: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 3

    # Sanity check the shape that was stored, without echoing the credential.
    stored = urlsplit(sqlalchemy_uri)
    print(
        f"{action} Superset Trino connection: "
        f"id={database_id}, name={database_name}, "
        f"scheme={stored.scheme}, "
        f"host={stored.hostname}, "
        f"port={stored.port or DEFAULT_TLS_PORT}, "
        f"auth_method={TRINO_AUTH_METHOD}, "
        f"verify={_normalise_verify_path(_extract_query_value(stored.query, VERIFY_KEY))}"
    )

    try:
        _verify_connection(database_name)
    except Exception as exc:  # noqa: BLE001 — surfaced verbatim for operators
        print(
            f"ERROR: connection registered but trial query failed: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 3

    print("Superset Trino connection is reachable.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())