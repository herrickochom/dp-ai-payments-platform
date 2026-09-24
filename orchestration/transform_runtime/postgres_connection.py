"""Production connection contract for the durable transform ledger."""

import os
import re
from urllib.parse import parse_qs, urlsplit


def validate_database_url(url: str, *, allow_schema_admin: bool = False) -> str:
    """Return a psycopg URL or fail before opening a production connection."""
    normalized = url.replace("postgresql+psycopg://", "postgresql://", 1)
    if not normalized.startswith("postgresql://"):
        raise ValueError("PostgreSQL transform ledger URL is required")
    if os.getenv("DP_SECURITY_MODE", "").lower() != "production":
        return normalized
    parsed = urlsplit(normalized)
    query = parse_qs(parsed.query)
    if not parsed.hostname or not parsed.username or not parsed.password:
        raise ValueError("production PostgreSQL requires a dedicated application credential")
    if not allow_schema_admin and parsed.username.lower() in {"postgres", "root", "admin"}:
        raise ValueError("production PostgreSQL requires a non-administrative role")
    if query.get("sslmode") != ["verify-full"] or not query.get("sslrootcert", [""])[0]:
        raise ValueError("production PostgreSQL requires TLS verification and a CA certificate")
    try:
        connect_timeout = int(query.get("connect_timeout", [""])[0])
        options = query.get("options", [""])[0]
        match = re.fullmatch(r"-c statement_timeout=(\d+)", options)
        statement_timeout = int(match.group(1)) if match else 0
    except ValueError as exc:
        raise ValueError("production PostgreSQL requires connection and statement timeouts") from exc
    if not 1 <= connect_timeout <= 30 or not 1000 <= statement_timeout <= 300000:
        raise ValueError("production PostgreSQL timeout is outside allowed bounds")
    return normalized
