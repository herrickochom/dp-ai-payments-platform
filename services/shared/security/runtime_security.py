"""Fail-closed production security configuration validation.

These validators are intentionally offline and side-effect free.

Local development may use HTTP and Kafka PLAINTEXT. Production callers
must select encrypted transports and provide the authentication material
required by the selected mechanism.

The validators do not modify endpoints, credentials, listeners, or
infrastructure.
"""

from __future__ import annotations

import os
from urllib.parse import urlsplit


def is_production_security_mode() -> bool:
    """Return True when production security enforcement is enabled."""
    return os.getenv("DP_SECURITY_MODE", "").strip().lower() == "production"


def _require_https(url: str, *, service: str) -> None:
    parsed = urlsplit((url or "").strip())

    if parsed.scheme.lower() != "https":
        raise ValueError(
            f"{service} must use HTTPS when DP_SECURITY_MODE=production"
        )

    if not parsed.hostname:
        raise ValueError(
            f"{service} must have a valid hostname when "
            "DP_SECURITY_MODE=production"
        )


def validate_nessie_security(
    endpoint: str,
    *,
    auth_mode: str | None = None,
    token: str | None = None,
) -> None:
    """Validate the selected Nessie transport/authentication contract."""
    if not is_production_security_mode():
        return

    _require_https(endpoint, service="Nessie")

    if (auth_mode or "").strip().lower() != "bearer":
        raise ValueError(
            "Nessie must use bearer authentication when "
            "DP_SECURITY_MODE=production"
        )

    if not (token or "").strip():
        raise ValueError(
            "Nessie bearer token is required when "
            "DP_SECURITY_MODE=production"
        )


def validate_trino_security(*, scheme: str) -> None:
    """Validate the transport selected by a Trino client."""
    if not is_production_security_mode():
        return

    if (scheme or "").strip().lower() != "https":
        raise ValueError(
            "Trino clients must use HTTPS when "
            "DP_SECURITY_MODE=production"
        )


def validate_kafka_security(
    *,
    security_protocol: str,
    ssl_ca_location: str | None = None,
    sasl_mechanism: str | None = None,
    sasl_username: str | None = None,
    sasl_password: str | None = None,
) -> None:
    """Validate the transport/authentication selected by a Kafka client."""
    protocol = (security_protocol or "").strip().upper()

    allowed = {
        "PLAINTEXT",
        "SSL",
        "SASL_PLAINTEXT",
        "SASL_SSL",
    }

    if protocol not in allowed:
        raise ValueError(
            "Kafka security protocol must be one of "
            "PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL"
        )

    if not is_production_security_mode():
        return

    if protocol in {"PLAINTEXT", "SASL_PLAINTEXT"}:
        raise ValueError(
            "Kafka clients must use an encrypted security protocol "
            "when DP_SECURITY_MODE=production"
        )

    if not (ssl_ca_location or "").strip():
        raise ValueError(
            "Kafka CA location is required for encrypted production "
            "connections"
        )

    if protocol == "SASL_SSL":
        missing = []

        if not (sasl_mechanism or "").strip():
            missing.append("SASL mechanism")
        if not (sasl_username or "").strip():
            missing.append("SASL username")
        if not (sasl_password or "").strip():
            missing.append("SASL password")

        if missing:
            raise ValueError(
                "Kafka SASL_SSL production configuration requires: "
                + ", ".join(missing)
            )


def validate_schema_registry_security(
    url: str,
    *,
    authentication_required: bool = False,
    authentication_material: str | None = None,
) -> None:
    """Validate the selected Schema Registry transport/authentication."""
    if not is_production_security_mode():
        return

    _require_https(url, service="Schema Registry")

    if authentication_required and not (
        authentication_material or ""
    ).strip():
        raise ValueError(
            "Schema Registry authentication material is required when "
            "production authentication is required"
        )


def _parse_security_bool(value: bool | str, *, name: str) -> bool:
    """Parse a security-sensitive boolean without permissive coercion."""
    if isinstance(value, bool):
        return value

    normalized = str(value).strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False

    raise ValueError(f"{name} must be 'true' or 'false'")


def validate_object_store_security(
    endpoint: str,
    *,
    use_ssl: bool | str,
    ca_bundle: str | None = None,
) -> tuple[str, bool, str | None]:
    """Validate object-store transport without performing I/O."""
    from urllib.parse import urlparse

    endpoint = (endpoint or "").strip()
    if not endpoint:
        raise ValueError("S3 endpoint is required")

    parsed = urlparse(endpoint)

    if parsed.scheme not in {"http", "https"}:
        raise ValueError("S3 endpoint must use http or https")

    if not parsed.hostname:
        raise ValueError("S3 endpoint must include a valid hostname")

    ssl_enabled = _parse_security_bool(
        use_ssl,
        name="S3_USE_SSL",
    )

    endpoint_uses_tls = parsed.scheme == "https"

    if endpoint_uses_tls != ssl_enabled:
        raise ValueError(
            "S3 endpoint scheme and S3_USE_SSL must agree"
        )

    if is_production_security_mode():
        if not endpoint_uses_tls or not ssl_enabled:
            raise ValueError(
                "S3 must use HTTPS when DP_SECURITY_MODE=production"
            )

    normalized_ca = (ca_bundle or "").strip() or None

    return endpoint, ssl_enabled, normalized_ca
