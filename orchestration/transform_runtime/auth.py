"""Small replaceable service-auth boundary; shared-network access is insufficient."""

import hmac

from fastapi import Header, HTTPException

from services.shared.security.secret_provider import (
    SecretUnavailable,
    require_secret,
    resolve_secret,
)


def _expected_airflow_token() -> str:
    """Resolve the expected Airflow credential; absence denies rather than raises."""
    return resolve_secret("TRANSFORM_RUNTIME_AIRFLOW_TOKEN") or ""


def require_airflow_identity(authorization: str | None = Header(default=None)) -> str:
    expected = _expected_airflow_token()
    supplied = authorization.removeprefix("Bearer ") if authorization else ""
    if not expected or not hmac.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="service authentication required")
    return "airflow"


def runner_headers() -> dict[str, str]:
    try:
        token = require_secret("TRANSFORM_RUNTIME_RUNNER_TOKEN")
    except SecretUnavailable as exc:
        raise RuntimeError("runner service credential is unavailable") from exc
    return {"Authorization": f"Bearer {token}"}
