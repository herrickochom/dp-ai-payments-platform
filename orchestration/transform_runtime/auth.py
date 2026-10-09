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


def require_bootstrap_operator_identity(
    authorization: str | None = Header(default=None),
) -> str:
    """Authenticate the non-Airflow control-plane identity for bootstrap admission."""
    expected = resolve_secret("TRANSFORM_RUNTIME_BOOTSTRAP_OPERATOR_TOKEN") or ""
    supplied = authorization.removeprefix("Bearer ") if authorization else ""
    if not expected or not hmac.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="service authentication required")

    airflow = _expected_airflow_token()
    if airflow and hmac.compare_digest(supplied, airflow):
        raise HTTPException(status_code=403, detail="Airflow bootstrap invocation forbidden")
    return "protected-bootstrap-operator"


def runner_headers() -> dict[str, str]:
    try:
        token = require_secret("TRANSFORM_RUNTIME_RUNNER_TOKEN")
    except SecretUnavailable as exc:
        raise RuntimeError("runner service credential is unavailable") from exc
    return {"Authorization": f"Bearer {token}"}
