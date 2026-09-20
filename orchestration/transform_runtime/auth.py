"""Small replaceable service-auth boundary; shared-network access is insufficient."""

import hmac
import os

from fastapi import Header, HTTPException


def require_airflow_identity(authorization: str | None = Header(default=None)) -> str:
    expected = os.getenv("TRANSFORM_RUNTIME_AIRFLOW_TOKEN", "")
    supplied = authorization.removeprefix("Bearer ") if authorization else ""
    if not expected or not hmac.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="service authentication required")
    return "airflow"


def runner_headers() -> dict[str, str]:
    token = os.getenv("TRANSFORM_RUNTIME_RUNNER_TOKEN", "")
    if not token:
        raise RuntimeError("runner service credential is unavailable")
    return {"Authorization": f"Bearer {token}"}
