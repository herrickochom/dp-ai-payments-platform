"""
Bounded read-only platform preflight.

The preflight performs only network-level availability checks.
It does not execute shell commands, mutate platform state, publish
Kafka messages, run dbt, invoke ML workloads, or access protected data.
"""

from __future__ import annotations

import socket
import urllib.request
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class PreflightTarget:
    name: str
    host: str
    port: int
    health_url: str | None = None


@dataclass(frozen=True)
class PreflightCheck:
    name: str
    host: str
    port: int
    dns_ok: bool
    tcp_ok: bool
    http_ok: bool | None


TARGETS = (
    PreflightTarget(
        name="postgres",
        host="postgres",
        port=5432,
    ),
    PreflightTarget(
        name="redis",
        host="redis",
        port=6379,
    ),
    PreflightTarget(
        name="kafka",
        host="kafka",
        port=9092,
    ),
    PreflightTarget(
        name="minio",
        host="minio",
        port=9000,
        health_url=(
            "http://minio:9000/"
            "minio/health/live"
        ),
    ),
    PreflightTarget(
        name="nessie",
        host="nessie",
        port=19120,
        health_url=(
            "http://nessie:19120/"
            "api/v2/trees"
        ),
    ),
    PreflightTarget(
        name="trino",
        host="trino",
        port=8080,
        health_url=(
            "http://trino:8080/"
            "v1/info"
        ),
    ),
)


def _dns_check(host: str) -> bool:
    try:
        socket.getaddrinfo(
            host,
            None,
            type=socket.SOCK_STREAM,
        )
        return True
    except OSError:
        return False


def _tcp_check(
    host: str,
    port: int,
    *,
    timeout: float,
) -> bool:
    try:
        with socket.create_connection(
            (host, port),
            timeout=timeout,
        ):
            return True
    except OSError:
        return False


def _http_check(
    url: str,
    *,
    timeout: float,
) -> bool:
    try:
        request = urllib.request.Request(
            url,
            method="GET",
        )
        with urllib.request.urlopen(
            request,
            timeout=timeout,
        ) as response:
            return 200 <= response.status < 400
    except Exception:
        return False


def run_preflight(
    *,
    timeout: float = 3.0,
) -> dict:
    if timeout <= 0 or timeout > 10:
        raise ValueError(
            "timeout must be > 0 and <= 10 seconds"
        )

    checks: list[PreflightCheck] = []

    for target in TARGETS:
        dns_ok = _dns_check(target.host)

        tcp_ok = (
            _tcp_check(
                target.host,
                target.port,
                timeout=timeout,
            )
            if dns_ok
            else False
        )

        http_ok = (
            _http_check(
                target.health_url,
                timeout=timeout,
            )
            if (
                tcp_ok
                and target.health_url is not None
            )
            else (
                None
                if target.health_url is None
                else False
            )
        )

        checks.append(
            PreflightCheck(
                name=target.name,
                host=target.host,
                port=target.port,
                dns_ok=dns_ok,
                tcp_ok=tcp_ok,
                http_ok=http_ok,
            )
        )

    healthy = all(
        check.dns_ok
        and check.tcp_ok
        and (
            check.http_ok is not False
        )
        for check in checks
    )

    return {
        "status": (
            "SUCCESS"
            if healthy
            else "FAILED"
        ),
        "check_count": len(checks),
        "checks": [
            asdict(check)
            for check in checks
        ],
    }
