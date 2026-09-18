from unittest.mock import patch

import pytest

from platform_preflight import (
    TARGETS,
    run_preflight,
)


def test_target_contract_is_bounded():
    assert {
        target.name
        for target in TARGETS
    } == {
        "postgres",
        "redis",
        "kafka",
        "minio",
        "nessie",
        "trino",
    }

    assert len(TARGETS) == 6


def test_successful_preflight():
    with (
        patch(
            "platform_preflight._dns_check",
            return_value=True,
        ),
        patch(
            "platform_preflight._tcp_check",
            return_value=True,
        ),
        patch(
            "platform_preflight._http_check",
            return_value=True,
        ),
    ):
        result = run_preflight()

    assert result["status"] == "SUCCESS"
    assert result["check_count"] == 6


def test_dns_failure_fails_closed():
    with (
        patch(
            "platform_preflight._dns_check",
            return_value=False,
        ),
        patch(
            "platform_preflight._tcp_check",
            return_value=True,
        ),
        patch(
            "platform_preflight._http_check",
            return_value=True,
        ),
    ):
        result = run_preflight()

    assert result["status"] == "FAILED"


def test_tcp_failure_fails_closed():
    with (
        patch(
            "platform_preflight._dns_check",
            return_value=True,
        ),
        patch(
            "platform_preflight._tcp_check",
            return_value=False,
        ),
        patch(
            "platform_preflight._http_check",
            return_value=True,
        ),
    ):
        result = run_preflight()

    assert result["status"] == "FAILED"


def test_http_failure_fails_closed():
    with (
        patch(
            "platform_preflight._dns_check",
            return_value=True,
        ),
        patch(
            "platform_preflight._tcp_check",
            return_value=True,
        ),
        patch(
            "platform_preflight._http_check",
            return_value=False,
        ),
    ):
        result = run_preflight()

    assert result["status"] == "FAILED"


@pytest.mark.parametrize(
    "timeout",
    [
        0,
        -1,
        10.1,
    ],
)
def test_timeout_is_bounded(timeout):
    with pytest.raises(ValueError):
        run_preflight(
            timeout=timeout,
        )
