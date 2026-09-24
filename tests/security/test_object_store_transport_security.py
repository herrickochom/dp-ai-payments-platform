"""Offline contract tests for object-store transport security."""

from __future__ import annotations

import pytest

from services.shared.security.runtime_security import (
    validate_object_store_security,
)


def test_development_http_without_ssl_is_allowed(monkeypatch):
    monkeypatch.delenv("DP_SECURITY_MODE", raising=False)

    endpoint, use_ssl, ca_bundle = validate_object_store_security(
        "http://minio:9000",
        use_ssl="false",
    )

    assert endpoint == "http://minio:9000"
    assert use_ssl is False
    assert ca_bundle is None


def test_development_https_with_ssl_is_allowed(monkeypatch):
    monkeypatch.delenv("DP_SECURITY_MODE", raising=False)

    endpoint, use_ssl, ca_bundle = validate_object_store_security(
        "https://minio.example.test",
        use_ssl="true",
        ca_bundle="/etc/ssl/private-ca.pem",
    )

    assert endpoint == "https://minio.example.test"
    assert use_ssl is True
    assert ca_bundle == "/etc/ssl/private-ca.pem"


@pytest.mark.parametrize(
    ("endpoint", "use_ssl"),
    [
        ("http://minio:9000", "true"),
        ("https://minio.example.test", "false"),
    ],
)
def test_endpoint_scheme_and_ssl_flag_must_agree(
    monkeypatch,
    endpoint,
    use_ssl,
):
    monkeypatch.delenv("DP_SECURITY_MODE", raising=False)

    with pytest.raises(
        ValueError,
        match="S3 endpoint scheme and S3_USE_SSL must agree",
    ):
        validate_object_store_security(
            endpoint,
            use_ssl=use_ssl,
        )


@pytest.mark.parametrize(
    "endpoint",
    [
        "",
        "minio:9000",
        "ftp://minio.example.test",
    ],
)
def test_invalid_endpoint_is_rejected(monkeypatch, endpoint):
    monkeypatch.delenv("DP_SECURITY_MODE", raising=False)

    with pytest.raises(ValueError):
        validate_object_store_security(
            endpoint,
            use_ssl="false",
        )


@pytest.mark.parametrize(
    "use_ssl",
    [
        "",
        "yes",
        "1",
        "enabled",
    ],
)
def test_security_boolean_is_strict(monkeypatch, use_ssl):
    monkeypatch.delenv("DP_SECURITY_MODE", raising=False)

    with pytest.raises(
        ValueError,
        match="S3_USE_SSL must be 'true' or 'false'",
    ):
        validate_object_store_security(
            "http://minio:9000",
            use_ssl=use_ssl,
        )


def test_production_http_is_rejected(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    with pytest.raises(
        ValueError,
        match="S3 must use HTTPS",
    ):
        validate_object_store_security(
            "http://minio:9000",
            use_ssl="false",
        )


def test_production_https_with_ssl_is_allowed_without_custom_ca(
    monkeypatch,
):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    endpoint, use_ssl, ca_bundle = validate_object_store_security(
        "https://s3.example.test",
        use_ssl="true",
    )

    assert endpoint == "https://s3.example.test"
    assert use_ssl is True
    assert ca_bundle is None


def test_production_https_with_custom_ca_is_allowed(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    endpoint, use_ssl, ca_bundle = validate_object_store_security(
        "https://s3.internal.example.test",
        use_ssl=True,
        ca_bundle=" /run/secrets/object-store-ca.pem ",
    )

    assert endpoint == "https://s3.internal.example.test"
    assert use_ssl is True
    assert ca_bundle == "/run/secrets/object-store-ca.pem"


def _load_payment_consumer():
    """Load the payment consumer module from its filesystem path."""
    import importlib.util
    from pathlib import Path

    path = (
        Path(__file__).resolve().parents[2]
        / "services"
        / "kafka-consumer-events"
        / "kafka_consumer_events.py"
    )

    spec = importlib.util.spec_from_file_location(
        "payment_consumer_object_store_security",
        path,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_payment_consumer_rejects_production_http_before_boto_client(
    monkeypatch,
):
    consumer = _load_payment_consumer()

    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    monkeypatch.setenv("S3_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("S3_USE_SSL", "false")
    monkeypatch.setenv("RAW_INGEST_S3_ACCESS_KEY_ID", "raw-ingest")
    monkeypatch.setenv("RAW_INGEST_S3_SECRET_ACCESS_KEY", "secret")

    calls = []

    def unexpected_boto_client(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("boto3.client must not be called")

    monkeypatch.setattr(
        consumer.boto3,
        "client",
        unexpected_boto_client,
    )

    with pytest.raises(ValueError, match="S3 must use HTTPS"):
        consumer.get_minio_client()

    assert calls == []


def test_payment_consumer_propagates_custom_ca_to_boto_client(
    monkeypatch,
):
    consumer = _load_payment_consumer()

    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    monkeypatch.setenv(
        "S3_ENDPOINT",
        "https://object-store.example.test",
    )
    monkeypatch.setenv("S3_USE_SSL", "true")
    monkeypatch.setenv("OBJECT_STORE_REGION", "us-east-1")
    monkeypatch.setenv(
        "S3_CA_BUNDLE",
        "/run/secrets/object-store-ca.pem",
    )
    monkeypatch.setenv("RAW_INGEST_S3_ACCESS_KEY_ID", "raw-ingest")
    monkeypatch.setenv("RAW_INGEST_S3_SECRET_ACCESS_KEY", "secret")

    captured = {}
    sentinel = object()

    def fake_boto_client(service_name, **kwargs):
        captured["service_name"] = service_name
        captured["kwargs"] = kwargs
        return sentinel

    monkeypatch.setattr(
        consumer.boto3,
        "client",
        fake_boto_client,
    )

    client = consumer.get_minio_client()

    assert client is sentinel
    assert captured["service_name"] == "s3"
    assert (
        captured["kwargs"]["endpoint_url"]
        == "https://object-store.example.test"
    )
    assert (
        captured["kwargs"]["verify"]
        == "/run/secrets/object-store-ca.pem"
    )


def _load_raw_data_healthcheck():
    """Load the raw-data healthcheck from its filesystem path."""
    import importlib.util
    from pathlib import Path

    path = (
        Path(__file__).resolve().parents[2]
        / "services"
        / "kafka-consumer-events"
        / "raw_data_healthcheck.py"
    )

    spec = importlib.util.spec_from_file_location(
        "raw_data_healthcheck_object_store_security",
        path,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_raw_healthcheck_rejects_production_http_before_boto_client(
    monkeypatch,
):
    healthcheck = _load_raw_data_healthcheck()

    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    monkeypatch.setenv("DP_SECRET_SOURCE", "environment")
    monkeypatch.setenv("S3_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("S3_USE_SSL", "false")
    monkeypatch.setenv("KAFKA_TOPICS", "test.topic")
    monkeypatch.setenv("RAW_PREFIX", "raw/v2")
    monkeypatch.setenv("OBJECT_STORE_BUCKET", "dp-ai-payment")
    monkeypatch.setenv("OBJECT_STORE_REGION", "us-east-1")
    monkeypatch.setenv("PLATFORM_RAW_READ_ACCESS_KEY", "raw-read")
    monkeypatch.setenv(
        "PLATFORM_RAW_READ_SECRET_KEY",
        "secret",
    )

    calls = []

    def unexpected_boto_client(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("boto3.client must not be called")

    monkeypatch.setattr(
        healthcheck.boto3,
        "client",
        unexpected_boto_client,
    )

    with pytest.raises(ValueError, match="S3 must use HTTPS"):
        healthcheck.main()

    assert calls == []


def test_raw_healthcheck_propagates_custom_ca_to_boto_client(
    monkeypatch,
):
    healthcheck = _load_raw_data_healthcheck()

    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    monkeypatch.setenv("DP_SECRET_SOURCE", "environment")
    monkeypatch.setenv(
        "S3_ENDPOINT",
        "https://object-store.example.test",
    )
    monkeypatch.setenv("S3_USE_SSL", "true")
    monkeypatch.setenv(
        "S3_CA_BUNDLE",
        "/run/secrets/object-store-ca.pem",
    )
    monkeypatch.setenv("KAFKA_TOPICS", "test.topic")
    monkeypatch.setenv("RAW_PREFIX", "raw/v2")
    monkeypatch.setenv("OBJECT_STORE_BUCKET", "dp-ai-payment")
    monkeypatch.setenv("OBJECT_STORE_REGION", "us-east-1")
    monkeypatch.setenv("PLATFORM_RAW_READ_ACCESS_KEY", "raw-read")
    monkeypatch.setenv(
        "PLATFORM_RAW_READ_SECRET_KEY",
        "secret",
    )

    captured = {}

    class FakePaginator:
        def paginate(self, **kwargs):
            captured["paginate"] = kwargs
            return [
                {
                    "Contents": [
                        {
                            "Key": (
                                "raw/v2/date=2026-09-21/"
                                "topic=test.topic/event.avro"
                            )
                        }
                    ]
                }
            ]

    class FakeClient:
        def get_paginator(self, name):
            captured["paginator_name"] = name
            return FakePaginator()

    def fake_boto_client(service_name, **kwargs):
        captured["service_name"] = service_name
        captured["kwargs"] = kwargs
        return FakeClient()

    monkeypatch.setattr(
        healthcheck.boto3,
        "client",
        fake_boto_client,
    )

    assert healthcheck.main() == 0
    assert captured["service_name"] == "s3"
    assert (
        captured["kwargs"]["endpoint_url"]
        == "https://object-store.example.test"
    )
    assert (
        captured["kwargs"]["verify"]
        == "/run/secrets/object-store-ca.pem"
    )
    assert captured["paginator_name"] == "list_objects_v2"


def _load_raw_readiness():
    """Load the job-runner raw-readiness module."""
    import importlib.util
    import sys
    from pathlib import Path

    path = (
        Path(__file__).resolve().parents[2]
        / "orchestration"
        / "job_runner"
        / "raw_readiness.py"
    )

    module_name = "raw_readiness_object_store_security"
    spec = importlib.util.spec_from_file_location(
        module_name,
        path,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None

    previous = sys.modules.get(module_name)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        if previous is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = previous

    return module


def test_raw_readiness_rejects_production_http_before_boto_client(
    monkeypatch,
):
    readiness = _load_raw_readiness()

    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    monkeypatch.setenv("S3_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("OBJECT_STORE_REGION", "us-east-1")
    monkeypatch.setenv("S3_USE_SSL", "false")
    monkeypatch.setenv(
        "PLATFORM_RAW_READ_ACCESS_KEY",
        "raw-read",
    )
    monkeypatch.setenv(
        "PLATFORM_RAW_READ_SECRET_KEY",
        "secret",
    )

    calls = []

    def unexpected_boto_client(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("boto3.client must not be called")

    monkeypatch.setattr(
        readiness.boto3,
        "client",
        unexpected_boto_client,
    )

    with pytest.raises(ValueError, match="S3 must use HTTPS"):
        readiness._client()

    assert calls == []


def test_raw_readiness_propagates_custom_ca_to_boto_client(
    monkeypatch,
):
    readiness = _load_raw_readiness()

    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    monkeypatch.setenv(
        "S3_ENDPOINT",
        "https://object-store.example.test",
    )
    monkeypatch.setenv("OBJECT_STORE_REGION", "us-east-1")
    monkeypatch.setenv("S3_USE_SSL", "true")
    monkeypatch.setenv(
        "S3_CA_BUNDLE",
        "/run/secrets/object-store-ca.pem",
    )
    monkeypatch.setenv(
        "PLATFORM_RAW_READ_ACCESS_KEY",
        "raw-read",
    )
    monkeypatch.setenv(
        "PLATFORM_RAW_READ_SECRET_KEY",
        "secret",
    )

    captured = {}
    sentinel = object()

    def fake_boto_client(service_name, **kwargs):
        captured["service_name"] = service_name
        captured["kwargs"] = kwargs
        return sentinel

    monkeypatch.setattr(
        readiness.boto3,
        "client",
        fake_boto_client,
    )

    client = readiness._client()

    assert client is sentinel
    assert captured["service_name"] == "s3"
    assert (
        captured["kwargs"]["endpoint_url"]
        == "https://object-store.example.test"
    )
    assert (
        captured["kwargs"]["verify"]
        == "/run/secrets/object-store-ca.pem"
    )
    assert (
        captured["kwargs"]["aws_access_key_id"]
        == "raw-read"
    )
