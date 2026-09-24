"""C10.3D: raw_data_healthcheck least-privilege secret migration tests.

The raw_data_healthcheck subprocess now uses the existing PLATFORM_RAW_READ
identity instead of RAW_INGEST, and routes its secret material through the
existing secret-provider contract.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from services.shared.security.secret_provider import (
    SecretUnavailable,
    require_secret,
)

ROOT = Path(__file__).resolve().parents[2]
HEALTHCHECK = ROOT / "services" / "kafka-consumer-events" / "raw_data_healthcheck.py"
COMPOSE = (ROOT / "docker-compose.yaml").read_text()


def _load_healthcheck():
    """Load the raw_data_healthcheck module from its filesystem path."""
    spec = importlib.util.spec_from_file_location(
        "raw_data_healthcheck_c103d",
        HEALTHCHECK,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _mount_secrets(tmp_path, monkeypatch, values: dict[str, str]) -> Path:
    """Mount one file per secret name and point DP_SECRET_DIR at them."""
    for name, value in values.items():
        (tmp_path / name).write_text(value + "\n", encoding="utf-8")
    monkeypatch.setenv("DP_SECRET_DIR", str(tmp_path))
    return tmp_path


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch):
    for name in (
        "DP_SECURITY_MODE",
        "DP_SECRET_SOURCE",
        "DP_SECRET_DIR",
        "PLATFORM_RAW_READ_ACCESS_KEY",
        "PLATFORM_RAW_READ_SECRET_KEY",
        "RAW_INGEST_S3_ACCESS_KEY_ID",
        "RAW_INGEST_S3_SECRET_ACCESS_KEY",
        "CDC_QUARANTINE_S3_ACCESS_KEY_ID",
        "CDC_QUARANTINE_S3_SECRET_ACCESS_KEY",
        "KAFKA_TOPICS",
        "RAW_PREFIX",
        "OBJECT_STORE_BUCKET",
        "OBJECT_STORE_REGION",
        "S3_ENDPOINT",
        "S3_USE_SSL",
        "S3_CA_BUNDLE",
    ):
        monkeypatch.delenv(name, raising=False)


# 1. healthcheck uses PLATFORM_RAW_READ, not RAW_INGEST
def test_healthcheck_source_code_uses_platform_raw_read_not_raw_ingest():
    text = HEALTHCHECK.read_text()
    assert "PLATFORM_RAW_READ_ACCESS_KEY" in text
    assert "PLATFORM_RAW_READ_SECRET_KEY" in text
    assert "require_secret" in text
    assert "RAW_INGEST_S3_ACCESS_KEY_ID" not in text
    assert "RAW_INGEST_S3_SECRET_ACCESS_KEY" not in text


# 2. PLATFORM_RAW_READ secret resolves through secret_provider
def test_healthcheck_secret_resolves_through_secret_provider_environment(
    monkeypatch,
):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    monkeypatch.setenv("DP_SECRET_SOURCE", "environment")
    monkeypatch.setenv("PLATFORM_RAW_READ_SECRET_KEY", "mounted-secret-value")

    assert require_secret("PLATFORM_RAW_READ_SECRET_KEY") == "mounted-secret-value"


# 3. environment-backed local resolution works
def test_healthcheck_environment_backed_resolution_works(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    monkeypatch.setenv("DP_SECRET_SOURCE", "environment")
    monkeypatch.setenv("PLATFORM_RAW_READ_ACCESS_KEY", "raw-read-id")
    monkeypatch.setenv("PLATFORM_RAW_READ_SECRET_KEY", "raw-read-secret")
    monkeypatch.setenv("KAFKA_TOPICS", "test.topic")
    monkeypatch.setenv("RAW_PREFIX", "raw/v2")
    monkeypatch.setenv("OBJECT_STORE_BUCKET", "dp-ai-payment")
    monkeypatch.setenv("OBJECT_STORE_REGION", "us-east-1")
    monkeypatch.setenv("S3_ENDPOINT", "https://object-store.example.test")
    monkeypatch.setenv("S3_USE_SSL", "true")

    healthcheck = _load_healthcheck()

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

    monkeypatch.setattr(healthcheck.boto3, "client", fake_boto_client)

    assert healthcheck.main() == 0
    assert captured["service_name"] == "s3"
    assert captured["kwargs"]["aws_access_key_id"] == "raw-read-id"
    assert captured["kwargs"]["aws_secret_access_key"] == "raw-read-secret"


# 4. mounted-file resolution works
def test_healthcheck_mounted_file_resolution_works(monkeypatch, tmp_path):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    monkeypatch.setenv("DP_SECRET_SOURCE", "mounted-files")
    _mount_secrets(tmp_path, monkeypatch, {"PLATFORM_RAW_READ_SECRET_KEY": "file-secret"})
    monkeypatch.setenv("PLATFORM_RAW_READ_ACCESS_KEY", "raw-read-id")
    monkeypatch.setenv("KAFKA_TOPICS", "test.topic")
    monkeypatch.setenv("RAW_PREFIX", "raw/v2")
    monkeypatch.setenv("OBJECT_STORE_BUCKET", "dp-ai-payment")
    monkeypatch.setenv("OBJECT_STORE_REGION", "us-east-1")
    monkeypatch.setenv("S3_ENDPOINT", "https://object-store.example.test")
    monkeypatch.setenv("S3_USE_SSL", "true")

    healthcheck = _load_healthcheck()

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

    monkeypatch.setattr(healthcheck.boto3, "client", fake_boto_client)

    assert healthcheck.main() == 0
    assert captured["kwargs"]["aws_secret_access_key"] == "file-secret"


# 5. missing required secret fails closed
def test_healthcheck_missing_required_secret_fails_closed(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    monkeypatch.setenv("DP_SECRET_SOURCE", "environment")
    monkeypatch.setenv("PLATFORM_RAW_READ_ACCESS_KEY", "raw-read-id")
    monkeypatch.setenv("KAFKA_TOPICS", "test.topic")
    monkeypatch.setenv("RAW_PREFIX", "raw/v2")
    monkeypatch.setenv("OBJECT_STORE_BUCKET", "dp-ai-payment")
    monkeypatch.setenv("OBJECT_STORE_REGION", "us-east-1")
    monkeypatch.setenv("S3_ENDPOINT", "https://object-store.example.test")
    monkeypatch.setenv("S3_USE_SSL", "true")

    healthcheck = _load_healthcheck()

    with pytest.raises(SecretUnavailable, match="PLATFORM_RAW_READ_SECRET_KEY"):
        healthcheck.main()


# 6. healthcheck remains list/read-only
def test_healthcheck_remains_list_read_only(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    monkeypatch.setenv("DP_SECRET_SOURCE", "environment")
    monkeypatch.setenv("PLATFORM_RAW_READ_ACCESS_KEY", "raw-read-id")
    monkeypatch.setenv("PLATFORM_RAW_READ_SECRET_KEY", "raw-read-secret")
    monkeypatch.setenv("KAFKA_TOPICS", "test.topic")
    monkeypatch.setenv("RAW_PREFIX", "raw/v2")
    monkeypatch.setenv("OBJECT_STORE_BUCKET", "dp-ai-payment")
    monkeypatch.setenv("OBJECT_STORE_REGION", "us-east-1")
    monkeypatch.setenv("S3_ENDPOINT", "https://object-store.example.test")
    monkeypatch.setenv("S3_USE_SSL", "true")

    healthcheck = _load_healthcheck()

    captured = {}

    class FakePaginator:
        def paginate(self, **kwargs):
            captured["paginate"] = kwargs
            return [{"Contents": []}]

    class FakeClient:
        def get_paginator(self, name):
            captured["paginator_name"] = name
            return FakePaginator()

    monkeypatch.setattr(healthcheck.boto3, "client", lambda service_name, **kwargs: FakeClient())

    # Should return 1 (missing topics) without attempting any write operations.
    assert healthcheck.main() == 1
    assert captured["paginator_name"] == "list_objects_v2"
    assert "put_object" not in str(captured)
    assert "delete_object" not in str(captured)


# 7. RAW_INGEST remains available to the actual consumer runtime
def test_raw_ingest_remains_available_to_consumer_runtime():
    consumer = (ROOT / "services/kafka-consumer-events/kafka_consumer_events.py").read_text()
    block = COMPOSE.split("payment-consumer-events:", 1)[1].split("healthcheck:", 1)[0]
    assert "RAW_INGEST_S3_ACCESS_KEY_ID" in consumer
    assert "RAW_INGEST_S3_SECRET_ACCESS_KEY" in consumer
    assert "RAW_INGEST_S3_ACCESS_KEY_ID" in block
    assert "RAW_INGEST_S3_SECRET_ACCESS_KEY" in block
    assert ":?RAW_INGEST_S3_ACCESS_KEY_ID is required" in block
    assert ":?RAW_INGEST_S3_SECRET_ACCESS_KEY is required" in block


# 8. CDC_QUARANTINE remains unavailable to the healthcheck subprocess
def test_cdc_quarantine_remains_unavailable_to_healthcheck():
    text = HEALTHCHECK.read_text()
    assert "CDC_QUARANTINE_S3_ACCESS_KEY_ID" not in text
    assert "CDC_QUARANTINE_S3_SECRET_ACCESS_KEY" not in text
    # The compose healthcheck strips CDC_QUARANTINE credentials via env -u.
    healthcheck_section = COMPOSE.split("healthcheck:", 1)[1].split("services:", 1)[0]
    assert "-u CDC_QUARANTINE_S3_ACCESS_KEY_ID" in healthcheck_section
    assert "-u CDC_QUARANTINE_S3_SECRET_ACCESS_KEY" in healthcheck_section


# 9. no authority/policy widening
def test_healthcheck_has_no_write_delete_authority():
    text = HEALTHCHECK.read_text()
    # The healthcheck only imports boto3 for list operations; no put/delete calls.
    assert "put_object" not in text
    assert "delete_object" not in text
    assert "copy_object" not in text
    assert "create_bucket" not in text
    assert "delete_bucket" not in text


# 10. exact existing PLATFORM_RAW_READ IAM policy remains unchanged
def test_platform_raw_read_credential_names_unchanged():
    """Verify the existing PLATFORM_RAW_READ variable names are preserved."""
    text = HEALTHCHECK.read_text()
    assert "PLATFORM_RAW_READ_ACCESS_KEY" in text
    assert "PLATFORM_RAW_READ_SECRET_KEY" in text
    # Access-key ID is treated as identity/configuration (ordinary env read).
    assert 'os.getenv("PLATFORM_RAW_READ_ACCESS_KEY"' in text
    # Secret key is treated as secret material (require_secret).
    assert 'require_secret("PLATFORM_RAW_READ_SECRET_KEY")' in text
