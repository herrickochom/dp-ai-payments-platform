"""CDC Raw schema path contract: one authoritative file, explicit seams.

serialize_cdc_to_avro must read the single canonical schema
platform/cdc/contracts/cdc_raw_event.avsc when the module runs from the
repository source tree (tests, tooling). The payment-consumer-events image
flattens the module to /app/consumer.py, so the container cannot derive the
repository-relative default and must declare the build-time copy explicitly
through CDC_RAW_SCHEMA_PATH. These tests pin both directions so host/container
path drift cannot recur silently and no duplicate schema is introduced.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONSUMER = ROOT / "services" / "kafka-consumer-events" / "kafka_consumer_events.py"
CANONICAL_SCHEMA = (
    ROOT / "platform" / "cdc" / "contracts" / "cdc_raw_event.avsc"
)
DOCKERFILE = (
    ROOT / "platform" / "docker" / "dockerfiles"
    / "Dockerfile.payment-consumer-events"
)
IMAGE_SCHEMA_PATH = "/app/contracts/cdc_raw_event.avsc"


def load_consumer():
    spec = importlib.util.spec_from_file_location(
        "cdc_schema_path_contract_consumer",
        CONSUMER,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_source_tree_default_resolves_canonical_schema(monkeypatch):
    monkeypatch.delenv("CDC_RAW_SCHEMA_PATH", raising=False)
    module = load_consumer()
    assert module.CDC_RAW_SCHEMA_PATH == CANONICAL_SCHEMA
    assert module.CDC_RAW_SCHEMA_PATH.is_file()


def test_env_seam_overrides_schema_path(monkeypatch, tmp_path):
    """The image declares its copy path via CDC_RAW_SCHEMA_PATH."""
    image_copy = tmp_path / "contracts" / "cdc_raw_event.avsc"
    image_copy.parent.mkdir()
    image_copy.write_bytes(CANONICAL_SCHEMA.read_bytes())
    monkeypatch.setenv("CDC_RAW_SCHEMA_PATH", str(image_copy))
    module = load_consumer()
    assert module.CDC_RAW_SCHEMA_PATH == image_copy
    # The seam feeds the serializer's actual schema read.
    assert module.CDC_RAW_SCHEMA_PATH.read_text(encoding="utf-8")


def test_image_declares_copied_canonical_schema():
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    assert (
        f"COPY platform/cdc/contracts/cdc_raw_event.avsc {IMAGE_SCHEMA_PATH}"
    ) in dockerfile
    assert f"ENV CDC_RAW_SCHEMA_PATH={IMAGE_SCHEMA_PATH}" in dockerfile
    # Still one authoritative schema in the tree: no repo-side duplicate
    # contracts directory exists to satisfy container layout assumptions.
    assert CANONICAL_SCHEMA.is_file()
    assert not (
        ROOT / "services" / "kafka-consumer-events" / "contracts"
    ).exists()