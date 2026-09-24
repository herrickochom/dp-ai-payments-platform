"""Offline regression checks for canonical runtime object-store settings."""

from __future__ import annotations

import os
from pathlib import Path

import jinja2
import pytest
import yaml

from orchestration.job_runner.transform_execution import build_transform_command

ROOT = Path(__file__).resolve().parents[2]
EXECUTION_ID = "be_" + "a" * 32


def _profile(storage: dict[str, str]) -> dict:
    values = {
        "DBT_DUCKDB_PATH": "/tmp/runtime.duckdb",
        "DBT_DATABASE": "lakehouse",
        "TRANSFORM_S3_ACCESS_KEY_ID": "key",
        "TRANSFORM_S3_SECRET_ACCESS_KEY": "secret",
        "NESSIE_ENDPOINT": "https://nessie.example.test",
        "DBT_NESSIE_BRANCH": "transform_test",
        "NESSIE_TRANSFORM_TOKEN": "token",
        "DUCKDB_S3_ENDPOINT": "object-store.example.test",
        **storage,
    }
    template = jinja2.Environment(undefined=jinja2.StrictUndefined).from_string(
        (ROOT / "transform/dbt/profiles.yml").read_text()
    )
    return yaml.safe_load(template.render(env_var=lambda name, default=None: values.get(name, default)))


@pytest.mark.parametrize("ssl", ["true", "false"])
def test_dbt_profile_uses_environment_region_and_boolean_tls(ssl):
    output = _profile({
        "OBJECT_STORE_REGION": "eu-central-2",
        "S3_ENDPOINT": "https://object-store.example.test" if ssl == "true" else "http://object-store.example.test",
        "DBT_S3_URL_STYLE": "path",
        "S3_USE_SSL": ssl,
        "S3_CA_BUNDLE": "/run/secrets/ca.pem" if ssl == "true" else "",
    })["pdm_platform"]["outputs"]["runtime"]
    assert output["settings"]["s3_region"] == "eu-central-2"
    assert output["settings"]["s3_endpoint"] == "object-store.example.test"
    assert output["settings"]["s3_use_ssl"] == ssl
    assert output["settings"]["ca_cert_file"] == ("/run/secrets/ca.pem" if ssl == "true" else "")
    assert output["config_options"]["extension_directory"] == "/opt/duckdb/extensions"
    assert output["secrets"][0]["region"] == "eu-central-2"
    assert output["secrets"][0]["endpoint"] == "object-store.example.test"
    assert output["secrets"][0]["use_ssl"] == ssl


def test_runner_propagates_validated_object_store_configuration(monkeypatch):
    monkeypatch.delenv("DP_SECURITY_MODE", raising=False)
    source = {
        "ML_S3_ACCESS_KEY_ID": "key", "ML_S3_SECRET_ACCESS_KEY": "secret",
        "NESSIE_TRANSFORM_TOKEN": "token", "NESSIE_ENDPOINT": "http://nessie:19120",
        "S3_ENDPOINT": "https://object-store.example.test", "S3_USE_SSL": "true",
        "S3_CA_BUNDLE": "/run/secrets/ca.pem", "OBJECT_STORE_REGION": "eu-central-2",
        "S3_PATH_STYLE_ACCESS": "true", "DBT_S3_URL_STYLE": "path",
        "OBJECT_STORE_BUCKET": "dp-ai-payment", "RAW_ROOT": "raw",
        "RAW_VERSION": "v2", "RAW_PREFIX": "raw/v2",
        "WAREHOUSE_PREFIX": "warehouse", "WAREHOUSE_URI": "s3://dp-ai-payment/warehouse",
    }
    child = build_transform_command("C4_ML_01", EXECUTION_ID, source).environment
    for name in ("S3_ENDPOINT", "S3_USE_SSL", "S3_CA_BUNDLE", "OBJECT_STORE_REGION",
                 "S3_PATH_STYLE_ACCESS", "RAW_ROOT", "RAW_VERSION", "RAW_PREFIX"):
        assert child[name] == source[name]
    assert child["DUCKDB_S3_ENDPOINT"] == "object-store.example.test"
    assert "ML_S3_ACCESS_KEY_ID" not in child
    assert child["TRANSFORM_S3_ACCESS_KEY_ID"] == "key"


def test_runtime_configuration_contains_no_hidden_storage_deployment_values():
    runtime_files = [
        "services/kafka-consumer-events/kafka_consumer_events.py",
        "orchestration/job_runner/transform_execution.py",
        "platform/minio/init_databases.py",
        "services/shared/materialise_token_link.py",
        "transform/dbt/profiles.yml",
        "platform/trino/catalog/iceberg.properties",
    ]
    forbidden = ("http://minio:9000", "minio:9000", "s3_region: \"eu-west-1\"",
                 "region_name='us-east-1'", "s3_use_ssl: false", "SET s3_use_ssl = false",
                 "verify=False", "dp-ai-payment-warehouse")
    for name in runtime_files:
        contents = (ROOT / name).read_text()
        for literal in forbidden:
            assert literal not in contents, (name, literal)
    catalog = (ROOT / "platform/trino/catalog/iceberg.properties").read_text()
    assert "s3.endpoint=${env:S3_ENDPOINT}" in catalog
    assert "s3.region=${env:OBJECT_STORE_REGION}" in catalog
    assert "s3.path-style-access=${env:S3_PATH_STYLE_ACCESS}" in catalog


def test_dbt_profile_is_valid_yaml_before_dbt_renders_scalar_jinja():
    profile = yaml.safe_load((ROOT / "transform/dbt/profiles.yml").read_text())

    runtime = profile["pdm_platform"]["outputs"]["runtime"]
    assert runtime["settings"]["s3_use_ssl"] == "{{ env_var('S3_USE_SSL') | lower }}"
    assert runtime["secrets"][0]["use_ssl"] == "{{ env_var('S3_USE_SSL') | lower }}"


def test_token_link_storage_settings_are_runtime_selected(monkeypatch):
    import importlib.util
    import sys

    shared = ROOT / "services/shared"
    monkeypatch.syspath_prepend(str(shared))
    spec = importlib.util.spec_from_file_location(
        "materialise_token_link_contract", shared / "materialise_token_link.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.delenv("DP_SECURITY_MODE", raising=False)
    for name, value in {
        "S3_ENDPOINT": "https://object-store.example.test:9443",
        "S3_USE_SSL": "true",
        "S3_CA_BUNDLE": "/run/secrets/ca.pem",
        "OBJECT_STORE_REGION": "eu-central-2",
        "DBT_S3_URL_STYLE": "path",
    }.items():
        monkeypatch.setenv(name, value)
    assert module.object_store_settings() == {
        "endpoint": "object-store.example.test:9443",
        "use_ssl": True,
        "ca_bundle": "/run/secrets/ca.pem",
        "region": "eu-central-2",
        "url_style": "path",
    }
    monkeypatch.setenv("S3_USE_SSL", "false")
    with pytest.raises(ValueError, match="scheme and S3_USE_SSL"):
        module.object_store_settings()
