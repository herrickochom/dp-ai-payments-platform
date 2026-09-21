"""Static boundary checks for application object-store credentials."""

from pathlib import Path
import re

import pytest

from orchestration.job_runner.transform_execution import build_transform_command
from orchestration.transform_runtime.execution_plan import EXECUTION_BATCHES


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = (ROOT / "docker-compose.yaml").read_text()


def service(name):
    match = re.search(rf"(?ms)^  {re.escape(name)}:\n(.*?)(?=^  [\w-]+:|\Z)", COMPOSE)
    assert match is not None
    return match.group(1)


def test_raw_consumer_uses_dedicated_credentials():
    block = service("payment-consumer-events")
    consumer = (ROOT / "services/kafka-consumer-events/kafka_consumer_events.py").read_text()
    health = (ROOT / "services/kafka-consumer-events/raw_data_healthcheck.py").read_text()
    for text in (block, consumer, health):
        assert "MINIO_ROOT_" not in text
        assert "minioadmin" not in text
        assert "RAW_INGEST_S3_ACCESS_KEY_ID" in text
        assert "RAW_INGEST_S3_SECRET_ACCESS_KEY" in text
    assert ":?RAW_INGEST_S3_ACCESS_KEY_ID is required" in block
    assert ":?RAW_INGEST_S3_SECRET_ACCESS_KEY is required" in block
    for name in ("CDC_QUARANTINE_S3_ACCESS_KEY_ID", "CDC_QUARANTINE_S3_SECRET_ACCESS_KEY"):
        assert f"{name}: ${{{name}:?{name} is required}}" in block
        assert name in consumer
        assert name not in health
        assert f"-u {name}" in block.split("healthcheck:", 1)[1]
    assert 'client = get_cdc_quarantine_client()' in consumer.split("def store_cdc_quarantine_metadata(", 1)[1].split("def quarantine_permanent_cdc_failure(", 1)[0]
    assert 'client = get_minio_client()' in consumer.split("def store_cdc_event_to_s3(", 1)[1].split("def deterministic_cdc_quarantine_key(", 1)[0]
    assert 'minio_client = get_minio_client()' in consumer
    assert 'os.getenv("RAW_INGEST_S3_ACCESS_KEY_ID", "")' in consumer
    assert 'os.getenv("CDC_QUARANTINE_S3_ACCESS_KEY_ID", "")' in consumer
    assert "MINIO_ROOT_" not in consumer + health


def test_nessie_and_trino_use_service_credentials():
    nessie = service("nessie")
    trino = service("trino")
    nessie_properties = (ROOT / "platform/nessie/config/application.properties").read_text()
    trino_properties = (ROOT / "platform/trino/catalog/iceberg.properties").read_text()
    for text in (nessie, trino, nessie_properties, trino_properties):
        assert "MINIO_ROOT_" not in text
    assert "MINIO_NAME:" not in nessie and "MINIO_SECRET:" not in nessie
    assert "minio-credentials." not in nessie_properties
    for name in ("NESSIE_S3_ACCESS_KEY", "NESSIE_S3_SECRET_KEY"):
        assert name in nessie and name in nessie_properties
    for name in ("TRINO_S3_ACCESS_KEY_ID", "TRINO_S3_SECRET_ACCESS_KEY"):
        assert name in trino and f"${{env:{name}}}" in trino_properties


def test_ml_services_have_no_unused_object_store_credentials():
    for name in ("pdm-ml-features", "pdm-ml-scoring"):
        block = service(name)
        assert "MINIO_ROOT_" not in block
        assert "MINIO_ENDPOINT:" not in block
        assert "MINIO_BUCKET:" not in block


def test_restricted_utility_fails_closed_on_dedicated_credentials():
    utility = (ROOT / "services/shared/materialise_token_link.py").read_text()
    assert "MINIO_ROOT_" not in utility
    assert 'require_env("RESTRICTED_TRANSFORM_S3_ACCESS_KEY_ID")' in utility
    assert 'require_env("RESTRICTED_TRANSFORM_S3_SECRET_ACCESS_KEY")' in utility


def test_root_credentials_remain_in_admin_code_and_boundary_guard_only():
    root_services = {
        name for name in re.findall(r"(?m)^  ([\w-]+):$", COMPOSE)
        if "MINIO_ROOT_" in service(name)
    }
    assert root_services == {"minio", "minio-init-buckets", "minio-init-databases"}
    runtime = (ROOT / "orchestration/job_runner/transform_execution.py").read_text()
    assert '"MINIO_ROOT_USER"' in runtime and '"MINIO_ROOT_PASSWORD"' in runtime
    assert '"AWS_ACCESS_KEY_ID"' in runtime and '"AWS_SECRET_ACCESS_KEY"' in runtime
    for path in (ROOT / "services").rglob("*.py"):
        text = path.read_text()
        assert "MINIO_ROOT_USER" not in text and "MINIO_ROOT_PASSWORD" not in text
        assert "minioadmin" not in text
    bootstrap = (ROOT / "platform/minio/init_databases.py").read_text()
    assert "os.environ['MINIO_ROOT_USER']" in bootstrap
    assert "os.environ['MINIO_ROOT_PASSWORD']" in bootstrap
    assert "minioadmin" not in bootstrap


def test_bucket_bootstrap_never_grants_public_access():
    script = (ROOT / "platform/minio/minio-init-mc-buckets.sh").read_text().lower()
    assert "local/dp-ai-payment" in script
    assert not re.search(r"\bmc\s+(?:anonymous|policy)\s+set\b", script)
    assert not re.search(r"\b(public|anonymous|download)\b", script)


def test_runtime_has_no_legacy_warehouse_bucket_dependency():
    paths = (
        ROOT / "docker-compose.yaml",
        ROOT / "platform/nessie/config/application.properties",
        ROOT / "orchestration/job_runner/transform_execution.py",
        ROOT / "transform/dbt/dbt_project.yml",
    )
    for path in paths:
        text = path.read_text()
        assert "WAREHOUSE_BUCKET" not in text
        assert "dp-ai-payment-warehouse" not in text


@pytest.mark.parametrize("authority,source_keys", [
    ("ordinary_transform", ("ORDINARY_S3_ACCESS_KEY_ID", "ORDINARY_S3_SECRET_ACCESS_KEY")),
    ("restricted_identity_transform", ("RESTRICTED_S3_ACCESS_KEY_ID", "RESTRICTED_S3_SECRET_ACCESS_KEY")),
    ("ml_prediction_transform", ("ML_S3_ACCESS_KEY_ID", "ML_S3_SECRET_ACCESS_KEY")),
])
def test_transform_child_receives_only_selected_authority(authority, source_keys):
    batch = next(batch for batch in EXECUTION_BATCHES.values() if batch.authority == authority)
    source = {
        "ORDINARY_S3_ACCESS_KEY_ID": "ordinary-id",
        "ORDINARY_S3_SECRET_ACCESS_KEY": "ordinary-secret",
        "RESTRICTED_S3_ACCESS_KEY_ID": "restricted-id",
        "RESTRICTED_S3_SECRET_ACCESS_KEY": "restricted-secret",
        "ML_S3_ACCESS_KEY_ID": "ml-id",
        "ML_S3_SECRET_ACCESS_KEY": "ml-secret",
        "PLATFORM_RAW_READ_ACCESS_KEY": "read-id",
        "PLATFORM_RAW_READ_SECRET_KEY": "read-secret",
        "MINIO_ROOT_USER": "root-id",
        "MINIO_ROOT_PASSWORD": "root-secret",
        "AWS_ACCESS_KEY_ID": "global-id",
        "AWS_SECRET_ACCESS_KEY": "global-secret",
        "NESSIE_AUTH_TOKEN": "catalog-token",
    }
    command = build_transform_command(batch.batch_id, "be_" + "a" * 32, source)
    assert command.environment["TRANSFORM_S3_ACCESS_KEY_ID"] == source[source_keys[0]]
    assert command.environment["TRANSFORM_S3_SECRET_ACCESS_KEY"] == source[source_keys[1]]
    for name in source.keys() - {"NESSIE_AUTH_TOKEN"}:
        assert name not in command.environment


def test_platform_raw_read_stays_in_readiness_authority():
    readiness = (ROOT / "orchestration/job_runner/raw_readiness.py").read_text()
    runner = service("platform-job-runner")
    for name in ("PLATFORM_RAW_READ_ACCESS_KEY", "PLATFORM_RAW_READ_SECRET_KEY"):
        assert name in readiness
        assert name in runner
    assert "RAW_INGEST_S3_ACCESS_KEY_ID" not in readiness
    assert "CDC_QUARANTINE_S3_ACCESS_KEY_ID" not in readiness


def test_live_red_module_skips_before_missing_credentials_are_read():
    live = (ROOT / "tests/live_red_invariant_test.py").read_text()
    guard = live.index('pytest.skip("live RED invariant requires RAW_INGEST_S3 credentials"')
    access = live.index('MINIO_USER = os.environ["RAW_INGEST_S3_ACCESS_KEY_ID"]')
    secret = live.index('MINIO_PASS = os.environ["RAW_INGEST_S3_SECRET_ACCESS_KEY"]')
    assert guard < access < secret
    assert "minioadmin" not in live and "MINIO_ROOT_" not in live
