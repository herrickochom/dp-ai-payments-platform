"""Static contract for the one physical object-store bucket."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_canonical_object_store_layout():
    values = dict(
        line.split("=", 1)
        for line in (ROOT / ".env.example").read_text().splitlines()
        if line and not line.startswith("#")
    )
    assert values == {
        "OBJECT_STORE_BUCKET": "dp-ai-payment",
        "RAW_ROOT": "raw",
        "RAW_VERSION": "v2",
        "RAW_PREFIX": "raw/v2",
        "WAREHOUSE_PREFIX": "warehouse",
        "WAREHOUSE_URI": "s3://dp-ai-payment/warehouse",
        "NESSIE_WAREHOUSE": "payments",
        "RAW_INGEST_S3_ACCESS_KEY_ID": "",
        "RAW_INGEST_S3_SECRET_ACCESS_KEY": "",
        "CDC_QUARANTINE_S3_ACCESS_KEY_ID": "",
        "CDC_QUARANTINE_S3_SECRET_ACCESS_KEY": "",
        "PLATFORM_RAW_READ_ACCESS_KEY": "",
        "PLATFORM_RAW_READ_SECRET_KEY": "",
        "NESSIE_S3_ACCESS_KEY": "",
        "NESSIE_S3_SECRET_KEY": "",
        "TRINO_S3_ACCESS_KEY_ID": "",
        "TRINO_S3_SECRET_ACCESS_KEY": "",
        "ORDINARY_TRANSFORM_S3_ACCESS_KEY_ID": "",
        "ORDINARY_TRANSFORM_S3_SECRET_ACCESS_KEY": "",
        "RESTRICTED_TRANSFORM_S3_ACCESS_KEY_ID": "",
        "RESTRICTED_TRANSFORM_S3_SECRET_ACCESS_KEY": "",
        "ML_TRANSFORM_S3_ACCESS_KEY_ID": "",
        "ML_TRANSFORM_S3_SECRET_ACCESS_KEY": "",
    }
    assert values["RAW_PREFIX"] == f'{values["RAW_ROOT"]}/{values["RAW_VERSION"]}'
    assert values["WAREHOUSE_URI"] == (
        f's3://{values["OBJECT_STORE_BUCKET"]}/{values["WAREHOUSE_PREFIX"]}'
    )


def test_runtime_uses_canonical_warehouse_location():
    compose = (ROOT / "docker-compose.yaml").read_text()
    nessie = (ROOT / "platform/nessie/config/application.properties").read_text()
    assert "WAREHOUSE_BUCKET" not in compose + nessie
    assert 'NESSIE_CATALOG_WAREHOUSES_PAYMENTS_LOCATION: "${WAREHOUSE_URI:-s3://dp-ai-payment/warehouse}"' in compose
    assert "nessie.catalog.warehouses.payments.location=${WAREHOUSE_URI}" in nessie
    assert "nessie.catalog.default-warehouse=${NESSIE_WAREHOUSE}" in nessie
    assert 'MINIO_BUCKET: "${OBJECT_STORE_BUCKET:-dp-ai-payment}"' in compose
    assert 'RAW_PREFIX: "${RAW_PREFIX:-raw/v2}"' in compose


def test_dbt_raw_source_uses_physical_bucket_and_raw_root():
    project = (ROOT / "transform/dbt/dbt_project.yml").read_text()
    assert "WAREHOUSE_BUCKET" not in project
    assert 's3_bucket: "{{ env_var(\'OBJECT_STORE_BUCKET\') }}"' in project
    assert 's3_path: "{{ env_var(\'RAW_ROOT\') }}"' in project
    assert "WAREHOUSE_URI" not in project
    values = dict(
        line.split("=", 1)
        for line in (ROOT / ".env.example").read_text().splitlines()
        if line and not line.startswith("#")
    )
    assert (
        f's3://{values["OBJECT_STORE_BUCKET"]}/{values["RAW_ROOT"]}/'
        f'{values["RAW_VERSION"]}/**'
    ) == "s3://dp-ai-payment/raw/v2/**"


def test_runtime_does_not_define_a_second_warehouse_bucket():
    runtime_files = [ROOT / "docker-compose.yaml"]
    for directory in ("services", "orchestration", "platform", "transform"):
        runtime_files.extend(
            path for path in (ROOT / directory).rglob("*")
            if path.is_file() and path.suffix in {".py", ".yaml", ".yml", ".properties", ".sql"}
        )
    assert all("dp-ai-payment-warehouse" not in path.read_text() for path in runtime_files)
    assert "WAREHOUSE_URI=s3://dp-ai-payment/warehouse" in (ROOT / ".env.example").read_text()
