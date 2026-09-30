from orchestration.job_runner.transform_execution import (
    FORBIDDEN_ENV,
    build_transform_command,
)


EXECUTION_ID = "be_" + "a" * 32
CONTROLLED_JNA_OPTION = "-Djna.tmpdir=/var/lib/platform-transform-worker/jna"


def source_environment():
    return {
        "ORDINARY_TRANSFORM_S3_ACCESS_KEY_ID": "ordinary-access",
        "ORDINARY_TRANSFORM_S3_SECRET_ACCESS_KEY": "ordinary-secret",
        "NESSIE_TRANSFORM_TOKEN": "nessie-token",
        "DBT_TRINO_PASSWORD": "dbt-password",
        "TRINO_KEYSTORE_PASSWORD": "keystore-password",
        "TRINO_INTERNAL_SHARED_SECRET": "internal-secret",
        "S3_ENDPOINT": "http://minio:9000",
        "S3_USE_SSL": "false",
        "OBJECT_STORE_REGION": "us-east-1",
        "OBJECT_STORE_BUCKET": "dp-ai-payment",
        "RAW_ROOT": "raw",
        "RAW_VERSION": "v2",
        "RAW_PREFIX": "raw/v2",
        "WAREHOUSE_PREFIX": "warehouse",
        "WAREHOUSE_URI": "s3://dp-ai-payment/warehouse",
        "NESSIE_WAREHOUSE": "s3://dp-ai-payment/warehouse",
        "S3_PATH_STYLE_ACCESS": "true",
        "DBT_S3_URL_STYLE": "path",
        "JAVA_TOOL_OPTIONS": "-Xmx4g -Dunsafe.option=true",
        "MINIO_ROOT_USER": "forbidden-root",
        "MINIO_ROOT_PASSWORD": "forbidden-password",
        "AWS_ACCESS_KEY_ID": "forbidden-access",
        "AWS_SECRET_ACCESS_KEY": "forbidden-secret",
    }


def test_private_trino_receives_only_the_controlled_jna_option(monkeypatch):
    monkeypatch.setenv("JAVA_TOOL_OPTIONS", "-Xmx8g -Dambient.option=true")

    command = build_transform_command("C4_RAW_02", EXECUTION_ID, source_environment())

    assert command.trino_environment["JAVA_TOOL_OPTIONS"] == CONTROLLED_JNA_OPTION
    assert "-Xmx4g" not in command.trino_environment["JAVA_TOOL_OPTIONS"]
    assert "-Xmx8g" not in command.trino_environment["JAVA_TOOL_OPTIONS"]
    assert "ambient.option" not in command.trino_environment["JAVA_TOOL_OPTIONS"]
    assert "JAVA_TOOL_OPTIONS" not in command.environment
    assert not FORBIDDEN_ENV.intersection(command.environment)
    assert not FORBIDDEN_ENV.intersection(command.trino_environment)
