import hashlib
import re
from pathlib import Path

import pytest
import yaml

from orchestration.job_runner.transform_execution import (
    AUTHORITY_ENV,
    FORBIDDEN_ENV,
    build_transform_command,
)
from orchestration.transform_runtime.execution_plan import EXECUTION_BATCHES


EXECUTION_ID = "be_" + ("a" * 32)


def credentials_for(authority):
    source = {
        "ORDINARY_TRANSFORM_S3_ACCESS_KEY_ID": "ordinary-key",
        "ORDINARY_TRANSFORM_S3_SECRET_ACCESS_KEY": "ordinary-secret",
        "RESTRICTED_TRANSFORM_S3_ACCESS_KEY_ID": "restricted-key",
        "RESTRICTED_TRANSFORM_S3_SECRET_ACCESS_KEY": "restricted-secret",
        "ML_TRANSFORM_S3_ACCESS_KEY_ID": "ml-key",
        "ML_TRANSFORM_S3_SECRET_ACCESS_KEY": "ml-secret",
        "NESSIE_TRANSFORM_TOKEN": "catalog-token",
        "DBT_TRINO_PASSWORD": "test-trino-password",
        "S3_ENDPOINT": "http://minio:9000", "S3_USE_SSL": "false", "OBJECT_STORE_REGION": "us-east-1", "OBJECT_STORE_BUCKET": "dp-ai-payment", "RAW_ROOT": "raw", "RAW_VERSION": "v2", "RAW_PREFIX": "raw/v2", "WAREHOUSE_PREFIX": "warehouse", "WAREHOUSE_URI": "s3://dp-ai-payment/warehouse", "NESSIE_WAREHOUSE": "s3://dp-ai-payment/warehouse", "S3_PATH_STYLE_ACCESS": "true", "DBT_S3_URL_STYLE": "path",
        "MINIO_ROOT_USER": "must-not-cross-boundary",
        "MINIO_ROOT_PASSWORD": "must-not-cross-boundary",
        "AWS_ACCESS_KEY_ID": "must-not-cross-boundary",
        "AWS_SECRET_ACCESS_KEY": "must-not-cross-boundary",
    }
    return source


@pytest.mark.parametrize("batch_id", sorted(EXECUTION_BATCHES))
def test_command_is_server_owned_and_bounded(batch_id):
    batch = EXECUTION_BATCHES[batch_id]
    command = build_transform_command(
        batch_id,
        EXECUTION_ID,
        credentials_for(batch.authority),
    )

    if batch.ownership_unit in {"raw_to_bronze", "ml_derived_bronze"}:
        assert command.argv[:2] == (
            "/usr/local/bin/python",
            "/app/job_runner/raw_to_bronze_executor.py",
        )
        selected = command.argv[2:]
    else:
        assert command.argv[0] == "/opt/dbt/bin/dbt"
        assert command.argv[1] == "build"
        assert "--project-dir" in command.argv
        assert "--profiles-dir" in command.argv
        assert "--select" in command.argv

        select_index = command.argv.index("--select")
        selected = command.argv[select_index + 1 :]

    expected = tuple(
        model.rsplit(".", 1)[-1]
        for model in sorted(batch.model_allowlist)
    )

    assert selected == expected

    for value in selected:
        assert "+" not in value
        assert "*" not in value
        assert ":" not in value

    assert "--vars" not in command.argv
    assert "--exclude" not in command.argv
    assert "--selector" not in command.argv


@pytest.mark.parametrize("batch_id", sorted(EXECUTION_BATCHES))
def test_only_authority_credentials_cross_process_boundary(batch_id):
    batch = EXECUTION_BATCHES[batch_id]

    command = build_transform_command(
        batch_id,
        EXECUTION_ID,
        credentials_for(batch.authority),
    )

    assert command.trino_environment["TRINO_S3_ACCESS_KEY_ID"]
    assert command.trino_environment["TRINO_S3_SECRET_ACCESS_KEY"]
    assert "NESSIE_TRANSFORM_TOKEN" not in command.environment

    for key in FORBIDDEN_ENV:
        assert key not in command.environment

    authority_source_credentials = {
        "ORDINARY_TRANSFORM_S3_ACCESS_KEY_ID",
        "ORDINARY_TRANSFORM_S3_SECRET_ACCESS_KEY",
        "RESTRICTED_TRANSFORM_S3_ACCESS_KEY_ID",
        "RESTRICTED_TRANSFORM_S3_SECRET_ACCESS_KEY",
        "ML_TRANSFORM_S3_ACCESS_KEY_ID",
        "ML_TRANSFORM_S3_SECRET_ACCESS_KEY",
    }

    selected_credentials = set(AUTHORITY_ENV[batch.authority]) - {
        "NESSIE_TRANSFORM_TOKEN"
    }

    if batch.ownership_unit in {"raw_to_bronze", "ml_derived_bronze"}:
        assert authority_source_credentials.intersection(
            command.environment
        ) == selected_credentials
    else:
        assert not authority_source_credentials.intersection(
            command.environment
        )


def test_raw_to_bronze_is_one_process_with_exactly_24_explicit_models():
    batch = EXECUTION_BATCHES["C4_RAW_02"]

    assert len(batch.model_allowlist) == 24

    command = build_transform_command(
        "C4_RAW_02",
        EXECUTION_ID,
        credentials_for(batch.authority),
    )

    assert command.argv[:2] == (
        "/usr/local/bin/python",
        "/app/job_runner/raw_to_bronze_executor.py",
    )
    assert "build" not in command.argv
    assert "--select" not in command.argv

    selected = command.argv[2:]

    assert len(selected) == 24
    assert len(set(selected)) == 24


def test_each_execution_gets_isolated_duckdb_path():
    # DuckDB file-per-execution was retired with the Trino profile; the
    # per-execution isolation contract is now DBT_LOG_PATH/DBT_TARGET_PATH.
    first = build_transform_command(
        "C4_ML_01",
        "be_" + ("1" * 32),
        credentials_for("ml_prediction_transform"),
    )
    second = build_transform_command(
        "C4_ML_01",
        "be_" + ("2" * 32),
        credentials_for("ml_prediction_transform"),
    )

    assert first.environment["DBT_LOG_PATH"] != second.environment["DBT_LOG_PATH"]
    assert first.environment["DBT_TARGET_PATH"] != second.environment["DBT_TARGET_PATH"]


def test_each_execution_gets_isolated_dbt_log_path():
    first = build_transform_command(
        "C4_ML_01",
        "be_" + ("1" * 32),
        credentials_for("ml_prediction_transform"),
    )
    second = build_transform_command(
        "C4_ML_01",
        "be_" + ("2" * 32),
        credentials_for("ml_prediction_transform"),
    )

    assert first.environment["DBT_LOG_PATH"] == (
        "/var/lib/platform-job-runner/work/be_" + ("1" * 32) + "/logs"
    )
    assert second.environment["DBT_LOG_PATH"] == (
        "/var/lib/platform-job-runner/work/be_" + ("2" * 32) + "/logs"
    )
    assert first.environment["DBT_LOG_PATH"] != second.environment["DBT_LOG_PATH"]
    assert not first.environment["DBT_LOG_PATH"].startswith("/app/dbt")


def test_each_execution_gets_isolated_dbt_target_path():
    first = build_transform_command(
        "C4_ML_01",
        "be_" + ("1" * 32),
        credentials_for("ml_prediction_transform"),
    )
    second = build_transform_command(
        "C4_ML_01",
        "be_" + ("2" * 32),
        credentials_for("ml_prediction_transform"),
    )

    assert first.environment["DBT_TARGET_PATH"] == (
        "/var/lib/platform-job-runner/work/be_" + ("1" * 32) + "/target"
    )
    assert second.environment["DBT_TARGET_PATH"] == (
        "/var/lib/platform-job-runner/work/be_" + ("2" * 32) + "/target"
    )
    assert first.environment["DBT_TARGET_PATH"] != second.environment["DBT_TARGET_PATH"]
    assert not first.environment["DBT_TARGET_PATH"].startswith("/app/dbt")


def test_model_fingerprint_is_server_derived():
    batch = EXECUTION_BATCHES["C4_ORD_FOUNDATION_04"]

    command = build_transform_command(
        batch.batch_id,
        EXECUTION_ID,
        credentials_for(batch.authority),
    )

    expected = hashlib.sha256(
        "\n".join(sorted(batch.model_allowlist)).encode()
    ).hexdigest()

    assert command.model_fingerprint == expected


def test_invalid_execution_identity_is_rejected():
    with pytest.raises(ValueError, match="invalid execution identity"):
        build_transform_command(
            "C4_ML_01",
            "../../escape",
            credentials_for("ml_prediction_transform"),
        )


def test_unknown_batch_is_rejected():
    with pytest.raises(ValueError, match="batch is not allowlisted"):
        build_transform_command(
            "C4_NOT_REAL",
            EXECUTION_ID,
            credentials_for("ordinary_transform"),
        )


def test_publication_macro_never_drops_existing_table():
    macro = Path(
        "transform/dbt/macros/iceberg_table.sql"
    ).read_text().lower()

    assert "drop table" not in macro
    assert "unsafe iceberg replacement blocked" in macro


def test_profile_uses_execution_scoped_credentials():
    """Bearer credentials never enter profiles.yml.

    With the Trino profile, Trino reads its S3/Iceberg configuration from
    platform/trino/etc/catalog/nessie.properties; the dbt profile carries
    only Trino connection fields. This test keeps the "no bearer token in
    profile" assertion.
    """
    profile = Path("transform/dbt/profiles.yml").read_text()

    # No bearer credential is referenced or interpolated in the profile.
    assert "NESSIE_TRANSFORM_TOKEN" not in profile
    assert "NESSIE_AUTH_TOKEN" not in profile
    interpolated = re.findall(r"env_var\(\s*['\"]([^'\"]+)['\"]", profile)
    assert not any("TOKEN" in name or name.startswith("NESSIE") for name in interpolated)


def test_worker_image_bakes_required_extensions_into_immutable_directory():
    dockerfile = Path(
        "platform/docker/dockerfiles/Dockerfile.platform-job-runner"
    ).read_text()

    assert "extension_directory': '/opt/duckdb/extensions'" in dockerfile
    assert "c.install_extension('httpfs')" in dockerfile
    assert "c.install_extension('avro')" in dockerfile
    assert "install_extension('iceberg')" not in dockerfile


def test_worker_keeps_read_only_root_and_execution_tmpfs():
    compose = Path("docker-compose.yaml").read_text()
    runner = compose.split("  platform-job-runner:\n", 1)[1].split(
        "\n  airflow-init:", 1
    )[0]

    assert "read_only: true" in runner
    assert "/var/lib/platform-job-runner/work:uid=50001,gid=50001,mode=0700" in runner
    assert "/app/dbt:" not in runner


def test_dbt_target_is_fixed_and_not_execution_authority():
    authorities = set()

    for batch_id, batch in EXECUTION_BATCHES.items():
        command = build_transform_command(
            batch_id,
            EXECUTION_ID,
            credentials_for(batch.authority),
        )

        if batch.ownership_unit not in {"raw_to_bronze", "ml_derived_bronze"}:
            target_index = command.argv.index("--target")
            assert command.argv[target_index + 1] == "runtime"
        else:
            assert "--target" not in command.argv

        assert command.environment["TRANSFORM_AUTHORITY"] == batch.authority
        authorities.add(batch.authority)

    assert authorities == {
        "ordinary_transform",
        "restricted_identity_transform",
        "ml_prediction_transform",
    }


def test_profile_exposes_only_fixed_runtime_target():
    profile = Path("transform/dbt/profiles.yml").read_text()

    assert "target: runtime" in profile
    assert "    runtime:" in profile

    assert "ordinary_transform:" not in profile
    assert "restricted_identity_transform:" not in profile
    assert "ml_prediction_transform:" not in profile


def test_dbt_schema_test_arguments_are_flat_for_pinned_dbt_1_9_4():
    """Pinned dbt-core 1.9.4 rejects nested `arguments:` wrappers.

    The wrapper produces "Compilation Error ... takes no keyword argument
    'arguments'" during manifest load, which fails every dbt command
    (including the governed `dbt build`) before any connection is opened.
    Test arguments must stay one level under the test name, e.g.:
        - accepted_values:
            values: [...]
    """
    schema_files = sorted(Path("transform/dbt/models").rglob("*.yml"))
    assert schema_files

    test_entries = 0
    for path in schema_files:
        text = path.read_text()
        assert "arguments:" not in text

        data = yaml.safe_load(text)
        assert isinstance(data, dict), path

        stack = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                for key, value in node.items():
                    assert key != "arguments", f"{path}: nested arguments: unsupported by dbt 1.9.4"
                    if key in ("data_tests", "tests") and isinstance(value, list):
                        test_entries += len(value)
                    stack.append(value)
            elif isinstance(node, list):
                stack.extend(node)

    # Current canonical schemas contain 342 test entries; deleted duplicate
    # PLM/PMN models stay retired rather than inflating this safety baseline.
    assert test_entries >= 342
