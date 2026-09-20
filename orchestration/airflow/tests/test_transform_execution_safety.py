import hashlib
from pathlib import Path

import pytest

from orchestration.job_runner.transform_execution import (
    AUTHORITY_ENV,
    FORBIDDEN_ENV,
    build_transform_command,
)
from orchestration.transform_runtime.execution_plan import EXECUTION_BATCHES


EXECUTION_ID = "be_" + ("a" * 32)


def credentials_for(authority):
    source = {
        "ORDINARY_S3_ACCESS_KEY_ID": "ordinary-key",
        "ORDINARY_S3_SECRET_ACCESS_KEY": "ordinary-secret",
        "RESTRICTED_S3_ACCESS_KEY_ID": "restricted-key",
        "RESTRICTED_S3_SECRET_ACCESS_KEY": "restricted-secret",
        "ML_S3_ACCESS_KEY_ID": "ml-key",
        "ML_S3_SECRET_ACCESS_KEY": "ml-secret",
        "NESSIE_AUTH_TOKEN": "catalog-token",
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

    assert command.environment["TRANSFORM_S3_ACCESS_KEY_ID"]
    assert command.environment["TRANSFORM_S3_SECRET_ACCESS_KEY"]
    assert command.environment["NESSIE_AUTH_TOKEN"] == "catalog-token"

    for key in FORBIDDEN_ENV:
        assert key not in command.environment

    authority_source_credentials = {
        "ORDINARY_S3_ACCESS_KEY_ID",
        "ORDINARY_S3_SECRET_ACCESS_KEY",
        "RESTRICTED_S3_ACCESS_KEY_ID",
        "RESTRICTED_S3_SECRET_ACCESS_KEY",
        "ML_S3_ACCESS_KEY_ID",
        "ML_S3_SECRET_ACCESS_KEY",
    }
    for key in authority_source_credentials:
        assert key not in command.environment


def test_raw_to_bronze_is_one_process_with_exactly_48_explicit_models():
    batch = EXECUTION_BATCHES["C4_RAW_02"]

    assert len(batch.model_allowlist) == 48

    command = build_transform_command(
        "C4_RAW_02",
        EXECUTION_ID,
        credentials_for(batch.authority),
    )

    assert command.argv.count("build") == 1
    assert command.argv.count("--select") == 1

    selected = command.argv[
        command.argv.index("--select") + 1 :
    ]

    assert len(selected) == 48
    assert len(set(selected)) == 48


def test_each_execution_gets_isolated_duckdb_path():
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

    assert first.duckdb_path != second.duckdb_path
    assert first.environment["DBT_DUCKDB_PATH"] == first.duckdb_path
    assert second.environment["DBT_DUCKDB_PATH"] == second.duckdb_path


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
    profile = Path("transform/dbt/profiles.yml").read_text()

    assert "DBT_DUCKDB_PATH" in profile
    assert "TRANSFORM_S3_ACCESS_KEY_ID" in profile
    assert "TRANSFORM_S3_SECRET_ACCESS_KEY" in profile
    assert "NESSIE_AUTH_TOKEN" in profile

    assert "MINIO_ROOT_USER" not in profile
    assert "MINIO_ROOT_PASSWORD" not in profile
    assert "PLATFORM_RAW_READ_ACCESS_KEY" not in profile
    assert "PLATFORM_RAW_READ_SECRET_KEY" not in profile


def test_dbt_target_is_fixed_and_not_execution_authority():
    authorities = set()

    for batch_id, batch in EXECUTION_BATCHES.items():
        command = build_transform_command(
            batch_id,
            EXECUTION_ID,
            credentials_for(batch.authority),
        )

        target_index = command.argv.index("--target")
        assert command.argv[target_index + 1] == "runtime"

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
