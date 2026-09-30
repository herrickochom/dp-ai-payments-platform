from pathlib import Path

import pytest

from orchestration.job_runner.transform_trino_runtime import _runtime_config


RUN_BRANCH = "transform_tr_" + "a" * 32


def runtime_environment(access="authority-access", secret="authority-secret", branch=RUN_BRANCH):
    return {
        "DBT_NESSIE_BRANCH": branch,
        "TRINO_S3_ACCESS_KEY_ID": access,
        "TRINO_S3_SECRET_ACCESS_KEY": secret,
        "NESSIE_WAREHOUSE": "s3://dp-ai-payment/warehouse",
        "S3_ENDPOINT": "http://minio:9000",
        "S3_PATH_STYLE_ACCESS": "true",
        "OBJECT_STORE_REGION": "us-east-1",
        "TRINO_KEYSTORE_PASSWORD": "keystore-secret",
        "TRINO_INTERNAL_SHARED_SECRET": "internal-secret",
    }


def test_catalog_binds_exact_claimed_branch_and_selected_authority(tmp_path):
    config = _runtime_config(str(tmp_path), runtime_environment())
    catalog = (config / "catalog/iceberg.properties").read_text()
    assert "iceberg.nessie-catalog.ref=${ENV:DBT_NESSIE_BRANCH}" in catalog
    assert "s3.aws-access-key=${ENV:TRINO_S3_ACCESS_KEY_ID}" in catalog
    assert "s3.aws-secret-key=${ENV:TRINO_S3_SECRET_ACCESS_KEY}" in catalog
    assert "main" not in catalog
    assert not {"MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"}.intersection(runtime_environment())


@pytest.mark.parametrize("branch", ["", "main", "transform_main", "transform_tr_invalid"])
def test_catalog_has_no_implicit_main_or_invalid_branch_fallback(tmp_path, branch):
    with pytest.raises(ValueError, match="configuration is unavailable|execution-scoped Nessie branch"):
        _runtime_config(str(tmp_path), runtime_environment(branch=branch))


def test_generated_runtime_configuration_contains_no_secret_values(tmp_path):
    environment = runtime_environment()
    config = _runtime_config(str(tmp_path), environment)
    rendered = "\n".join(path.read_text() for path in config.rglob("*") if path.is_file())
    assert environment["TRINO_S3_ACCESS_KEY_ID"] not in rendered
    assert environment["TRINO_S3_SECRET_ACCESS_KEY"] not in rendered
    assert environment["TRINO_KEYSTORE_PASSWORD"] not in rendered


def test_compose_has_no_preclaim_generic_credentials_or_branch():
    compose = Path("docker-compose.transform-execution.yaml").read_text()
    assert "TRANSFORM_S3_ACCESS_KEY_ID" not in compose
    assert "TRANSFORM_S3_SECRET_ACCESS_KEY" not in compose
    assert "DBT_NESSIE_BRANCH" not in compose
    assert "transform-trino:" not in compose
    worker = compose.split("  platform-transform-worker:", 1)[1]
    assert "DBT_TRINO_HOST: 127.0.0.1" in worker


def test_worker_hardening_and_no_docker_control_are_preserved():
    compose = Path("docker-compose.yaml").read_text()
    worker = compose.split("  platform-job-runner:", 1)[1].split("\n  airflow-init:", 1)[0]
    assert "read_only: true" in worker
    assert "no-new-privileges:true" in worker
    assert "cap_drop:\n    - ALL" in worker
    assert "/var/run/docker.sock" not in worker


def test_runtime_waits_until_authenticated_readiness(monkeypatch, tmp_path):
    from orchestration.job_runner import transform_trino_runtime as runtime

    class FakeProcess:
        def __init__(self):
            self.returncode = None

        def poll(self):
            return self.returncode

        def terminate(self):
            self.returncode = 0

        def wait(self, timeout=None):
            return self.returncode

        def kill(self):
            self.returncode = -9

    class FakeCursor:
        def __init__(self, attempt):
            self.attempt = attempt
            self.closed = False

        def execute(self, sql):
            assert sql == "SELECT 1"
            if self.attempt < 3:
                raise RuntimeError("authenticators were not loaded")

        def fetchall(self):
            return [[1]]

        def close(self):
            self.closed = True

    class FakeConnection:
        def __init__(self, attempt):
            self.attempt = attempt
            self.closed = False

        def cursor(self):
            return FakeCursor(self.attempt)

        def close(self):
            self.closed = True

    process = FakeProcess()
    attempts = {"count": 0}

    monkeypatch.setattr(
        runtime.subprocess,
        "Popen",
        lambda *args, **kwargs: process,
    )

    def fake_connect(**kwargs):
        attempts["count"] += 1
        return FakeConnection(attempts["count"])

    monkeypatch.setattr(runtime.trino.dbapi, "connect", fake_connect)
    monkeypatch.setattr(runtime.time, "sleep", lambda _: None)

    environment = {
        "DBT_NESSIE_BRANCH": "transform_tr_" + "a" * 32,
        "TRINO_KEYSTORE_PASSWORD": "test-keystore-password",
        "TRINO_INTERNAL_SHARED_SECRET": "test-internal-secret",
        "TRINO_S3_ACCESS_KEY_ID": "test-access",
        "TRINO_S3_SECRET_ACCESS_KEY": "test-secret",
        "NESSIE_WAREHOUSE": "s3://test/warehouse",
        "S3_ENDPOINT": "http://minio:9000",
        "S3_PATH_STYLE_ACCESS": "true",
        "OBJECT_STORE_REGION": "us-east-1",
    }

    with runtime.transform_trino_runtime(
        str(tmp_path),
        environment,
        readiness_user="dbt",
        readiness_password="test-password",
        startup_timeout_seconds=5,
    ):
        assert attempts["count"] == 3

    assert process.returncode == 0


def test_runtime_readiness_uses_explicit_credentials_and_select_one(
    monkeypatch,
    tmp_path,
):
    from orchestration.job_runner import transform_trino_runtime as runtime

    class FakeProcess:
        def __init__(self):
            self.returncode = None

        def poll(self):
            return self.returncode

        def terminate(self):
            self.returncode = 0

        def wait(self, timeout=None):
            return self.returncode

        def kill(self):
            self.returncode = -9

    class FakeCursor:
        def execute(self, sql):
            captured["sql"] = sql

        def fetchall(self):
            return [[1]]

        def close(self):
            pass

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

    process = FakeProcess()
    captured = {}

    monkeypatch.setattr(
        runtime.subprocess,
        "Popen",
        lambda *args, **kwargs: process,
    )

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return FakeConnection()

    monkeypatch.setattr(runtime.trino.dbapi, "connect", fake_connect)

    environment = {
        "DBT_NESSIE_BRANCH": "transform_tr_" + "a" * 32,
        "TRINO_KEYSTORE_PASSWORD": "test-keystore-password",
        "TRINO_INTERNAL_SHARED_SECRET": "test-internal-secret",
        "TRINO_S3_ACCESS_KEY_ID": "test-access",
        "TRINO_S3_SECRET_ACCESS_KEY": "test-secret",
        "NESSIE_WAREHOUSE": "s3://test/warehouse",
        "S3_ENDPOINT": "http://minio:9000",
        "S3_PATH_STYLE_ACCESS": "true",
        "OBJECT_STORE_REGION": "us-east-1",
    }

    with runtime.transform_trino_runtime(
        str(tmp_path),
        environment,
        readiness_user="dbt",
        readiness_password="test-password",
        startup_timeout_seconds=5,
    ):
        pass

    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 18443
    assert captured["user"] == "dbt"
    assert captured["http_scheme"] == "https"
    assert captured["verify"] is False
    assert captured["sql"] == "SELECT 1"

    # dbt credentials remain outside the Trino child-process environment.
    assert "DBT_TRINO_USER" not in environment
    assert "DBT_TRINO_PASSWORD" not in environment
