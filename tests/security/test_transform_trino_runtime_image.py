from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = (
    ROOT
    / "platform"
    / "docker"
    / "dockerfiles"
    / "Dockerfile.platform-job-runner"
)


def test_transform_trino_keystore_owned_by_runtime_user():
    text = DOCKERFILE.read_text()

    assert (
        "COPY --chown=50001:50001 "
        "platform/trino/etc/trino-keystore.jks "
        "/opt/transform-trino-security/trino-keystore.jks"
    ) in text


def test_transform_trino_password_db_owned_by_runtime_user():
    text = DOCKERFILE.read_text()

    assert (
        "COPY --chown=50001:50001 "
        "platform/trino/etc/password.db "
        "/opt/transform-trino-security/password.db"
    ) in text


def test_job_runner_drops_root_privileges():
    text = DOCKERFILE.read_text()

    assert "USER 50001:50001" in text
