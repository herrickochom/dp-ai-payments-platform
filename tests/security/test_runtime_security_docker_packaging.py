"""Docker packaging contracts for the shared runtime-security module."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

DOCKERFILES = (
    ROOT / "platform/docker/dockerfiles/Dockerfile.payment-producer",
    ROOT / "platform/docker/dockerfiles/Dockerfile.payment-consumer-events",
    ROOT / "platform/docker/dockerfiles/Dockerfile.agent-api",
)

EXPECTED_COPIES = (
    "COPY services/shared/security/__init__.py "
    "/app/services/shared/security/__init__.py",
    "COPY services/shared/security/runtime_security.py "
    "/app/services/shared/security/runtime_security.py",
)


def test_containerised_security_consumers_package_shared_security():
    for dockerfile in DOCKERFILES:
        source = dockerfile.read_text()

        for expected in EXPECTED_COPIES:
            assert expected in source, (
                f"{dockerfile.name} does not package the "
                f"shared runtime-security module: {expected}"
            )


def test_shared_security_package_contains_required_sources():
    assert (
        ROOT / "services/shared/security/__init__.py"
    ).is_file()

    assert (
        ROOT / "services/shared/security/runtime_security.py"
    ).is_file()
