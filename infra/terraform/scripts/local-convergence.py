#!/usr/bin/env python3

import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]

CONTRACT_ROOT = (
    ROOT
    / "infra"
    / "terraform"
    / "targets"
    / "local"
    / "contracts"
)

CORE_PATH = CONTRACT_ROOT / "operational_core.json"
POLICY_PATH = CONTRACT_ROOT / "deployment_convergence.json"


def load(path):
    return json.loads(path.read_text())


def running_services():
    result = subprocess.run(
        [
            "docker",
            "compose",
            "ps",
            "--services",
            "--status",
            "running",
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    return {
        line.strip()
        for line in result.stdout.splitlines()
        if line.strip()
    }


def unhealthy_containers():
    result = subprocess.run(
        [
            "docker",
            "ps",
            "--filter",
            "health=unhealthy",
            "--format",
            "{{.Names}}",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    return {
        line.strip()
        for line in result.stdout.splitlines()
        if line.strip()
    }


def main():
    core = load(CORE_PATH)
    policy = load(POLICY_PATH)

    desired = set(core["services"])

    assert len(desired) == 7

    assert (
        policy["current_enablement"]
        ["convergence_inspection_enabled"]
        is True
    )

    assert (
        policy["current_enablement"]
        ["converged_noop_enabled"]
        is True
    )

    assert (
        policy["current_enablement"]
        ["mutation_on_drift_enabled"]
        is False
    )

    running = running_services()
    missing = sorted(desired - running)

    unhealthy = unhealthy_containers()

    print("TARGET=local")
    print("DEPLOYMENT_SCOPE=operational_core")
    print("DESIRED_SERVICE_COUNT=" + str(len(desired)))
    print(
        "DESIRED_RUNNING_COUNT="
        + str(len(desired & running))
    )

    if unhealthy:
        print(
            "UNHEALTHY_CONTAINER_COUNT="
            + str(len(unhealthy))
        )
        print("CONVERGENCE_STATUS=DRIFT")
        print("MUTATION=DENIED")
        print("REASON=unhealthy_runtime")
        return 4

    if missing:
        print(
            "MISSING_OR_STOPPED_SERVICE_COUNT="
            + str(len(missing))
        )

        for service in missing:
            print(
                "MISSING_OR_STOPPED_SERVICE="
                + service
            )

        print("CONVERGENCE_STATUS=DRIFT")
        print("MUTATION=DENIED")
        print(
            "REASON=mutation_on_drift_not_enabled"
        )
        return 4

    print("MISSING_OR_STOPPED_SERVICE_COUNT=0")
    print("UNHEALTHY_CONTAINER_COUNT=0")
    print("CONVERGENCE_STATUS=CONVERGED")
    print("DEPLOYMENT_RESULT=SUCCESS_NOOP")
    print("COMPOSE_UP_REQUIRED=NO")
    print("MUTATION_EXECUTED=NO")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
