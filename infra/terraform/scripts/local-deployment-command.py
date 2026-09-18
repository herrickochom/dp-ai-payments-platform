#!/usr/bin/env python3

import json
import shlex
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]

CONTRACT = (
    ROOT
    / "infra"
    / "terraform"
    / "targets"
    / "local"
    / "contracts"
    / "deployment_command.json"
)

INVENTORY = (
    ROOT
    / "infra"
    / "terraform"
    / "targets"
    / "local"
    / "contracts"
    / "compose_inventory.json"
)


def load(path):
    return json.loads(path.read_text())


def build_command():
    contract = load(CONTRACT)
    inventory = load(INVENTORY)

    services = inventory["default_services"]

    if not services:
        raise RuntimeError(
            "approved core service inventory is empty"
        )

    if len(services) != len(set(services)):
        raise RuntimeError(
            "duplicate service in approved inventory"
        )

    prefix = [
        contract["command"]["executable"],
        *contract["command"]["arguments_prefix"],
    ]

    command = [
        *prefix,
        *services,
    ]

    return contract, services, command


def validate(contract, services, command):
    assert contract["target"] == "local"
    assert contract["scope"] == "core"

    execution = contract["execution"]

    assert execution["enabled"] is False
    assert execution["design_only"] is True
    assert execution["explicit_service_list_required"] is True
    assert execution["implicit_default_up_prohibited"] is True
    assert execution["no_recreate_required"] is True

    assert command[:5] == [
        "docker",
        "compose",
        "up",
        "-d",
        "--no-recreate",
    ]

    assert command[5:] == services

    for flag in contract["required_flags"]:
        assert flag in command

    prohibited = set(
        contract["prohibited_tokens"]
    )

    for token in command:
        assert token not in prohibited

    acceptance = contract["acceptance"]

    assert acceptance["command_may_be_rendered"] is True
    assert acceptance["command_may_be_executed"] is False
    assert (
        acceptance[
            "separate_execution_acceptance_required"
        ]
        is True
    )


def main():
    contract, services, command = build_command()

    validate(
        contract,
        services,
        command,
    )

    print("TARGET=local")
    print("DEPLOYMENT_SCOPE=core")
    print(
        "APPROVED_SERVICE_COUNT="
        + str(len(services))
    )

    print(
        "RENDERED_COMMAND="
        + shlex.join(command)
    )

    print("COMMAND_VALIDATION=PASS")
    print("COMMAND_EXECUTION=DISABLED")
    print("MUTATING_ACTION_EXECUTED=NO")


if __name__ == "__main__":
    main()
