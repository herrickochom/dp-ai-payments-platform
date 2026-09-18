import hashlib
import json
from pathlib import Path
from typing import Any


EXPECTED_CONTRACT_NAME = "lakehouse_transform"
EXPECTED_MODE = "production_bounded_transform"


class ContractError(RuntimeError):
    pass


def load_contract(path: Path) -> tuple[dict[str, Any], str]:
    raw = path.read_bytes()
    payload = json.loads(raw)

    digest = hashlib.sha256(raw).hexdigest()

    if payload.get("name") != EXPECTED_CONTRACT_NAME:
        raise ContractError("unexpected transform contract name")

    if payload.get("mode") != EXPECTED_MODE:
        raise ContractError("unexpected transform contract mode")

    models = payload.get("models")

    if not isinstance(models, list) or not models:
        raise ContractError("transform model allowlist is empty")

    waves = payload.get("topological_waves")

    if not isinstance(waves, list) or not waves:
        raise ContractError("transform topology is empty")

    return payload, digest
