"""Loading of the programme registries from controlled YAML files."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import yaml

from .config import (
    load_ownership,
    load_register_specs,
    load_yaml,
    find_repository_root,
)
from .errors import RegistryError
from .model import Programme, Record, Register, RegisterSpec

REGISTRY_DIRNAME = "registry"


def load_register_file(path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Read one register file and return (envelope metadata, records)."""
    if not path.is_file():
        raise RegistryError(f"Register file not found: {path}")
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise RegistryError(f"Malformed YAML in register {path}: {exc}") from exc

    if document is None:
        document = {}
    if not isinstance(document, Mapping):
        raise RegistryError(f"Register {path} must contain a YAML mapping")

    raw_records = document.get("records", [])
    if raw_records is None:
        raw_records = []
    if not isinstance(raw_records, list):
        raise RegistryError(f"Register {path} 'records' must be a list")

    records: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_records):
        if not isinstance(raw, Mapping):
            raise RegistryError(
                f"Register {path} record #{index + 1} must be a mapping"
            )
        records.append(dict(raw))

    envelope = {
        key: value
        for key, value in document.items()
        if key != "records"
    }
    return envelope, records


def load_programme(root: Path | None = None) -> Programme:
    """Load the controlled configuration and every declared register."""
    repo_root = Path(root) if root else find_repository_root()
    config_dir = repo_root / "programme" / "config"

    config = load_yaml(config_dir / "programme.yaml") or {}
    specs, _areas = load_register_specs(config_dir / "registers.yaml")
    ownership = load_ownership(str(config_dir / "field_ownership.yaml"))

    registers: dict[str, Register] = {}
    for name, spec in specs.items():
        path = repo_root / "programme" / "registry" / spec.file
        if not path.is_file():
            raise RegistryError(
                f"Register {name!r} declared at {spec.file} is missing on disk"
            )
        envelope, raw_records = load_register_file(path)
        records = [Record(register=name, data=raw) for raw in raw_records]
        registers[name] = Register(
            name=name,
            spec=spec,
            path=path,
            description=str(envelope.get("description", spec.description)),
            records=records,
        )

    programme = Programme(
        root=repo_root,
        config=config,
        register_specs=specs,
        registers=registers,
    )
    # Ownership is attached for downstream consumers without a second load.
    setattr(programme, "ownership", ownership)
    return programme


def register_summaries(programme: Programme) -> list[dict[str, Any]]:
    """Compact per-register counts used by dashboards and reports."""
    rows: list[dict[str, Any]] = []
    for name, register in programme.registers.items():
        spec: RegisterSpec = register.spec
        rows.append(
            {
                "register": name,
                "label": spec.label,
                "area": spec.file.split("/")[1] if "/" in spec.file else "",
                "id_prefix": spec.id_prefix,
                "record_count": len(register.records),
                "path": str(spec.file),
            }
        )
    return rows
