"""Loading of controlled configuration and the register catalogue."""

from __future__ import annotations

import functools
from pathlib import Path
from typing import Any, Mapping

import yaml

from .errors import RegistryError
from .model import RegisterSpec

CONFIG_DIRNAME = "config"
PROGRAMME_CONFIG = "programme.yaml"
REGISTERS_CONFIG = "registers.yaml"
OWNERSHIP_CONFIG = "field_ownership.yaml"

# Vocabularies declared in registers.yaml use the pattern
# ``<field>_values``. Map every one of them onto a field name.
_VALUE_SUFFIX = "_values"
# Keys that are not vocabularies even though they end in ``_values``.
_NON_VOCABULARY_KEYS = frozenset({"contract_version", "schema_version"})
# ``status_values`` is the closed vocabulary of the shared ``status`` field.
_STATUS_VOCABULARY = "status_values"


def find_repository_root(start: Path | None = None) -> Path:
    """Locate the repository root by walking up to the ``.git`` marker."""
    current = Path(start or Path(__file__)).resolve()
    for candidate in [current, *current.parents]:
        if (candidate / ".git").exists():
            return candidate
    raise RegistryError(
        "Repository root not found: no .git marker found above "
        f"{Path(start or current)}"
    )


def load_yaml(path: Path) -> Any:
    if not path.is_file():
        raise RegistryError(f"Required configuration file not found: {path}")
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise RegistryError(f"Malformed YAML in {path}: {exc}") from exc


def _vocabularies(spec: Mapping[str, Any]) -> dict[str, tuple[str, ...]]:
    """Map every ``<field>_values`` key onto a closed field vocabulary."""
    vocabularies: dict[str, tuple[str, ...]] = {}
    for key, value in spec.items():
        if not key.endswith(_VALUE_SUFFIX) or key in _NON_VOCABULARY_KEYS:
            continue
        if not isinstance(value, (list, tuple)):
            continue
        field_name = "status" if key == _STATUS_VOCABULARY else key[: -len(_VALUE_SUFFIX)]
        vocabularies[field_name] = tuple(str(item) for item in value)
    return vocabularies


def load_register_specs(config_path: Path) -> tuple[dict[str, RegisterSpec], dict[str, str]]:
    """Return the register catalogue and the area-to-register mapping."""
    document = load_yaml(config_path) or {}
    raw_registers = document.get("registers")
    if not isinstance(raw_registers, Mapping):
        raise RegistryError(
            f"{config_path} must contain a 'registers' mapping"
        )

    specs: dict[str, RegisterSpec] = {}
    areas: dict[str, str] = {}
    for name, raw in raw_registers.items():
        if not isinstance(raw, Mapping):
            raise RegistryError(f"Register spec {name!r} must be a mapping")
        missing = [key for key in ("file", "label", "id_prefix") if not raw.get(key)]
        if missing:
            raise RegistryError(
                f"Register spec {name!r} is missing {', '.join(missing)}"
            )
        specs[name] = RegisterSpec(
            name=name,
            file=str(raw["file"]),
            label=str(raw["label"]),
            id_prefix=str(raw["id_prefix"]),
            required=tuple(str(item) for item in raw.get("required", ())),
            date_fields=tuple(str(item) for item in raw.get("date_fields", ())),
            description=str(raw.get("description", "")),
            wbs=bool(raw.get("wbs", False)),
            vocabularies=_vocabularies(raw),
            reference_fields={
                str(key): str(value)
                for key, value in (raw.get("reference_fields") or {}).items()
            },
            reference_list_fields={
                str(key): str(value)
                for key, value in (raw.get("reference_list_fields") or {}).items()
            },
        )
        area = str(raw["file"]).split("/")[1] if "/" in str(raw["file"]) else ""
        areas[name] = area
    return specs, areas


@functools.lru_cache(maxsize=4)
def load_ownership(path: str) -> Mapping[str, frozenset[str]]:
    """Load the field ownership policy."""
    document = load_yaml(Path(path)) or {}
    default = document.get("default", {}) or {}
    unknown_policy = str(default.get("unknown_field_policy", "HUMAN")).upper()
    return {
        "human": frozenset(str(item) for item in document.get("human_fields", ())),
        "machine": frozenset(str(item) for item in document.get("machine_fields", ())),
        "shared": frozenset(str(item) for item in document.get("shared_fields", ())),
        "unknown_policy": frozenset({unknown_policy}),
    }
