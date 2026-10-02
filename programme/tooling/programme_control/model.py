"""Core data model for programme registries.

A registry record is a plain mapping with a stable string ``id``. The model
adds only behaviour that the whole system relies on: identity, ownership
classification, reference extraction and machine-field separation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator, Mapping

from .errors import RegistryError

# Ownership classifications.
HUMAN = "HUMAN"
MACHINE = "MACHINE"
SHARED = "SHARED"

# Field names used across the programme control system.
ID_FIELD = "id"
TITLE_FIELD = "title"
NAME_FIELD = "name"
STATUS_FIELD = "status"


def _as_list(value: Any) -> list[str]:
    """Normalise a scalar-or-list field into a list of non-empty strings."""
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, (list, tuple, set)):
        out: list[str] = []
        for item in value:
            out.extend(_as_list(item))
        return out
    return [str(value)]


@dataclass(frozen=True)
class Record:
    """A single registry record with a stable identifier."""

    register: str
    data: Mapping[str, Any]

    @property
    def id(self) -> str:
        value = self.data.get(ID_FIELD)
        if not isinstance(value, str) or not value.strip():
            raise RegistryError(
                f"Record in register {self.register!r} has no usable 'id'"
            )
        return value.strip()

    @property
    def title(self) -> str:
        for key in (TITLE_FIELD, NAME_FIELD):
            value = self.data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return self.id

    def get(self, key: str, default: Any = None) -> Any:
        value = self.data.get(key, default)
        return default if value is None else value

    def text(self, key: str, default: str = "") -> str:
        value = self.data.get(key)
        if value is None:
            return default
        if isinstance(value, (list, tuple)):
            return ", ".join(str(item) for item in value)
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    def list_of(self, key: str) -> list[str]:
        return _as_list(self.data.get(key))

    def as_dict(self) -> dict[str, Any]:
        return dict(self.data)


@dataclass(frozen=True)
class RegisterSpec:
    """The controlled schema of one register."""

    name: str
    file: str
    label: str
    id_prefix: str
    required: tuple[str, ...] = ()
    date_fields: tuple[str, ...] = ()
    description: str = ""
    wbs: bool = False
    vocabularies: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    reference_fields: Mapping[str, str] = field(default_factory=dict)
    reference_list_fields: Mapping[str, str] = field(default_factory=dict)

    def vocabulary(self, field_name: str) -> tuple[str, ...]:
        return self.vocabularies.get(field_name, ())

    @property
    def status_values(self) -> tuple[str, ...]:
        return self.vocabularies.get(STATUS_FIELD, ())


@dataclass
class Register:
    """A loaded register: its spec, its records and its source file."""

    name: str
    spec: RegisterSpec
    path: Any
    description: str = ""
    records: list[Record] = field(default_factory=list)

    def __iter__(self) -> Iterator[Record]:
        return iter(self.records)

    def __len__(self) -> int:
        return len(self.records)

    def by_id(self) -> dict[str, Record]:
        return {record.id: record for record in self.records}

    def get(self, record_id: str) -> Record | None:
        for record in self.records:
            if record.id == record_id:
                return record
        return None

    def ids(self) -> set[str]:
        return {record.id for record in self.records}


@dataclass
class Programme:
    """The full loaded programme: configuration plus every register."""

    root: Any
    config: Mapping[str, Any]
    register_specs: Mapping[str, RegisterSpec]
    registers: dict[str, Register]

    def __getitem__(self, name: str) -> Register:
        try:
            return self.registers[name]
        except KeyError as exc:  # pragma: no cover - defensive
            raise RegistryError(f"Unknown register {name!r}") from exc

    def get(self, name: str) -> Register | None:
        return self.registers.get(name)

    def all_records(self) -> Iterable[tuple[str, Record]]:
        for name, register in self.registers.items():
            for record in register.records:
                yield name, record

    def global_ids(self) -> dict[str, str]:
        """Map every record ID to its owning register name."""
        mapping: dict[str, str] = {}
        for name, register in self.registers.items():
            for record_id in register.ids():
                mapping[record_id] = name
        return mapping
