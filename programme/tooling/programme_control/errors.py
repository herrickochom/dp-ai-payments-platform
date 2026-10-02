"""Typed errors for the programme control system."""

from __future__ import annotations


class ProgrammeControlError(Exception):
    """Base class for all programme control failures."""


class RegistryError(ProgrammeControlError):
    """A register could not be read or is structurally invalid."""


class ValidationError(ProgrammeControlError):
    """One or more validation rules failed."""


class SecretLeakError(ProgrammeControlError):
    """Secret-looking content was detected in programme metadata."""


class HumanFieldViolation(ProgrammeControlError):
    """Automation attempted to write a human-controlled field."""


class WorkbookIntegrityError(ProgrammeControlError):
    """The generated workbook failed integrity validation."""
