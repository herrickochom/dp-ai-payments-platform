"""Provider-neutral secret resolution with fail-closed source selection.

A secret provider answers one question: given a secret name, what is its
value, or is it absent. Two providers exist. The environment provider reads
process environment variables and preserves existing local development
behaviour. The mounted-file provider reads one file per secret beneath an
explicitly configured directory.

Selection is declared rather than assumed. Outside production the environment
remains the default, so local development is unchanged. Under
``DP_SECURITY_MODE=production`` a deployment must declare
``DP_SECRET_SOURCE`` as either ``environment`` or ``mounted-files``; an
undeclared or unsupported source fails closed. Declaring the environment
source is accepted as a current deployment mechanism, not as the final
production architecture: the seam exists so later deployment hardening can
move to mounted files without changing callers.

The contract is deliberately small: no caching, no global secret store, no
external secret-manager client library, and no value-bearing output. Callers
request names only, never provider-specific paths or expressions.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Protocol, runtime_checkable

from services.shared.security.runtime_security import is_production_security_mode


#: Environment-variable form. This also rejects path separators, relative
#: segments, absolute paths, dots and dashes before any path is built.
SECRET_NAME_PATTERN = re.compile(r"[A-Z][A-Z0-9_]*")

SECRET_SOURCE_ENVIRONMENT = "environment"
SECRET_SOURCE_MOUNTED_FILES = "mounted-files"
ALLOWED_SECRET_SOURCES = (SECRET_SOURCE_ENVIRONMENT, SECRET_SOURCE_MOUNTED_FILES)

SECRET_SOURCE_ENV = "DP_SECRET_SOURCE"
SECRET_DIR_ENV = "DP_SECRET_DIR"


class SecretNameError(ValueError):
    """A secret name is not in the allowed environment-variable form.

    The supplied text is deliberately not echoed: an invalid name has not been
    proved to be a name, so it must never be reproduced in a message.
    """


class SecretConfigurationError(ValueError):
    """Secret source selection or secret directory configuration is invalid."""


class SecretUnavailable(RuntimeError):
    """A required secret is absent or empty. Carries a name, never a value."""


@runtime_checkable
class SecretProvider(Protocol):
    """Minimal provider contract: a name in, a value or absence out."""

    def get(self, name: str) -> str | None:
        """Return the secret value, or None when this provider cannot supply it."""


class EnvSecretProvider:
    """Read secrets from process environment variables."""

    def get(self, name: str) -> str | None:
        _require_valid_name(name)
        return os.environ.get(name)


class FileSecretProvider:
    """Read one secret file per name beneath a configured directory."""

    def __init__(self, directory: str) -> None:
        self._directory = _require_secret_directory(directory)

    def get(self, name: str) -> str | None:
        path = self._path_for(name)
        try:
            content = path.read_text(encoding="utf-8")
        except (FileNotFoundError, IsADirectoryError, NotADirectoryError):
            return None
        except OSError as exc:
            raise SecretConfigurationError(
                f"secret {name} is not readable from the configured secret directory"
            ) from exc
        # Only trailing line-ending characters are removed. Surrounding
        # whitespace is part of the value, and contents are never logged.
        return content.rstrip("\r\n")

    def _path_for(self, name: str) -> Path:
        _require_valid_name(name)
        resolved = Path(os.path.realpath(self._directory / name))
        if resolved.parent != self._directory:
            raise SecretConfigurationError(
                f"secret {name} does not resolve inside the configured secret directory"
            )
        return resolved


def validate_secret_source() -> str:
    """Return the declared secret source, failing closed when it is unclear."""
    declared = os.getenv(SECRET_SOURCE_ENV, "").strip().lower()

    if not declared:
        if is_production_security_mode():
            raise SecretConfigurationError(
                "DP_SECRET_SOURCE must be declared explicitly as one of "
                + ", ".join(ALLOWED_SECRET_SOURCES)
                + " when DP_SECURITY_MODE=production"
            )
        declared = SECRET_SOURCE_ENVIRONMENT
    elif declared not in ALLOWED_SECRET_SOURCES:
        raise SecretConfigurationError(
            "DP_SECRET_SOURCE must be one of " + ", ".join(ALLOWED_SECRET_SOURCES)
        )

    if declared == SECRET_SOURCE_MOUNTED_FILES:
        _require_secret_directory(os.getenv(SECRET_DIR_ENV, ""))

    return declared


def resolve_secret(name: str) -> str | None:
    """Resolve one secret, or None when no source can supply it.

    When a secret directory is explicitly configured, mounted files are tried
    first and the environment is used only when the file cannot supply a
    value. Every call resolves afresh: nothing is cached and no global secret
    store is kept.
    """
    _require_valid_name(name)
    validate_secret_source()

    directory = os.getenv(SECRET_DIR_ENV, "").strip()
    if directory:
        value = FileSecretProvider(directory).get(name)
        if _is_available(value):
            return value

    value = EnvSecretProvider().get(name)
    return value if _is_available(value) else None


def require_secret(name: str) -> str:
    """Return a mandatory secret, or fail closed naming it without its value."""
    value = resolve_secret(name)
    if value is None:
        raise SecretUnavailable(
            f"secret {name} is not available from the configured secret source"
        )
    return value


def _is_available(value: str | None) -> bool:
    """An absent or empty value is unavailable; no other value is altered."""
    return bool(value)


def _require_valid_name(name: str) -> str:
    if not isinstance(name, str) or SECRET_NAME_PATTERN.fullmatch(name) is None:
        raise SecretNameError(
            "secret name must match [A-Z][A-Z0-9_]*; path-like, absolute, "
            "relative and provider-specific names are not permitted"
        )
    return name


def _require_secret_directory(directory: str) -> Path:
    candidate = (directory or "").strip()
    if not candidate:
        raise SecretConfigurationError(
            "DP_SECRET_DIR is required when mounted files supply secrets"
        )
    if not os.path.isabs(candidate):
        raise SecretConfigurationError("DP_SECRET_DIR must be an absolute path")

    resolved = Path(os.path.realpath(candidate))

    if not resolved.is_dir():
        raise SecretConfigurationError("DP_SECRET_DIR must be an existing directory")

    try:
        with os.scandir(resolved) as entries:
            next(iter(entries), None)
    except OSError as exc:
        raise SecretConfigurationError("DP_SECRET_DIR must be readable") from exc

    return resolved
