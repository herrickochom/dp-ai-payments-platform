"""Server-owned subject-to-role bindings (F1).

The verified token proves WHO a caller is.  This module decides WHAT that
subject may do, and it does so exclusively from server-owned configuration.

Design rules, all load-bearing:

* the binding is read from a server-owned file; it is NEVER request-body
  controlled, HTTP-header controlled, or JWT-claim controlled;
* a role that is not a known platform role is a configuration error, so a
  typo or a hostile edit cannot invent authority;
* an unknown subject is NOT an error.  It is simply unprivileged: the subject
  authenticates and receives no roles, which keeps authentication and
  authorisation as genuinely separate decisions.

File shape (YAML)::

    subjects:
      <subject-id>:
        roles:
          - programme_analyst
"""

from __future__ import annotations

import logging
from pathlib import Path

import yaml

from governance import ROLE_PERMISSIONS


logger = logging.getLogger(__name__)


class RoleBindingError(ValueError):
    """Server-owned role-binding configuration is invalid.

    Carries the offending subject/role only.  Never carries file contents.
    """


def _validate_roles(subject_id: str, roles) -> tuple[str, ...]:
    if not isinstance(roles, list) or not all(
        isinstance(role, str) for role in roles
    ):
        raise RoleBindingError(
            f"roles for subject {subject_id!r} must be a list of strings"
        )

    unknown = [
        role for role in roles if role not in ROLE_PERMISSIONS
    ]

    if unknown:
        #
        # Fail closed: an unknown role is a configuration error, never a
        # silently ignored entry.
        #
        raise RoleBindingError(
            f"subject {subject_id!r} references unknown platform role(s): "
            + ", ".join(sorted(unknown))
        )

    return tuple(dict.fromkeys(roles))


def load_role_bindings(path: str | Path | None) -> dict[str, frozenset[str]]:
    """Load and validate server-owned subject-to-role bindings.

    Returns an empty mapping when no file is configured.  An empty mapping is
    safe: every authenticated subject is then simply unprivileged.
    """

    if not path or not str(path).strip():
        return {}

    target = Path(str(path).strip())

    if not target.is_file():
        raise RoleBindingError(
            "configured role-binding file does not exist"
        )

    try:
        document = yaml.safe_load(target.read_text(encoding="utf-8"))

    except yaml.YAMLError:
        raise RoleBindingError("role-binding file is not valid YAML") from None

    except OSError:
        raise RoleBindingError("role-binding file could not be read") from None

    if document is None:
        return {}

    if not isinstance(document, dict):
        raise RoleBindingError("role-binding file must contain a mapping")

    subjects = document.get("subjects")

    if subjects is None:
        raise RoleBindingError("role-binding file must define 'subjects'")

    if not isinstance(subjects, dict):
        raise RoleBindingError("'subjects' must be a mapping")

    bindings: dict[str, frozenset[str]] = {}

    for subject_id, entry in subjects.items():
        if not isinstance(subject_id, str) or not subject_id.strip():
            raise RoleBindingError("subject identifiers must be non-empty strings")

        if not isinstance(entry, dict):
            raise RoleBindingError(
                f"subject {subject_id!r} must map to a mapping with 'roles'"
            )

        bindings[subject_id] = frozenset(
            _validate_roles(subject_id, entry.get("roles", []))
        )

    logger.info(
        "role_bindings_loaded",
        extra={"subject_count": len(bindings)},
    )

    return bindings