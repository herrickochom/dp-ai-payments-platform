"""Physical checks for protected prerequisites, executed before dbt launch."""
from __future__ import annotations

import re
from orchestration.transform_runtime.execution_plan import TOKEN_LINK
try:
    from services.shared.materialise_token_link import TARGET, TABLE, relation_exists, validate_table, _show_create
except ImportError:
    from materialise_token_link import TARGET, TABLE, relation_exists, validate_table, _show_create


class ProtectedPrerequisiteError(RuntimeError):
    pass


def deterministic_prerequisite_failure(
    command,
    *,
    token_link_bootstrap_succeeded: bool,
) -> str:
    """Deterministic RID prerequisite failure, mapped to FAILED (not ORPHANED).

    A failed prerequisite is a deterministic, fully-understood failure: the
    protected token-link prerequisite is absent or the physical contract is
    invalid. Such a failure is not an unknown write state, so it must never be
    recorded as ORPHANED or RECONCILIATION_REQUIRED. It is recorded as FAILED.
    """
    if not token_link_bootstrap_succeeded:
        return "BOOTSTRAP_PREREQUISITE_INCOMPLETE"
    import trino
    from trino.auth import BasicAuthentication

    env = command.environment
    try:
        connection = trino.dbapi.connect(host=env["DBT_TRINO_HOST"],
            port=int(env["DBT_TRINO_PORT"]), user=env["DBT_TRINO_USER"], http_scheme="https",
            auth=BasicAuthentication(env["DBT_TRINO_USER"], env["DBT_TRINO_PASSWORD"]), verify=False)
        cursor = connection.cursor()
        try:
            if not relation_exists(cursor, TABLE):
                return "PREREQUISITE_ABSENT"
            validate_table(cursor, TARGET)
            if re.search(r"protected-bootstrap:be_[a-f0-9]{32}", _show_create(cursor, TARGET)) is None:
                return "PREREQUISITE_PROVENANCE_INVALID"
        finally:
            cursor.close(); connection.close()
    except Exception:
        return "PREREQUISITE_PHYSICAL_CONTRACT_INVALID"
    return "PREREQUISITE_INCOMPLETE"


def validate_protected_prerequisites(command, *, token_link_bootstrap_succeeded: bool) -> None:
    if TOKEN_LINK not in command.external_prerequisites:
        return
    if not token_link_bootstrap_succeeded:
        raise ProtectedPrerequisiteError("governed token-link bootstrap is incomplete")
    import trino
    from trino.auth import BasicAuthentication
    env = command.environment
    try:
        connection = trino.dbapi.connect(host=env["DBT_TRINO_HOST"],
            port=int(env["DBT_TRINO_PORT"]), user=env["DBT_TRINO_USER"], http_scheme="https",
            auth=BasicAuthentication(env["DBT_TRINO_USER"], env["DBT_TRINO_PASSWORD"]), verify=False)
        cursor = connection.cursor()
        try:
            if not relation_exists(cursor, TABLE):
                raise RuntimeError("protected token-link prerequisite is absent")
            validate_table(cursor, TARGET)
            if re.search(r"protected-bootstrap:be_[a-f0-9]{32}", _show_create(cursor, TARGET)) is None:
                raise RuntimeError("protected token-link provenance is invalid")
        finally:
            cursor.close(); connection.close()
    except ProtectedPrerequisiteError:
        raise
    except Exception as exc:
        raise ProtectedPrerequisiteError("protected token-link physical contract is invalid") from exc
