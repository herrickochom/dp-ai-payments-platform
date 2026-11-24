"""Guarded Cloud Composer Stage 8–9 scheduler entry point.

Claim-source and PostgreSQL connection factories are
supplied by explicitly configured Python callables.

No credentials, execution identifiers or SQL connection
parameters are fabricated here.
"""

import importlib
import os

from stage89_scheduler_integration import (
    Stage89SchedulerIntegration,
)


class SchedulerConfigurationError(RuntimeError):
    pass


def load_callable(setting):
    value = os.environ.get(setting, "").strip()

    if not value or ":" not in value:
        raise SchedulerConfigurationError(
            f"{setting} requires module:function"
        )

    module_name, function_name = value.split(":", 1)

    if not module_name or not function_name:
        raise SchedulerConfigurationError(
            f"Invalid {setting}"
        )

    module = importlib.import_module(module_name)
    function = getattr(module, function_name)

    if not callable(function):
        raise SchedulerConfigurationError(
            f"{setting} is not callable"
        )

    return function


def run_scheduled_handoff():
    """One bounded scheduler tick; no external job submission."""

    if os.environ.get("PDM_STAGE89_ENABLED") != "true":
        raise SchedulerConfigurationError(
            "Stage 8–9 scheduler is not authorised for activation"
        )

    connection_factory = load_callable(
        "PDM_STAGE89_CONNECTION_FACTORY"
    )

    claim_source = load_callable(
        "PDM_STAGE89_CLAIM_SOURCE"
    )

    integration = Stage89SchedulerIntegration(
        connection_factory=connection_factory,
        claim_source=claim_source,
        max_items=10,
    )

    results = integration.run_once()

    return {
        "processed": len(results),
        "new": sum(
            result.decision == "NEW"
            for result in results
        ),
        "duplicate": sum(
            result.decision == "DUPLICATE"
            for result in results
        ),
    }
