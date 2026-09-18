#!/usr/bin/env bash

set -u

SCRIPT_DIR="$(
  cd "$(dirname "${BASH_SOURCE[0]}")" &&
  pwd
)"

MODE="${1:-inspect}"
ROOT="$(
  git rev-parse --show-toplevel 2>/dev/null
)"

PYTHON_BIN="$ROOT/.venv/bin/python"

if [ -z "${ROOT:-}" ]; then
    echo "ERROR=repository_root_not_found"
    exit 2
fi

cd "$ROOT" || exit 2

case "$MODE" in

  inspect)
    echo "TARGET=local"
    echo "MODE=inspect"
    echo "MUTATING=FALSE"

    if ! command -v terraform >/dev/null 2>&1; then
        echo "TERRAFORM=UNAVAILABLE"
        exit 1
    fi

    if ! docker compose version >/dev/null 2>&1; then
        echo "DOCKER_COMPOSE=UNAVAILABLE"
        exit 1
    fi

    echo "TERRAFORM=AVAILABLE"
    echo "DOCKER_COMPOSE=AVAILABLE"

    echo "TERRAFORM_VERSION=$(
      terraform version -json |
      python3 -c \
        'import json,sys; print(json.load(sys.stdin)["terraform_version"])'
    )"

    echo "DEFAULT_SERVICE_COUNT=$(
      docker compose config --services |
      wc -l
    )"

    echo "RUNNING_SERVICE_COUNT=$(
      docker compose ps \
        --services \
        --status running |
      wc -l
    )"

    echo "COMPOSE_CONFIG=CHECKING"

    if docker compose config --quiet; then
        echo "COMPOSE_CONFIG=PASS"
    else
        echo "COMPOSE_CONFIG=FAIL"
        exit 1
    fi

    echo "INSPECTION=PASS"
    ;;

  validate)
    echo "TARGET=local"
    echo "MODE=validate"
    echo "MUTATING=FALSE"

    terraform \
      -chdir=infra/terraform \
      fmt \
      -check \
      -recursive || exit 1

    terraform \
      -chdir=infra/terraform \
      validate \
      -no-color || exit 1

    docker compose \
      config \
      --quiet || exit 1

    echo "VALIDATION=PASS"
    ;;

  deploy)
    echo "TARGET=local"
    echo "MODE=deploy"
    echo "DEPLOYMENT_INTERFACE=CONVERGENCE_CONTROL"

    "$SCRIPT_DIR/local-preflight.sh" core
    preflight_rc=$?

    if [ "$preflight_rc" -ne 0 ]; then
        echo "DEPLOYMENT_RESULT=DENIED"
        echo "REASON=preflight_failed"
        exit 3
    fi

    "$PYTHON_BIN" - <<'PYCDC'
from services.shared.cdc.activation import current_activation_summary

summary = current_activation_summary()

assert summary["cdc_capable"] == []
assert summary["cdc_activated"] == []
assert summary["allowlist_count"] == 0
assert summary["contract_status"] == "fail_closed_not_activated"

print("CDC_RUNTIME_STATE=DORMANT_FAIL_CLOSED")
PYCDC

    cdc_rc=$?

    if [ "$cdc_rc" -ne 0 ]; then
        echo "DEPLOYMENT_RESULT=DENIED"
        echo "REASON=cdc_dormancy_check_failed"
        exit 3
    fi

    "$PYTHON_BIN" "$SCRIPT_DIR/local-convergence.py"
    convergence_rc=$?

    if [ "$convergence_rc" -eq 0 ]; then
        exit 0
    fi

    echo "DEPLOYMENT_RESULT=DENIED"
    echo "REASON=runtime_not_converged_mutation_disabled"
    exit 4
    ;;

  destroy)
    echo "TARGET=local"
    echo "MODE=destroy"
    echo "DESTROY=DENIED"
    echo "REASON=separate_explicit_destructive_control_required"
    exit 3
    ;;

  *)
    echo "ERROR=unsupported_mode"
    echo "SUPPORTED_MODES=inspect,validate,deploy,destroy"
    exit 2
    ;;
esac
