#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

REPORT="${1:-/tmp/trino-migration-audit.txt}"

SEARCH_PATHS=(
  orchestration/job_runner
  orchestration/transform_runtime
  transform/dbt
  platform/docker/dockerfiles
  platform/trino
  tests
  orchestration/airflow/tests
)

SEARCH_FILES=(
  docker-compose.yaml
  docker-compose.transform-execution.yaml
)

PATTERN='duckdb|DBT_DUCKDB|DUCKDB_|nessie_iceberg_plugin|dbt-duckdb|DBT_NESSIE_BRANCH|DBT_TRINO|TRINO_NESSIE_PREFIX|TRANSFORM_S3_ACCESS_KEY_ID|TRANSFORM_S3_SECRET_ACCESS_KEY'

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

{
    echo "============================================================"
    echo " DBT DUCKDB -> TRINO MIGRATION AUDIT"
    echo "============================================================"
    echo
    echo "Repository: $ROOT"
    echo

    echo "============================================================"
    echo "1. CURRENT DBT PROFILE"
    echo "============================================================"
    echo

    if [[ -f transform/dbt/profiles.yml ]]; then
        sed -E \
          's/^([[:space:]]*[^#]*(password|secret|access[_-]?key|token)[^:]*:).*/\1 <REDACTED>/I' \
          transform/dbt/profiles.yml
    else
        echo "MISSING: transform/dbt/profiles.yml"
    fi

    echo
    echo "============================================================"
    echo "2. TRANSFORM TRINO CATALOG"
    echo "============================================================"
    echo

    if [[ -f platform/trino/transform-catalog/iceberg.properties ]]; then
        sed -E \
          's/^([^#]*(password|secret|access-key|token)[^=]*)=.*/\1=<REDACTED>/I' \
          platform/trino/transform-catalog/iceberg.properties
    else
        echo "MISSING: platform/trino/transform-catalog/iceberg.properties"
    fi

    echo
    echo "============================================================"
    echo "3. ALL MIGRATION REFERENCES"
    echo "============================================================"
    echo

    grep -RInE \
      "$PATTERN" \
      "${SEARCH_PATHS[@]}" \
      "${SEARCH_FILES[@]}" \
      --include='*.py' \
      --include='*.yml' \
      --include='*.yaml' \
      --include='*.txt' \
      --include='*.properties' \
      --include='Dockerfile*' \
      2>/dev/null || true

    echo
    echo "============================================================"
    echo "4. DUCKDB REFERENCES STILL PRESENT"
    echo "============================================================"
    echo

    grep -RInEi \
      'duckdb|DBT_DUCKDB|DUCKDB_|dbt-duckdb|nessie_iceberg_plugin' \
      "${SEARCH_PATHS[@]}" \
      "${SEARCH_FILES[@]}" \
      --include='*.py' \
      --include='*.yml' \
      --include='*.yaml' \
      --include='*.txt' \
      --include='*.properties' \
      --include='Dockerfile*' \
      2>/dev/null || true

    echo
    echo "============================================================"
    echo "5. TRINO MIGRATION REFERENCES"
    echo "============================================================"
    echo

    grep -RInE \
      'DBT_TRINO|TRINO_NESSIE_PREFIX|type:[[:space:]]*trino|dbt-trino|transform-trino' \
      "${SEARCH_PATHS[@]}" \
      "${SEARCH_FILES[@]}" \
      --include='*.py' \
      --include='*.yml' \
      --include='*.yaml' \
      --include='*.txt' \
      --include='*.properties' \
      --include='Dockerfile*' \
      2>/dev/null || true

    echo
    echo "============================================================"
    echo "6. NESSIE BRANCH ROUTING"
    echo "============================================================"
    echo

    grep -RInE \
      'DBT_NESSIE_BRANCH|NESSIE_WAREHOUSE|TRINO_NESSIE_PREFIX|ensure_nessie_branch|ensure_nessie_namespaces' \
      orchestration/job_runner \
      orchestration/transform_runtime \
      transform/dbt \
      platform/trino \
      docker-compose.yaml \
      docker-compose.transform-execution.yaml \
      --include='*.py' \
      --include='*.yml' \
      --include='*.yaml' \
      --include='*.properties' \
      2>/dev/null || true

    echo
    echo "============================================================"
    echo "7. DEPENDENCIES"
    echo "============================================================"
    echo

    find orchestration transform platform \
      -type f \
      \( -name 'requirements*.txt' -o -name 'pyproject.toml' \) \
      -print0 2>/dev/null |
    while IFS= read -r -d '' f; do
        matches="$(grep -nEi 'dbt-(trino|duckdb)|duckdb|trino' "$f" || true)"
        if [[ -n "$matches" ]]; then
            echo "--- $f"
            printf '%s\n' "$matches"
            echo
        fi
    done

    echo
    echo "============================================================"
    echo "8. COMPOSE SERVICES"
    echo "============================================================"
    echo

    docker compose \
      -f docker-compose.yaml \
      -f docker-compose.transform-execution.yaml \
      config --services 2>/dev/null || true

    echo
    echo "============================================================"
    echo "9. WORKING TREE"
    echo "============================================================"
    echo

    git status --short

    echo
    echo "============================================================"
    echo "10. DIFF CHECK"
    echo "============================================================"
    echo

    if git diff --check; then
        echo "PASS: git diff --check"
    else
        echo "FAIL: git diff --check"
    fi

    echo
    echo "============================================================"
    echo "11. MIGRATION GATES"
    echo "============================================================"
    echo

    if grep -Eq '^[[:space:]]*type:[[:space:]]*trino[[:space:]]*$' \
        transform/dbt/profiles.yml 2>/dev/null; then
        echo "PASS  dbt profile uses Trino"
    else
        echo "FAIL  dbt profile does not use Trino"
    fi

    if grep -Eq '^iceberg\.rest-catalog\.prefix=\$\{ENV:TRINO_NESSIE_PREFIX\}$' \
        platform/trino/transform-catalog/iceberg.properties 2>/dev/null; then
        echo "PASS  transform catalog requires TRINO_NESSIE_PREFIX"
    else
        echo "FAIL  transform catalog prefix is missing or unexpected"
    fi

    if grep -RqsE 'DBT_TRINO_HOST|DBT_TRINO_PORT|DBT_TRINO_USER|DBT_TRINO_PASSWORD' \
        docker-compose.transform-execution.yaml \
        orchestration/job_runner 2>/dev/null; then
        echo "PART  Trino worker configuration references exist"
    else
        echo "FAIL  Trino worker configuration not wired"
    fi

    if grep -qsE 'transform-trino' docker-compose.transform-execution.yaml 2>/dev/null; then
        echo "PASS  transform-trino service exists"
    else
        echo "FAIL  transform-trino service not yet defined"
    fi

    if grep -RqsEi \
        'nessie_iceberg_plugin|type:[[:space:]]*duckdb|DBT_DUCKDB' \
        orchestration/job_runner \
        transform/dbt \
        docker-compose.transform-execution.yaml \
        2>/dev/null; then
        echo "WARN  DuckDB/plugin execution references remain"
    else
        echo "PASS  no DuckDB/plugin references in primary execution path"
    fi

    echo
    echo "============================================================"
    echo " AUDIT COMPLETE"
    echo "============================================================"
} | tee "$tmp"

cp "$tmp" "$REPORT"

echo
echo "Report saved to: $REPORT"
