#!/usr/bin/env bash

set +e
FAIL=0

SCOPE="${1:-core}"

ROOT="$(
  git rev-parse --show-toplevel 2>/dev/null
)"

if [ -z "${ROOT:-}" ]; then
    echo "ERROR=repository_root_not_found"
    exit 2
fi

cd "$ROOT" || exit 2

TF_ROOT="infra/terraform"
INVENTORY="$TF_ROOT/targets/local/contracts/compose_inventory.json"

echo "TARGET=local"
echo "PREFLIGHT_SCOPE=$SCOPE"
echo "MUTATING=FALSE"


echo
echo "--- terraform ---"

if command -v terraform >/dev/null 2>&1; then
    echo "TERRAFORM_AVAILABLE=PASS"

    TF_VERSION="$(
      terraform version -json 2>/dev/null |
      python3 -c \
        'import json,sys; print(json.load(sys.stdin)["terraform_version"])'
    )"

    echo "TERRAFORM_VERSION=$TF_VERSION"

    case "$TF_VERSION" in
      1.16.*)
        echo "TERRAFORM_VERSION_CONTRACT=PASS"
        ;;
      *)
        echo "TERRAFORM_VERSION_CONTRACT=FAIL"
        FAIL=1
        ;;
    esac
else
    echo "TERRAFORM_AVAILABLE=FAIL"
    FAIL=1
fi


echo
echo "--- docker ---"

if command -v docker >/dev/null 2>&1; then
    echo "DOCKER_AVAILABLE=PASS"
else
    echo "DOCKER_AVAILABLE=FAIL"
    FAIL=1
fi

if docker compose version >/dev/null 2>&1; then
    echo "DOCKER_COMPOSE_AVAILABLE=PASS"
else
    echo "DOCKER_COMPOSE_AVAILABLE=FAIL"
    FAIL=1
fi


echo
echo "--- deployment scope ---"

if [ "$SCOPE" = "core" ]; then
    echo "DEPLOYMENT_SCOPE_TYPE=DEFAULT"
    echo "DEPLOYMENT_PROFILE=NONE"
    echo "DEPLOYMENT_SCOPE_VALID=PASS"
else
    python3 - "$SCOPE" "$INVENTORY" <<'PY'
import json
import sys
from pathlib import Path

scope = sys.argv[1]
inventory = json.loads(
    Path(sys.argv[2]).read_text()
)

profiles = inventory["profiles"]

if scope in profiles:
    print("DEPLOYMENT_SCOPE_TYPE=PROFILE")
    print(f"DEPLOYMENT_PROFILE={scope}")
    print("DEPLOYMENT_SCOPE_VALID=PASS")
else:
    print("DEPLOYMENT_SCOPE_TYPE=UNKNOWN")
    print("DEPLOYMENT_SCOPE_VALID=FAIL")
    raise SystemExit(1)
PY

    RC=$?

    if [ "$RC" -ne 0 ]; then
        FAIL=1
    fi
fi


echo
echo "--- compose configuration ---"

if docker compose config --quiet; then
    echo "COMPOSE_CONFIG_VALID=PASS"
else
    echo "COMPOSE_CONFIG_VALID=FAIL"
    FAIL=1
fi


echo
echo "--- inventory conformance ---"

python3 - "$INVENTORY" <<'PY'
import json
import subprocess
import sys
from pathlib import Path


def values(args):
    result = subprocess.run(
        args,
        check=True,
        capture_output=True,
        text=True,
    )

    return sorted(
        {
            line.strip()
            for line in result.stdout.splitlines()
            if line.strip()
        }
    )


inventory = json.loads(
    Path(sys.argv[1]).read_text()
)

actual_services = values(
    ["docker", "compose", "config", "--services"]
)

actual_profiles = values(
    ["docker", "compose", "config", "--profiles"]
)

if inventory["default_services"] != actual_services:
    print("COMPOSE_SERVICE_INVENTORY=FAIL")
    raise SystemExit(1)

if inventory["profiles"] != actual_profiles:
    print("COMPOSE_PROFILE_INVENTORY=FAIL")
    raise SystemExit(1)

print("COMPOSE_SERVICE_INVENTORY=PASS")
print("COMPOSE_PROFILE_INVENTORY=PASS")
print(
    "DEFAULT_SERVICE_COUNT="
    + str(len(actual_services))
)
print(
    "PROFILE_COUNT="
    + str(len(actual_profiles))
)
PY

RC=$?

if [ "$RC" -ne 0 ]; then
    FAIL=1
fi


echo
echo "--- terraform configuration ---"

terraform \
  -chdir="$TF_ROOT" \
  fmt \
  -check \
  -recursive >/dev/null

RC=$?

if [ "$RC" -eq 0 ]; then
    echo "TERRAFORM_FORMAT=PASS"
else
    echo "TERRAFORM_FORMAT=FAIL"
    FAIL=1
fi

terraform \
  -chdir="$TF_ROOT" \
  validate \
  -no-color >/dev/null

RC=$?

if [ "$RC" -eq 0 ]; then
    echo "TERRAFORM_VALIDATE=PASS"
else
    echo "TERRAFORM_VALIDATE=FAIL"
    FAIL=1
fi


echo
echo "--- current runtime ---"

RUNNING_COUNT="$(
  docker compose ps \
    --services \
    --status running \
    2>/dev/null |
  sed '/^$/d' |
  wc -l
)"

echo "RUNNING_SERVICE_COUNT=$RUNNING_COUNT"

UNHEALTHY_COUNT="$(
  docker compose ps \
    --format json \
    2>/dev/null |
  python3 -c '
import json
import sys

text = sys.stdin.read().strip()

if not text:
    print(0)
    raise SystemExit

try:
    doc = json.loads(text)

    if isinstance(doc, dict):
        rows = [doc]
    else:
        rows = doc
except json.JSONDecodeError:
    rows = []

    for line in text.splitlines():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            pass

count = 0

for row in rows:
    health = str(
        row.get("Health", "")
    ).lower()

    if health == "unhealthy":
        count += 1

print(count)
'
)"

echo "UNHEALTHY_CONTAINER_COUNT=$UNHEALTHY_COUNT"

if [ "$UNHEALTHY_COUNT" -eq 0 ]; then
    echo "CURRENT_RUNTIME_HEALTH=PASS"
else
    echo "CURRENT_RUNTIME_HEALTH=FAIL"
    FAIL=1
fi


echo
echo "--- host capacity ---"

MEM_KB="$(
  awk '/MemTotal:/ {print $2}' /proc/meminfo
)"

CPU_COUNT="$(
  nproc 2>/dev/null || echo 0
)"

DISK_KB="$(
  df -Pk . |
  awk 'NR==2 {print $4}'
)"

echo "HOST_MEMORY_KB=$MEM_KB"
echo "HOST_CPU_COUNT=$CPU_COUNT"
echo "HOST_AVAILABLE_DISK_KB=$DISK_KB"

if [ "$MEM_KB" -gt 0 ] \
   && [ "$CPU_COUNT" -gt 0 ] \
   && [ "$DISK_KB" -gt 0 ]; then
    echo "HOST_CAPACITY_VISIBLE=PASS"
else
    echo "HOST_CAPACITY_VISIBLE=FAIL"
    FAIL=1
fi


echo
echo "--- compose variable names only ---"

VARIABLE_NAMES="$(
  grep -oE \
    '\$\{[A-Za-z_][A-Za-z0-9_]*' \
    docker-compose.yaml \
    2>/dev/null |
  sed 's/${//' |
  sort -u
)"

VARIABLE_COUNT="$(
  printf '%s\n' "$VARIABLE_NAMES" |
  sed '/^$/d' |
  wc -l
)"

echo "COMPOSE_VARIABLE_NAME_COUNT=$VARIABLE_COUNT"
echo "SECRET_VALUES_PRINTED=NO"


echo
echo "--- mutable image policy ---"

MUTABLE_COUNT="$(
  grep -Ec \
    'image:.*(:latest|:edge|:main|:master)([[:space:]]|$)' \
    docker-compose.yaml \
    2>/dev/null || true
)"

echo "MUTABLE_IMAGE_TAG_COUNT=$MUTABLE_COUNT"

if [ "$MUTABLE_COUNT" -gt 0 ]; then
    echo "LOCAL_MUTABLE_IMAGE_POLICY=WARNING"
    echo "PRODUCTION_PROMOTION_IMAGE_POLICY=BLOCKED"
else
    echo "LOCAL_MUTABLE_IMAGE_POLICY=PASS"
    echo "PRODUCTION_PROMOTION_IMAGE_POLICY=PASS"
fi


echo
echo "--- terraform runtime artefacts ---"

DATA_COUNT="$(
  find "$TF_ROOT" \
    -type d \
    -name '.terraform' \
    2>/dev/null |
  wc -l
)"

STATE_COUNT="$(
  find "$TF_ROOT" \
    -type f \
    \( \
      -name '*.tfstate' \
      -o -name '*.tfstate.*' \
    \) \
    2>/dev/null |
  wc -l
)"

echo "TERRAFORM_DATA_DIRECTORY_COUNT=$DATA_COUNT"
echo "TERRAFORM_STATE_FILE_COUNT=$STATE_COUNT"

if [ "$DATA_COUNT" -ne 0 ] \
   || [ "$STATE_COUNT" -ne 0 ]; then
    echo "TERRAFORM_RUNTIME_ARTIFACT_POLICY=FAIL"
    FAIL=1
else
    echo "TERRAFORM_RUNTIME_ARTIFACT_POLICY=PASS"
fi


echo
echo "--- deployment gate ---"

if [ "$FAIL" -eq 0 ]; then
    echo "LOCAL_PREFLIGHT=PASS"
    echo "DEPLOYMENT_ENABLEMENT=NOT_GRANTED"
    exit 0
else
    echo "LOCAL_PREFLIGHT=FAIL"
    echo "DEPLOYMENT_ENABLEMENT=DENIED"
    exit 1
fi
