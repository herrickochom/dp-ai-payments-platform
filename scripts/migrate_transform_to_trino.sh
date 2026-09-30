#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

BACKUP="/tmp/dp-ai-trino-migration-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$BACKUP"

FILES=(
  orchestration/job_runner/transform_execution.py
  orchestration/job_runner/transform_worker.py
  orchestration/job_runner/requirements.txt
  platform/docker/dockerfiles/Dockerfile.platform-job-runner
  docker-compose.transform-execution.yaml
  transform/dbt/profiles.yml
)

echo "=== PRE-FLIGHT ==="

for f in "${FILES[@]}"; do
    [[ -f "$f" ]] || {
        echo "ABORT: missing $f"
        exit 1
    }
    mkdir -p "$BACKUP/$(dirname "$f")"
    cp -a "$f" "$BACKUP/$f"
done

# profiles.yml must already be migrated.
grep -Eq '^[[:space:]]*type:[[:space:]]*trino[[:space:]]*$' \
    transform/dbt/profiles.yml || {
    echo "ABORT: transform/dbt/profiles.yml is not using Trino"
    exit 1
}

# Run-scoped catalog must already exist and fail closed.
grep -Fxq \
    'iceberg.rest-catalog.prefix=${ENV:TRINO_NESSIE_PREFIX}' \
    platform/trino/transform-catalog/iceberg.properties || {
    echo "ABORT: run-scoped transform Trino catalog is missing"
    exit 1
}

echo "Backup: $BACKUP"

echo
echo "=== 1. MIGRATE transform_execution.py ==="

python3 - <<'PY'
from pathlib import Path

p = Path("orchestration/job_runner/transform_execution.py")
s = p.read_text()

required = [
    "duckdb_path: str",
    'duckdb_path = str(work / "runtime.duckdb")',
    'duckdb_s3_endpoint = urlsplit(validated_s3_endpoint).netloc',
    '"DBT_DUCKDB_PATH": duckdb_path,',
    '"DUCKDB_S3_ENDPOINT": duckdb_s3_endpoint,',
    "return TransformCommand(batch_id, batch.authority, argv, environment, fingerprint, duckdb_path)",
]

missing = [x for x in required if x not in s]
if missing:
    raise SystemExit(
        "ABORT: transform_execution.py does not match audited version:\n"
        + "\n".join(missing)
    )

# urlsplit was only required for DuckDB's endpoint transformation.
s = s.replace("from urllib.parse import urlsplit\n", "")

# The command no longer owns a local execution database.
s = s.replace(
    "    duckdb_path: str\n",
    "    work_path: str\n",
)

s = s.replace(
    '    duckdb_path = str(work / "runtime.duckdb")\n',
    '    work_path = str(work)\n',
)

# Keep security validation. Remove only DuckDB-specific endpoint conversion.
lines = s.splitlines(keepends=True)
out = []
i = 0
removed_assignment = 0
removed_guard = 0

while i < len(lines):
    stripped = lines[i].strip()

    if stripped == "duckdb_s3_endpoint = urlsplit(validated_s3_endpoint).netloc":
        removed_assignment += 1
        i += 1

        if (
            i + 1 < len(lines)
            and lines[i].strip() == "if not duckdb_s3_endpoint:"
            and 'raise ValueError("validated S3 endpoint has no network location")'
                in lines[i + 1]
        ):
            removed_guard += 1
            i += 2

        # Remove one blank line left by the deleted DuckDB block.
        if i < len(lines) and not lines[i].strip():
            i += 1

        continue

    # The validated URL no longer needs to be assigned.
    if stripped.startswith("validated_s3_endpoint, _, _ = validate_object_store_security("):
        indent = lines[i][:len(lines[i]) - len(lines[i].lstrip())]
        out.append(indent + "validate_object_store_security(\n")
        i += 1
        continue

    out.append(lines[i])
    i += 1

if removed_assignment != 1 or removed_guard != 1:
    raise SystemExit(
        "ABORT: expected exactly one DuckDB endpoint assignment/guard; "
        f"found assignment={removed_assignment}, guard={removed_guard}"
    )

s = "".join(out)

old = '''        "TRANSFORM_S3_ACCESS_KEY_ID": supplied[access_key_name],
        "TRANSFORM_S3_SECRET_ACCESS_KEY": supplied[secret_key_name],
        "NESSIE_TRANSFORM_TOKEN": supplied["NESSIE_TRANSFORM_TOKEN"],
        "PATH": "/opt/dbt/bin:/usr/local/bin:/usr/bin",
        "DBT_DUCKDB_PATH": duckdb_path,
        "DBT_LOG_PATH": dbt_log_path,
        "DBT_TARGET_PATH": dbt_target_path,
        "DUCKDB_S3_ENDPOINT": duckdb_s3_endpoint,
        "DBT_NESSIE_BRANCH": (f"transform_{transform_run_id}" if transform_run_id else f"transform_{execution_id}"),
        "TRANSFORM_AUTHORITY": batch.authority,
'''

new = '''        # These authority-scoped credentials are retained for the
        # transform-Trino execution boundary. dbt itself does not consume
        # object-store credentials.
        "TRANSFORM_S3_ACCESS_KEY_ID": supplied[access_key_name],
        "TRANSFORM_S3_SECRET_ACCESS_KEY": supplied[secret_key_name],
        "NESSIE_TRANSFORM_TOKEN": supplied["NESSIE_TRANSFORM_TOKEN"],
        "PATH": "/opt/dbt/bin:/usr/local/bin:/usr/bin",
        "DBT_LOG_PATH": dbt_log_path,
        "DBT_TARGET_PATH": dbt_target_path,
        "DBT_NESSIE_BRANCH": (
            f"transform_{transform_run_id}"
            if transform_run_id
            else f"transform_{execution_id}"
        ),
        "DBT_TRINO_HOST": supplied.get("DBT_TRINO_HOST", "transform-trino"),
        "DBT_TRINO_PORT": supplied.get("DBT_TRINO_PORT", "8443"),
        "DBT_TRINO_USER": supplied.get("DBT_TRINO_USER", "dbt"),
        "TRANSFORM_AUTHORITY": batch.authority,
'''

if old not in s:
    raise SystemExit("ABORT: execution environment block changed")
s = s.replace(old, new)

s = s.replace(
    "return TransformCommand(batch_id, batch.authority, argv, environment, fingerprint, duckdb_path)",
    "return TransformCommand(batch_id, batch.authority, argv, environment, fingerprint, work_path)",
)

p.write_text(s)
print("updated", p)
PY

echo
echo "=== 2. REMOVE OBSOLETE DUCKDB WORKER COMMENTS ==="

python3 - <<'PY'
from pathlib import Path

p = Path("orchestration/job_runner/transform_worker.py")
s = p.read_text()

s = s.replace(
    """    ``staging`` remains a local DuckDB view namespace and is never created
    in Nessie.
""",
    """    Governed persistent namespaces are created only on the per-run
    Nessie branch. dbt execution is performed through transform Trino.
"""
)

s = s.replace(
    """    # staging stays a local DuckDB view namespace and is never created in
    # Nessie. main changes only via the governed publication path.
""",
    """    # Governed namespaces are created only on the per-run Nessie branch.
    # main changes only through the governed publication path.
"""
)

p.write_text(s)
print("updated", p)
PY

echo
echo "=== 3. REMOVE dbt-duckdb DEPENDENCY ==="

python3 - <<'PY'
from pathlib import Path

p = Path("orchestration/job_runner/requirements.txt")
lines = p.read_text().splitlines()

found = [x for x in lines if x.strip().startswith("dbt-duckdb==")]
if len(found) != 1:
    raise SystemExit(
        f"ABORT: expected one dbt-duckdb dependency, found {len(found)}"
    )

lines = [
    x for x in lines
    if not x.strip().startswith("dbt-duckdb==")
]

if not any(x.strip().startswith("dbt-trino==1.9.3") for x in lines):
    raise SystemExit("ABORT: pinned dbt-trino==1.9.3 is missing")

p.write_text("\n".join(lines) + "\n")
print("removed dbt-duckdb; retained dbt-trino")
PY

echo
echo "=== 4. REMOVE DUCKDB EXTENSION INSTALLATION ==="

python3 - <<'PY'
from pathlib import Path

p = Path("platform/docker/dockerfiles/Dockerfile.platform-job-runner")
s = p.read_text()

old = '''RUN pip install --no-cache-dir \\
    -r /tmp/requirements.txt \\
    && install -d -m 0755 /opt/duckdb/extensions \\
    && python -c "import duckdb; connection = duckdb.connect(':memory:', config={'extension_directory': '/opt/duckdb/extensions'}); [connection.install_extension(name) for name in ('httpfs', 'avro', 'iceberg')]" \\
    && install -d -m 0755 /opt/dbt/bin \\
    && ln -s /usr/local/bin/dbt /opt/dbt/bin/dbt
'''

new = '''RUN pip install --no-cache-dir \\
    -r /tmp/requirements.txt \\
    && install -d -m 0755 /opt/dbt/bin \\
    && ln -s /usr/local/bin/dbt /opt/dbt/bin/dbt
'''

if old not in s:
    raise SystemExit("ABORT: Dockerfile install block differs from audited version")

p.write_text(s.replace(old, new))
print("removed DuckDB extensions from job-runner image")
PY

echo
echo "=== 5. ADD WORKER TRINO CONNECTION CONTRACT ==="

python3 - <<'PY'
from pathlib import Path

p = Path("docker-compose.transform-execution.yaml")
s = p.read_text()

old = """    environment:
      PLATFORM_RUNNER_EXECUTION_ENABLED: 'true'
      PLATFORM_JOB_EXECUTION_ENABLED: 'true'
"""

new = """    environment:
      PLATFORM_RUNNER_EXECUTION_ENABLED: 'true'
      PLATFORM_JOB_EXECUTION_ENABLED: 'true'

      # dbt connects only to the dedicated transform Trino boundary.
      DBT_TRINO_HOST: transform-trino
      DBT_TRINO_PORT: '8443'
      DBT_TRINO_USER: dbt
      DBT_TRINO_PASSWORD: ${DBT_TRINO_PASSWORD:?DBT_TRINO_PASSWORD is required}
"""

if old not in s:
    raise SystemExit("ABORT: transform worker environment differs from audited version")

p.write_text(s.replace(old, new))
print("wired dbt Trino connection variables")
PY

echo
echo "=== 6. STATIC VALIDATION ==="

python3 -m py_compile \
    orchestration/job_runner/transform_execution.py \
    orchestration/job_runner/transform_worker.py

git diff --check

echo
echo "=== 7. POST-MIGRATION EXECUTION REFERENCES ==="

grep -RInEi \
  'DBT_DUCKDB|DUCKDB_S3_ENDPOINT|type:[[:space:]]*duckdb|nessie_iceberg_plugin' \
  orchestration/job_runner/transform_execution.py \
  orchestration/job_runner/transform_worker.py \
  transform/dbt/profiles.yml \
  platform/docker/dockerfiles/Dockerfile.platform-job-runner \
  orchestration/job_runner/requirements.txt \
  docker-compose.transform-execution.yaml \
  || true

echo
echo "=== 8. TRINO REFERENCES ==="

grep -RInE \
  'type:[[:space:]]*trino|DBT_TRINO|TRINO_NESSIE_PREFIX|dbt-trino' \
  orchestration/job_runner/transform_execution.py \
  transform/dbt/profiles.yml \
  platform/trino/transform-catalog/iceberg.properties \
  orchestration/job_runner/requirements.txt \
  docker-compose.transform-execution.yaml

echo
echo "============================================================"
echo " STATIC MIGRATION COMPLETE"
echo "============================================================"
echo
echo "Backup:"
echo "  $BACKUP"
echo
echo "IMPORTANT:"
echo "  C4 MUST NOT BE RUN YET."
echo
echo "Remaining runtime gate:"
echo "  transform-trino must be instantiated with:"
echo "    TRINO_NESSIE_PREFIX=<claimed branch>|<NESSIE_WAREHOUSE>"
echo "  and the claimed transform authority's S3 credentials."
echo
