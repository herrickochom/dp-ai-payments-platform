"""Generate synthetic Compose variables for CI-only configuration validation.

Never use this output to start services or deploy the platform.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

COMPOSE_FILES = (
    ROOT / "docker-compose.yaml",
    ROOT / "docker-compose.transform-execution.yaml",
)

SPECIAL_VALUES = {
    "AIRFLOW_FERNET_KEY": "CI_ONLY_NOT_A_REAL_FERNET_KEY",
    "NESSIE_AUTHENTICATION_ENABLED": "false",
    "NESSIE_OIDC_ISSUER_URL": "https://invalid.example/oidc",
    "NESSIE_TRINO_ENDPOINT": "http://nessie:19120",
    "S3_ENDPOINT": "http://minio:9000",
    "S3_USE_SSL": "false",
    "S3_PATH_STYLE_ACCESS": "true",
    "OBJECT_STORE_REGION": "europe-west2",
    "TRINO_BASE_IMAGE": "trinodb/trino:477",
    "TRINO_SQLALCHEMY_URI": (
        "trino://ci_user:ci_placeholder@trino:8443/iceberg"
    ),
    "TRANSFORM_LEDGER_APP_DATABASE_URL": (
        "postgresql://ci_user:ci_placeholder@postgres:5432/ci"
    ),
    "TRANSFORM_LEDGER_MIGRATION_DATABASE_URL": (
        "postgresql://ci_user:ci_placeholder@postgres:5432/ci"
    ),
    "PROTECTED_BOOTSTRAP_SECRET_DIR": "/tmp/ci-placeholder-secrets",
    "WAREHOUSE_URI": "s3://ci-placeholder/warehouse",
}

pattern = re.compile(
    r"(?<!\$)\$\{([A-Za-z_][A-Za-z_0-9]*):\?"
)

required = set()

for path in COMPOSE_FILES:
    required.update(pattern.findall(path.read_text()))

for name in sorted(required):
    value = SPECIAL_VALUES.get(name, f"ci_placeholder_{name.lower()}")
    print(f"{name}={value}", file=sys.stdout)
