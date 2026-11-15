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
