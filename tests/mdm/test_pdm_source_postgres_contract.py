import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from services.shared.mdm.postgres_store import TABLES, persist_mdm


ROOT = Path(__file__).resolve().parents[2]

# Single approved PostgreSQL runtime image for every repository-owned
# PostgreSQL definition. Verified Docker Official Image tag.
APPROVED_POSTGRES_IMAGE = "postgres:16.15-alpine"


def _generator():
    path = ROOT / "services/payment-xml-generator/mdm_generator.py"
    spec = importlib.util.spec_from_file_location("mdm_generator_contract", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def isolated_mdm_sources(tmp_path, monkeypatch):
    data_root = tmp_path / "data"
    generator_dir = ROOT / "services/payment-xml-generator"

    env = os.environ.copy()
    env["DATA_ROOT"] = str(data_root)

    subprocess.run(
        [
            sys.executable,
            str(generator_dir / "pdmis_generator.py"),
            "--count", "20",
            "--seed", "20260903",
            "--start-date", "2024-01-01",
            "--as-of-date", "2026-09-08",
            "--output-root", str(data_root / "pdmis"),
        ],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    subprocess.run(
        [
            sys.executable,
            str(generator_dir / "agent_network_generator.py"),
            "--count", "20",
        ],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    monkeypatch.setenv("DATA_ROOT", str(data_root))
    return data_root


def test_generator_keeps_pure_deterministic_contract(isolated_mdm_sources):
    generator = _generator()
    assert generator.beneficiary_sources() == generator.beneficiary_sources()
    datasets = generator.build_resolved_mdm()
    assert set(datasets) == set(TABLES)
    assert all(row["record_classification"] == "RESTRICTED_IDENTITY"
               for row in datasets["golden_beneficiaries_restricted"])
    assert all("nin" not in row for row in datasets["source_crosswalk"])


def test_json_export_remains_explicit_and_complete(
    tmp_path, isolated_mdm_sources
):
    generator = _generator()
    generator.MDM_ROOT = tmp_path
    generator.materialise_resolved_mdm()
    assert {path.stem for path in tmp_path.glob("*.json")} == set(TABLES)
    assert "materialise_token_link" not in generator.materialise_resolved_mdm.__code__.co_names


def test_generator_image_has_one_postgres_driver_and_no_automatic_mdm_run():
    """Cloud SQL MDM remains an explicitly invoked operation, never an automatic publisher."""
    requirements = (ROOT / "services/shared/requirements-runtime.txt").read_text()
    generator = (ROOT / "services/payment-xml-generator/mdm_generator.py").read_text()
    store = (ROOT / "services/shared/mdm/postgres_store.py").read_text()
    assert "psycopg2" in requirements
    assert "--validate-only" in generator
    assert "--target" in generator
    assert "persist_mdm" in generator
    assert "PDM_MDM_FULL_REPLACE_APPROVED" in store



def test_isolated_database_and_dormant_cdc_contract():
    """Operational MDM is external to Docker; CDC remains explicitly denied."""
    import json
    import yaml
    compose = yaml.safe_load((ROOT / "docker-compose.yaml").read_text())
    services = compose.get("services", {})
    assert "pdm-source-postgres" not in services
    assert "pgadmin" not in services
    schema = (ROOT / "platform/databases/pdm-source-postgres/initdb/10_mdm_schema.sql").read_text()
    assert schema.count("CREATE TABLE mdm.") == 10
    assert "CREATE PUBLICATION pdm_mdm_ordinary_pub" in schema
    assert "FOR TABLE mdm.sacco_master_sources" in schema
    cdc = json.loads((ROOT / "platform/cdc/contracts/pdm_cdc_contract.json").read_text())
    assert cdc["activation_policy"]["default"] == "DENY"
    assert cdc["cdc_sources"] == []
    connector = json.loads((ROOT / "platform/cdc/debezium/templates/pdm_mdm_sacco_connector.json").read_text())["config"]
    assert connector["table.include.list"] == "mdm.sacco_master_sources"



def _base_image(dockerfile: Path) -> str:
    for line in dockerfile.read_text().splitlines():
        if line.startswith("FROM "):
            return line.split()[1]
    raise AssertionError(f"no FROM directive in {dockerfile}")


def test_postgresql_version_standard_and_instance_isolation():
    """The local MDM instance has been retired; Cloud SQL is not provisioned by Compose."""
    import yaml
    compose = yaml.safe_load((ROOT / "docker-compose.yaml").read_text())
    services = compose.get("services", {})
    assert "postgres" not in services
    assert "postgres-healthcheck" not in services
    assert "pdm-source-postgres" not in services
    assert "pgadmin" not in services
    store = (ROOT / "services/shared/mdm/postgres_store.py").read_text()
    assert "sslmode" in store and "sslrootcert" in store
    assert "connect_timeout" in store and "statement_timeout" in store



class _Cursor:
    def __init__(self, fail=False):
        self.fail = fail
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, statement):
        self.statements.append(statement)


class _Connection:
    def __init__(self):
        self.cur = _Cursor()
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, kind, *_):
        self.rolled_back = kind is not None
        self.committed = kind is None
        return False

    def cursor(self):
        return self.cur

    def close(self):
        self.closed = True


@pytest.mark.parametrize("fail", [False, True])
def test_postgres_materialisation_is_one_transaction(monkeypatch, fail):
    import psycopg2
    import psycopg2.extras

    for name in ("PDM_SOURCE_HOST", "PDM_SOURCE_DB", "PDM_SOURCE_USER", "PDM_SOURCE_PASSWORD"):
        monkeypatch.setenv(name, "offline-test")
    monkeypatch.setenv('PDM_MDM_FULL_REPLACE_APPROVED', 'I_APPROVE_FULL_MDM_REPLACEMENT')
    monkeypatch.setenv('PDM_MDM_TARGET', 'cloudsql')
    monkeypatch.setenv('PDM_SOURCE_CONNECT_TIMEOUT', '10')
    monkeypatch.setenv('PDM_SOURCE_SSLMODE', 'verify-full')
    monkeypatch.setenv('PDM_SOURCE_SSLROOTCERT', '/tmp/offline-test-ca.pem')
    monkeypatch.setenv('PDM_SOURCE_STATEMENT_TIMEOUT_MS', '30000')
    monkeypatch.setenv('PDM_SOURCE_HOST', '/cloudsql/offline-project:europe-west2:offline-mdm')
    connection = _Connection()
    monkeypatch.setattr(psycopg2, "connect", lambda **_: connection)

    def fake_insert(cursor, statement, rows):
        cursor.statements.append(statement)
        if fail:
            raise RuntimeError("offline insert failure")

    monkeypatch.setattr(psycopg2.extras, "execute_values", fake_insert)
    datasets = {name: [] for name in TABLES}
    datasets["sacco_master_sources"] = [{"source_record_id": "synthetic-id"}]

    if fail:
        with pytest.raises(RuntimeError, match="offline insert failure"):
            persist_mdm(datasets)
    else:
        persist_mdm(datasets)
    assert connection.committed is not fail
    assert connection.rolled_back is fail
    assert connection.closed
    assert sum(statement.startswith("DELETE FROM mdm.") for statement in connection.cur.statements) == 10



def test_optional_ui_credentials_fail_at_startup_not_compose_render():
    """Retired local pgAdmin must not reappear in the runtime Compose contract."""
    import yaml
    compose = yaml.safe_load((ROOT / "docker-compose.yaml").read_text())
    assert "pgadmin" not in compose.get("services", {})

