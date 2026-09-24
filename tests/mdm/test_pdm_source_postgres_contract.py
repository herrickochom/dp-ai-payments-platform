import importlib.util
import json
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


def test_generator_keeps_pure_deterministic_contract():
    generator = _generator()
    assert generator.beneficiary_sources() == generator.beneficiary_sources()
    datasets = generator.build_resolved_mdm()
    assert set(datasets) == set(TABLES)
    assert all(row["record_classification"] == "RESTRICTED_IDENTITY"
               for row in datasets["golden_beneficiaries_restricted"])
    assert all("nin" not in row for row in datasets["source_crosswalk"])


def test_json_export_remains_explicit_and_complete(tmp_path):
    generator = _generator()
    generator.MDM_ROOT = tmp_path
    generator.materialise_resolved_mdm()
    assert {path.stem for path in tmp_path.glob("*.json")} == set(TABLES)
    assert "materialise_token_link" not in generator.materialise_resolved_mdm.__code__.co_names


def test_generator_image_has_one_postgres_driver_and_no_automatic_mdm_run():
    requirements = (ROOT / "services/shared/requirements-runtime.txt").read_text()
    dockerfile = (ROOT / "platform/docker/dockerfiles/Dockerfile.payment-xml-generator").read_text()
    workflow = (ROOT / "services/payment-xml-generator/generate_all_sources.py").read_text()
    assert "psycopg2-binary==" in requirements
    assert "COPY services/shared/mdm/ /app/services/shared/mdm/" in dockerfile
    assert "mdm_generator.py" not in workflow


def test_isolated_database_and_dormant_cdc_contract():
    compose = yaml.safe_load((ROOT / "docker-compose.yaml").read_text())
    platform = compose["services"]["postgres"]
    source = compose["services"]["pdm-source-postgres"]
    assert source["profiles"] == ["cdc"]
    assert source["image"] == APPROVED_POSTGRES_IMAGE
    assert platform["build"]["dockerfile"] == "platform/docker/dockerfiles/Dockerfile.postgresql"
    assert (ROOT / platform["build"]["dockerfile"]).read_text().startswith(
        f"FROM {APPROVED_POSTGRES_IMAGE}\n")
    assert (ROOT / "platform/docker/dockerfiles/Dockerfile.postgres-healthcheck").read_text(
    ).startswith(f"FROM {APPROVED_POSTGRES_IMAGE}\n")
    assert source["environment"]["POSTGRES_DB"] != platform["environment"]["POSTGRES_DB"]
    assert source["environment"]["POSTGRES_USER"] != platform["environment"]["POSTGRES_USER"]
    assert source["environment"]["POSTGRES_PASSWORD"] != platform["environment"]["POSTGRES_PASSWORD"]
    assert "PDM_CDC_USER" not in platform["environment"]
    assert "PDM_CDC_PASSWORD" not in platform["environment"]
    assert any(volume.startswith("pdm-source-pgdata:") for volume in source["volumes"])
    assert not any(volume.startswith("pdm-source-pgdata:") for volume in platform["volumes"])
    assert platform["volumes"] != source["volumes"]
    assert "pdm-source-pgdata" in compose["volumes"]
    assert "wal_level=logical" in source["command"]
    assert "POSTGRES_PASSWORD" in source["environment"]
    assert "PDM_CDC_USER" in source["environment"]
    assert "data-platform-network" in source["networks"]
    assert "wal_level=logical" not in platform.get("command", [])

    sql = (ROOT / "platform/pdm-source-postgres/initdb/10_mdm_schema.sql").read_text()
    for table in TABLES:
        assert f"mdm.{table}" in sql
    assert "customer_id" not in sql
    assert "FOR ALL TABLES" not in sql
    assert "CREATE PUBLICATION pdm_mdm_ordinary_pub FOR TABLE mdm.sacco_master_sources" in sql
    for table in ("beneficiary_master_sources", "golden_beneficiaries_restricted",
                  "beneficiary_identity_alerts_restricted", "source_crosswalk"):
        assert f"COMMENT ON TABLE mdm.{table} IS 'RESTRICTED_" in sql
    grants = (ROOT / "platform/pdm-source-postgres/initdb/20_cdc_identity.sh").read_text()
    assert "GRANT SELECT ON mdm.sacco_master_sources" in grants
    assert "GRANT SELECT ON mdm.beneficiary" not in grants
    assert "pg_create_logical_replication_slot('pdm_mdm_ordinary_slot'" in grants

    cdc = json.loads((ROOT / "platform/cdc/contracts/pdm_mdm_source.json").read_text())
    assert cdc["table_include_list"] == ["mdm.sacco_master_sources"]
    assert cdc["topic"] == "cdc.pdm_mdm.mdm.sacco_master_sources"
    registry = json.loads((ROOT / "platform/source_registry/contracts/source_systems.json").read_text())
    source_entry = next(x for x in registry["source_systems"] if x["source_system"] == "pdm_mdm")
    assert source_entry["cdc_activated"] is False
    assert source_entry["cdc_capable"] is False
    assert next(x for x in registry["platform_databases"] if x["name"] == "platform_postgres")["cdc_source"] is False


def _base_image(dockerfile: Path) -> str:
    for line in dockerfile.read_text().splitlines():
        if line.startswith("FROM "):
            return line.split()[1]
    raise AssertionError(f"no FROM directive in {dockerfile}")


def test_postgresql_version_standard_and_instance_isolation():
    """One image standard, two PostgreSQL instances, no isolation regression."""
    compose = yaml.safe_load((ROOT / "docker-compose.yaml").read_text())
    platform = compose["services"]["postgres"]
    healthcheck = compose["services"]["postgres-healthcheck"]
    source = compose["services"]["pdm-source-postgres"]

    # 1-3. Every repository-owned PostgreSQL runtime image uses one tag.
    assert _base_image(ROOT / platform["build"]["dockerfile"]) == APPROVED_POSTGRES_IMAGE
    assert _base_image(
        ROOT / "platform/docker/dockerfiles/Dockerfile.postgres-healthcheck"
    ) == APPROVED_POSTGRES_IMAGE
    assert source["image"] == APPROVED_POSTGRES_IMAGE

    # No other repository-owned definition may pin a different PostgreSQL tag.
    postgres_bases = {
        dockerfile.name: _base_image(dockerfile)
        for dockerfile in sorted((ROOT / "platform/docker/dockerfiles").glob("Dockerfile*"))
        if _base_image(dockerfile).startswith("postgres:")
    }
    assert postgres_bases == {
        "Dockerfile.postgres-healthcheck": APPROVED_POSTGRES_IMAGE,
        "Dockerfile.postgresql": APPROVED_POSTGRES_IMAGE,
    }
    assert {
        name: service["image"]
        for name, service in compose["services"].items()
        if str(service.get("image", "")).startswith("postgres:")
    } == {"pdm-source-postgres": APPROVED_POSTGRES_IMAGE}

    # 4. Separate services with separate definitions.
    assert platform["build"]["dockerfile"] != source["image"]
    assert "build" in platform and "image" not in platform
    assert "image" in source and "build" not in source
    assert platform["container_name"] != source["container_name"]

    # 5. Separate volumes.
    platform_volumes = {volume.split(":")[0] for volume in platform["volumes"]}
    source_volumes = {volume.split(":")[0] for volume in source["volumes"]}
    assert platform_volumes.isdisjoint(source_volumes)
    assert platform_volumes == {"pgdata"}
    assert any(volume.startswith("pdm-source-pgdata:") for volume in source["volumes"])
    assert not any(volume.startswith("pgdata:") for volume in source["volumes"])
    assert not any(volume.startswith("pdm-source-pgdata:") for volume in platform["volumes"])
    assert "pgdata" in compose["volumes"] and "pdm-source-pgdata" in compose["volumes"]

    # 6. Separate databases and credentials.
    assert source["environment"]["POSTGRES_DB"] != platform["environment"]["POSTGRES_DB"]
    assert source["environment"]["POSTGRES_USER"] != platform["environment"]["POSTGRES_USER"]
    assert source["environment"]["POSTGRES_PASSWORD"] != platform["environment"]["POSTGRES_PASSWORD"]
    assert "PDM_CDC_USER" not in platform["environment"]
    assert "PDM_CDC_PASSWORD" not in platform["environment"]
    assert "PDM_CDC_USER" not in healthcheck["environment"]
    assert "PDM_CDC_PASSWORD" not in healthcheck["environment"]

    # 7. Only the PDM source carries logical CDC configuration.
    assert "wal_level=logical" in source["command"]
    assert "wal_level=logical" not in platform.get("command", [])
    assert "max_replication_slots=4" in source["command"]
    assert not any("replication" in argument for argument in platform.get("command", []))

    # 8. The platform instance stays a non-CDC source in the registry.
    registry = json.loads((ROOT / "platform/source_registry/contracts/source_systems.json").read_text())
    assert next(
        x for x in registry["platform_databases"] if x["name"] == "platform_postgres"
    )["cdc_source"] is False


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
    compose = yaml.safe_load((ROOT / "docker-compose.yaml").read_text())
    pgadmin = compose["services"]["pgadmin"]
    kafka_ui = compose["services"]["kafka-ui"]
    assert pgadmin["environment"]["PGADMIN_DEFAULT_PASSWORD"] == "${PGADMIN_DEFAULT_PASSWORD:-}"
    assert "PGADMIN_DEFAULT_PASSWORD is required" in pgadmin["entrypoint"][-1]
    assert kafka_ui["environment"]["SPRING_SECURITY_USER_PASSWORD"] == "${KAFKA_UI_PASSWORD:-}"
    assert "KAFKA_UI_PASSWORD is required" in kafka_ui["command"][-1]
    assert kafka_ui["environment"]["KAFKA_CLUSTERS_0_READONLY"] == "true"
    assert kafka_ui["environment"]["MCP_ENABLED"] == "false"
    assert kafka_ui["environment"]["DYNAMIC_CONFIG_ENABLED"] == "false"
    assert kafka_ui["environment"]["KAFKA_CLUSTERS_0_SCHEMAREGISTRY"]
    assert pgadmin["ports"][0].startswith("127.0.0.1:")
    assert kafka_ui["ports"][0].startswith("127.0.0.1:")
    assert "POSTGRES_PASSWORD" not in pgadmin["environment"]
    assert "PDM_CDC_PASSWORD" not in kafka_ui["environment"]
