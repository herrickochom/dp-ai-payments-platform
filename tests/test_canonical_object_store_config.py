"""Static contract for canonical object-store configuration.

The local .env file is the operational configuration source of truth.
It is deliberately untracked and must never be inspected by repository tests.
Repository tests instead verify that runtime configuration is environment-driven
and that no competing object-store layout is defined.
"""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_private_env_is_not_tracked_or_replaced_by_template():
    """The repository must not carry an environment template or private .env."""
    assert not (ROOT / ".env.example").exists()

    gitignore = (ROOT / ".gitignore").read_text()
    lines = {
        line.strip()
        for line in gitignore.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }

    assert ".env" in lines
    assert ".env.*" in lines
    assert "!.env.example" not in lines


def test_runtime_uses_canonical_warehouse_location():
    compose = (ROOT / "docker-compose.yaml").read_text()
    nessie = (ROOT / "platform/nessie/config/application.properties").read_text()

    assert "WAREHOUSE_BUCKET" not in compose + nessie
    assert "nessie.catalog.warehouses.payments.location=${WAREHOUSE_URI}" in nessie
    assert "nessie.catalog.default-warehouse=${NESSIE_WAREHOUSE}" in nessie

    assert "WAREHOUSE_URI" in compose
    assert "OBJECT_STORE_BUCKET" in compose
    assert "RAW_PREFIX" in compose


def test_dbt_raw_source_is_environment_driven():
    project = (ROOT / "transform/dbt/dbt_project.yml").read_text()

    assert "WAREHOUSE_BUCKET" not in project
    assert 's3_bucket: "{{ env_var(\'OBJECT_STORE_BUCKET\') }}"' in project
    assert 's3_path: "{{ env_var(\'RAW_ROOT\') }}"' in project
    assert "WAREHOUSE_URI" not in project


def test_runtime_does_not_define_a_second_warehouse_bucket():
    runtime_files = [ROOT / "docker-compose.yaml"]

    for directory in ("services", "orchestration", "platform", "transform"):
        runtime_files.extend(
            path
            for path in (ROOT / directory).rglob("*")
            if path.is_file()
            and path.suffix
            in {".py", ".yaml", ".yml", ".properties", ".sql"}
        )

    assert all(
        "dp-ai-payment-warehouse" not in path.read_text()
        for path in runtime_files
    )


def test_object_store_configuration_names_are_canonical():
    """Runtime configuration must use the canonical environment vocabulary."""
    compose = (ROOT / "docker-compose.yaml").read_text()
    runtime = "\n".join(
        path.read_text()
        for directory in ("services", "orchestration", "platform", "transform")
        for path in (ROOT / directory).rglob("*")
        if path.is_file()
        and path.suffix in {".py", ".yaml", ".yml", ".properties"}
    )

    combined = compose + "\n" + runtime

    for name in (
        "OBJECT_STORE_BUCKET",
        "OBJECT_STORE_REGION",
        "RAW_PREFIX",
        "WAREHOUSE_URI",
        "S3_ENDPOINT",
    ):
        assert name in combined
