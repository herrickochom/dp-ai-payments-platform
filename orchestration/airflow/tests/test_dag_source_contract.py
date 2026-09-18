from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DAGS = ROOT / "orchestration" / "airflow" / "dags"


def read(name: str) -> str:
    return (DAGS / name).read_text()


def test_main_pipeline_has_expected_jobs():
    source = read("pdm_platform_pipeline.py")

    expected = (
        "platform_preflight",
        "raw_readiness",
        "lakehouse_transform",
        "data_quality",
        "ml_feature_generation",
        "ml_default_risk_scoring",
        "reconciliation",
        "acceptance",
    )

    for name in expected:
        assert name in source


def test_main_pipeline_does_not_generate_data():
    source = read("pdm_platform_pipeline.py")

    assert "generate_payment_source_data" not in source
    assert "generate_source_data" not in source


def test_generator_is_manual_only():
    source = read("pdm_generate_source_data.py")

    assert "schedule=None" in source


def test_generator_is_bounded():
    source = read("pdm_generate_source_data.py")

    assert "minimum=1" in source
    assert "maximum=10000" in source


def test_generator_does_not_clean():
    source = read("pdm_generate_source_data.py")

    assert "--clean" not in source


def test_generator_does_not_start_producer():
    source = read("pdm_generate_source_data.py")

    executable_lines = [
        line
        for line in source.splitlines()
        if not line.strip().startswith((
            "#",
            '"""',
            "-",
        ))
    ]

    executable_source = "\n".join(executable_lines)

    assert "docker compose" not in executable_source
    assert "payment-producer" not in executable_source


def test_no_dag_uses_bash_operator():
    for path in DAGS.glob("*.py"):
        assert "BashOperator" not in path.read_text()


def test_no_dag_uses_subprocess():
    for path in DAGS.glob("*.py"):
        assert "subprocess" not in path.read_text()


def test_no_dag_mounts_docker_socket():
    for path in DAGS.glob("*.py"):
        assert "/var/run/docker.sock" not in path.read_text()


def test_forbidden_operations_absent():
    forbidden = (
        "replay_dlq",
        "reset-offsets",
        "materialise_token_link",
        "train_default_risk",
        "mc rm",
        "mc mirror",
    )

    for path in DAGS.glob("*.py"):
        source = path.read_text()

        for term in forbidden:
            assert term not in source
