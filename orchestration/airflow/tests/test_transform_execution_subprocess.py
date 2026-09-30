import os
import sys
import time

from orchestration.job_runner.transform_execution import TransformCommand, execute


def command_for(script: str) -> TransformCommand:
    return TransformCommand(
        batch_id="test",
        authority="test",
        argv=(sys.executable, "-c", script),
        environment={"PATH": os.environ.get("PATH", "")},
        trino_environment={},
        model_fingerprint="test",
        work_path="/tmp",
    )


def run(script: str, *, output_cap_bytes: int = 128 * 1024, **kwargs):
    return execute(
        command_for(script),
        timeout_seconds=kwargs.pop("timeout_seconds", 5),
        termination_grace_seconds=kwargs.pop(
            "termination_grace_seconds", 0.2
        ),
        output_cap_bytes=output_cap_bytes,
        **kwargs,
    )


def test_noisy_child_is_drained_without_deadlock_and_output_is_bounded():
    output_cap_bytes = 128 * 1024
    emitted_bytes = 4 * 1024 * 1024

    result = run(
        "import os; "
        f"os.write(1, b'x' * {emitted_bytes}); "
        "os.write(1, b'RAW_TO_BRONZE_PUBLICATION=PASS models=24\\n')",
        output_cap_bytes=output_cap_bytes,
    )

    assert result.status == "SUCCEEDED"
    assert result.return_code == 0
    assert result.failure_class is None
    assert len(result.output.encode()) == output_cap_bytes
    assert result.output == "x" * output_cap_bytes
    assert "RAW_TO_BRONZE_PUBLICATION=PASS" not in result.output
    assert result.output_truncated is True


def test_output_below_cap_is_preserved_and_redacted():
    result = run(
        "import os; os.write(2, b'before token=secret-value after')",
        output_cap_bytes=1024,
    )

    assert result.status == "SUCCEEDED"
    assert result.output == "before token=[REDACTED] after"
    assert result.output_truncated is False


def test_success_does_not_require_a_textual_publication_marker():
    result = run(
        "import os; os.write(1, b'completed without a marker')",
        output_cap_bytes=1024,
    )

    assert result.status == "SUCCEEDED"
    assert result.return_code == 0
    assert result.failure_class is None
    assert "RAW_TO_BRONZE_PUBLICATION" not in result.output
    assert result.output_truncated is False


def test_nonzero_child_return_code_remains_failed():
    result = run(
        "import os; os.write(1, b'failure output'); raise SystemExit(7)",
        output_cap_bytes=1024,
    )

    assert result.status == "FAILED"
    assert result.return_code == 7
    assert result.failure_class == "DBT_BUILD_FAILED"
    assert result.output == "failure output"
    assert result.output_truncated is False


def test_redaction_expansion_does_not_exceed_output_cap():
    result = run(
        "import os; os.write(1, b'token=x')",
        output_cap_bytes=7,
    )

    assert result.status == "SUCCEEDED"
    assert len(result.output.encode("utf-8")) <= 7
    assert "x" not in result.output
    assert result.output_truncated is True


def test_timeout_terminates_noisy_child_with_reconciliation_semantics():
    result = run(
        "import os\nwhile True: os.write(1, b'x' * 65536)",
        timeout_seconds=0.2,
        output_cap_bytes=4096,
    )

    assert result.status == "ORPHANED"
    assert result.failure_class == "TIMEOUT_REQUIRES_RECONCILIATION"
    assert result.return_code is not None
    assert len(result.output.encode()) <= 4096
    assert result.output_truncated is True


def test_operator_cancellation_terminates_noisy_child():
    started = time.monotonic()

    result = run(
        "import os\nwhile True: os.write(1, b'x' * 65536)",
        cancel_requested=lambda: time.monotonic() - started >= 0.2,
        output_cap_bytes=4096,
    )

    assert result.status == "CANCELLED"
    assert result.failure_class == "OPERATOR_CANCELLED"
    assert result.return_code is not None
    assert len(result.output.encode()) <= 4096
    assert result.output_truncated is True
