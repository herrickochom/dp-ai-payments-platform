"""Run one fail-closed Trino boundary for one claimed transform batch."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import time

import trino
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Mapping


_REQUIRED = frozenset(
    {
        "DBT_NESSIE_BRANCH",
        "TRINO_S3_ACCESS_KEY_ID",
        "TRINO_S3_SECRET_ACCESS_KEY",
        "NESSIE_WAREHOUSE",
        "TRINO_KEYSTORE_PASSWORD",
        "TRINO_INTERNAL_SHARED_SECRET",
    }
)

#: JDK 24 enforces restricted-method access. JNA's Native.load is a
#: restricted method, and oshi (used by Trino's TaskManagerConfig) calls
#: into it, so the child JVM must be started with native access enabled.
#: Without this flag, Trino aborts during startup with
#: "Configuration is invalid" on TaskManagerConfig.
TRINO_JNA_TMPDIR = "/var/lib/platform-transform-worker/jna"
TRINO_JAVA_TOOL_OPTIONS = (
    f"-Djna.tmpdir={TRINO_JNA_TMPDIR} "
    "--enable-native-access=ALL-UNNAMED"
)

_DEFAULT_DIAGNOSTIC_CAP_BYTES = 16 * 1024


def _write(path: Path, value: str) -> None:
    path.write_text(value)
    path.chmod(0o600)


def _runtime_config(work_path: str, environment: Mapping[str, str]) -> Path:
    """Create non-secret Trino config in the execution's private tmpfs path."""
    if any(not environment.get(name) for name in _REQUIRED):
        raise ValueError("required transform Trino configuration is unavailable")

    branch = environment["DBT_NESSIE_BRANCH"]
    if not re.fullmatch(r"transform_(?:tr|be)_[a-f0-9]{32}", branch):
        raise ValueError("invalid execution-scoped Nessie branch")

    root = Path(work_path) / "trino"
    if root.exists():
        shutil.rmtree(root)

    catalog = root / "catalog"
    catalog.mkdir(parents=True, mode=0o700)

    _write(
        root / "config.properties",
        "coordinator=true\n"
        "node-scheduler.include-coordinator=true\n"
        "http-server.http.port=18080\n"
        "http-server.https.enabled=true\n"
        "http-server.https.port=18443\n"
        "http-server.https.keystore.path=/opt/transform-trino-security/trino-keystore.jks\n"
        "http-server.https.keystore.key=${ENV:TRINO_KEYSTORE_PASSWORD}\n"
        "http-server.authentication.type=PASSWORD\n"
        "http-server.authentication.allow-insecure-over-http=true\n"
        "http-server.authentication.password.user-mapping.pattern=(.*)\n"
        "internal-communication.shared-secret=${ENV:TRINO_INTERNAL_SHARED_SECRET}\n"
        "query.max-memory=400MB\n"
        "query.max-memory-per-node=300MB\n"
        "discovery.uri=http://127.0.0.1:18080\n",
    )

    _write(
        root / "node.properties",
        f"node.environment=production\n"
        f"node.id={branch}\n"
        f"node.data-dir={root / 'data'}\n",
    )

    _write(
        root / "jvm.config",
        "-server\n"
        "-Xmx768M\n"
        "-XX:+UseG1GC\n"
        "-XX:+ExitOnOutOfMemoryError\n"
        "-XX:ReservedCodeCacheSize=128M\n",
    )

    _write(root / "log.properties", "io.trino=INFO\n")

    _write(
        root / "access-control.properties",
        "access-control.name=file\n"
        "security.config-file=/opt/transform-trino-security/rules.json\n",
    )

    _write(
        root / "password-authenticator.properties",
        "password-authenticator.name=file\n"
        "file.password-file=/opt/transform-trino-security/password.db\n",
    )

    _write(
        catalog / "iceberg.properties",
        "connector.name=iceberg\n"
        "iceberg.catalog.type=nessie\n"
        "iceberg.nessie-catalog.uri=http://nessie:19120/api/v2\n"
        "iceberg.nessie-catalog.ref=${ENV:DBT_NESSIE_BRANCH}\n"
        "iceberg.nessie-catalog.default-warehouse-dir=${ENV:NESSIE_WAREHOUSE}\n"
        "fs.native-s3.enabled=true\n"
        "s3.endpoint=${ENV:S3_ENDPOINT}\n"
        "s3.path-style-access=${ENV:S3_PATH_STYLE_ACCESS}\n"
        "s3.region=${ENV:OBJECT_STORE_REGION}\n"
        "s3.aws-access-key=${ENV:TRINO_S3_ACCESS_KEY_ID}\n"
        "s3.aws-secret-key=${ENV:TRINO_S3_SECRET_ACCESS_KEY}\n"
        "iceberg.add-files-procedure.enabled=true\n",
    )

    return root


def _diagnostic_cap_bytes() -> int:
    raw = os.getenv(
        "TRANSFORM_TRINO_DIAGNOSTIC_CAP_BYTES",
        str(_DEFAULT_DIAGNOSTIC_CAP_BYTES),
    )

    try:
        value = int(raw)
    except ValueError:
        return _DEFAULT_DIAGNOSTIC_CAP_BYTES

    if value <= 0:
        return _DEFAULT_DIAGNOSTIC_CAP_BYTES

    return min(value, 64 * 1024)


def _startup_diagnostics(path: Path, environment: Mapping[str, str]) -> str:
    """Return a bounded and redacted tail of the private Trino startup log."""
    try:
        size = path.stat().st_size
    except OSError:
        return "startup diagnostics unavailable"

    cap = _diagnostic_cap_bytes()

    try:
        with path.open("rb") as handle:
            if size > cap:
                handle.seek(-cap, os.SEEK_END)
            data = handle.read(cap)
    except OSError:
        return "startup diagnostics unavailable"

    text = data.decode("utf-8", errors="replace")

    secrets = {
        environment.get("TRINO_S3_ACCESS_KEY_ID", ""),
        environment.get("TRINO_S3_SECRET_ACCESS_KEY", ""),
        environment.get("TRINO_KEYSTORE_PASSWORD", ""),
        environment.get("TRINO_INTERNAL_SHARED_SECRET", ""),
    }

    for secret in sorted(
        (value for value in secrets if value),
        key=len,
        reverse=True,
    ):
        text = text.replace(secret, "<REDACTED>")

    text = text.strip()
    if not text:
        return "startup diagnostics unavailable"

    if size > cap:
        return f"[truncated to last {cap} bytes]\n{text}"

    return text


@contextmanager
def transform_trino_runtime(
    work_path: str,
    environment: Mapping[str, str],
    *,
    readiness_user: str,
    readiness_password: str,
    startup_timeout_seconds: float = 90.0,
) -> Iterator[None]:
    """Start, verify, and always terminate a per-execution Trino child."""
    config = _runtime_config(work_path, environment)
    startup_log = config / "startup.log"

    # The child Trino JVM must see the JNA tmpdir and native-access flag.
    # Merge rather than replace, so any existing JAVA_TOOL_OPTIONS the
    # caller set is preserved (though at present there are none).
    child_environment = dict(environment)
    existing_java_tool_options = child_environment.get("JAVA_TOOL_OPTIONS", "").strip()
    child_environment["JAVA_TOOL_OPTIONS"] = (
        (existing_java_tool_options + " ") if existing_java_tool_options else ""
    ) + TRINO_JAVA_TOOL_OPTIONS
    child_environment["JAVA_HOME"] = child_environment.get(
        "JAVA_HOME", "/usr/lib/jvm/temurin/jdk-24.0.2+12"
    )

    process: subprocess.Popen | None = None

    try:
        with startup_log.open("wb") as output:
            startup_log.chmod(0o600)

            process = subprocess.Popen(
                [
                    os.getenv(
                        "TRANSFORM_TRINO_EXECUTABLE",
                        "/usr/lib/trino/bin/run-trino",
                    ),
                    "--etc-dir",
                    str(config),
                ],
                env=child_environment,
                stdout=output,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )

            deadline = time.monotonic() + startup_timeout_seconds

            while True:
                return_code = process.poll()

                if return_code is not None:
                    output.flush()
                    diagnostics = _startup_diagnostics(
                        startup_log,
                        environment,
                    )
                    raise RuntimeError(
                        "transform Trino exited during startup "
                        f"(return_code={return_code}):\n{diagnostics}"
                    )

                try:
                    connection = trino.dbapi.connect(
                        host="127.0.0.1",
                        port=18443,
                        user=readiness_user,
                        http_scheme="https",
                        auth=trino.auth.BasicAuthentication(
                            readiness_user,
                            readiness_password,
                        ),
                        verify=False,
                    )

                    cursor = connection.cursor()

                    try:
                        cursor.execute("SELECT 1")
                        rows = cursor.fetchall()
                    finally:
                        cursor.close()
                        connection.close()

                    if rows == [[1]]:
                        break

                    raise RuntimeError(
                        "transform Trino readiness query returned "
                        "unexpected result"
                    )

                except Exception:
                    if time.monotonic() >= deadline:
                        output.flush()
                        diagnostics = _startup_diagnostics(
                            startup_log,
                            environment,
                        )
                        raise RuntimeError(
                            "transform Trino startup timed out:\n"
                            f"{diagnostics}"
                        )

                    time.sleep(0.2)

        yield

    finally:
        if process is not None and process.poll() is None:
            process.terminate()

            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

        shutil.rmtree(config, ignore_errors=True)