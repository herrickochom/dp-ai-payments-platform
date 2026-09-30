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

_REQUIRED = frozenset({"DBT_NESSIE_BRANCH", "TRINO_S3_ACCESS_KEY_ID", "TRINO_S3_SECRET_ACCESS_KEY", "NESSIE_WAREHOUSE", "TRINO_KEYSTORE_PASSWORD", "TRINO_INTERNAL_SHARED_SECRET"})

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
    _write(root / "config.properties", "coordinator=true\nnode-scheduler.include-coordinator=true\nhttp-server.http.port=18080\nhttp-server.https.enabled=true\nhttp-server.https.port=18443\nhttp-server.https.keystore.path=/opt/transform-trino-security/trino-keystore.jks\nhttp-server.https.keystore.key=${ENV:TRINO_KEYSTORE_PASSWORD}\nhttp-server.authentication.type=PASSWORD\nhttp-server.authentication.allow-insecure-over-http=true\nhttp-server.authentication.password.user-mapping.pattern=(.*)\ninternal-communication.shared-secret=${ENV:TRINO_INTERNAL_SHARED_SECRET}\nquery.max-memory=400MB\nquery.max-memory-per-node=300MB\ndiscovery.uri=http://127.0.0.1:18080\n")
    _write(root / "node.properties", f"node.environment=production\nnode.id={branch}\nnode.data-dir={root / 'data'}\n")
    _write(root / "jvm.config", "-server\n-Xmx768M\n-XX:+UseG1GC\n-XX:+ExitOnOutOfMemoryError\n-XX:ReservedCodeCacheSize=128M\n")
    _write(root / "log.properties", "io.trino=INFO\n")
    _write(root / "access-control.properties", "access-control.name=file\nsecurity.config-file=/opt/transform-trino-security/rules.json\n")
    _write(root / "password-authenticator.properties", "password-authenticator.name=file\nfile.password-file=/opt/transform-trino-security/password.db\n")
    _write(catalog / "iceberg.properties", "connector.name=iceberg\niceberg.catalog.type=nessie\niceberg.nessie-catalog.uri=http://nessie:19120/api/v2\niceberg.nessie-catalog.ref=${ENV:DBT_NESSIE_BRANCH}\niceberg.nessie-catalog.default-warehouse-dir=${ENV:NESSIE_WAREHOUSE}\nfs.native-s3.enabled=true\ns3.endpoint=${ENV:S3_ENDPOINT}\ns3.path-style-access=${ENV:S3_PATH_STYLE_ACCESS}\ns3.region=${ENV:OBJECT_STORE_REGION}\ns3.aws-access-key=${ENV:TRINO_S3_ACCESS_KEY_ID}\ns3.aws-secret-key=${ENV:TRINO_S3_SECRET_ACCESS_KEY}\niceberg.add-files-procedure.enabled=true\n")
    return root

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
    process = subprocess.Popen([os.getenv("TRANSFORM_TRINO_EXECUTABLE", "/usr/lib/trino/bin/run-trino"), "--etc-dir", str(config)], env=dict(environment), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    deadline = time.monotonic() + startup_timeout_seconds
    try:
        while True:
            if process.poll() is not None:
                raise RuntimeError("transform Trino exited during startup")
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
                    "transform Trino readiness query returned unexpected result"
                )
            except Exception:
                if time.monotonic() >= deadline:
                    raise RuntimeError("transform Trino startup timed out")
                time.sleep(0.2)
        yield
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        shutil.rmtree(config, ignore_errors=True)
