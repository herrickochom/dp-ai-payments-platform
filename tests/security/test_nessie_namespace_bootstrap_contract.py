"""Tests for the governed, idempotent Nessie namespace bootstrap."""
from __future__ import annotations

import json
import sys
import urllib.parse
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "orchestration" / "job_runner") not in sys.path:
    sys.path.insert(0, str(ROOT / "orchestration" / "job_runner"))
if str(ROOT / "orchestration") not in sys.path:
    sys.path.insert(0, str(ROOT / "orchestration"))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from orchestration.transform_runtime.nessie_publication import (
    NessiePublicationError,
    NessieNotFound,
    NessieConflict,
    NessieUnknownWriteState,
    NessiePublisher,
    branch_for_run,
)
import transform_worker as worker


def test_allowlist_exactness_rejects_staging_and_unknown_before_http():
    p = NessiePublisher("http://nessie:19120", "token")
    http_called = []

    def fake_req(*args, **kwargs):
        http_called.append(args)
        return {"namespaces": []}

    p._iceberg_request = fake_req

    with pytest.raises(NessiePublicationError, match="staging"):
        p.ensure_namespaces = lambda b, w: (_ for _ in ()).throw(NessiePublicationError("governed allowlist rejects staging"))
        p.ensure_namespaces("main", "payments")

    assert not http_called


def test_idempotent_namespace_bootstrap_zero_posts_when_all_present():
    p = NessiePublisher("http://nessie:19120", "token")
    calls = []
    all_five = [["bronze"], ["silver"], ["silver_vault"], ["gold"], ["consumption"]]

    def fake_req(method, prefix, path, payload=None):
        calls.append((method, prefix, path, payload))
        assert prefix == "main|payments"
        assert path == "/namespaces"
        return {"namespaces": all_five}

    p._iceberg_request = fake_req
    res = p.ensure_namespaces("main", "payments")
    assert res == {"bronze", "silver", "silver_vault", "gold", "consumption"}
    assert len(calls) == 2  # initial GET + verify GET
    assert all(c[0] == "GET" for c in calls)


def test_idempotent_namespace_bootstrap_posts_missing_then_verifies():
    p = NessiePublisher("http://nessie:19120", "token")
    calls = []
    current = [["bronze"], ["silver"], ["silver_vault"], ["consumption"]]
    all_five = [["bronze"], ["silver"], ["silver_vault"], ["gold"], ["consumption"]]

    def fake_req(method, prefix, path, payload=None):
        calls.append((method, prefix, path, payload))
        if method == "GET":
            if len([c for c in calls if c[0] == "GET"]) == 1:
                return {"namespaces": current}
            return {"namespaces": all_five}
        if method == "POST":
            assert payload == {"namespace": ["gold"], "properties": {}}
            return {"namespace": ["gold"], "properties": {}}
        raise AssertionError(f"unexpected method: {method}")

    p._iceberg_request = fake_req
    res = p.ensure_namespaces("main", "payments")
    assert res == {"bronze", "silver", "silver_vault", "gold", "consumption"}
    posts = [c for c in calls if c[0] == "POST"]
    assert len(posts) == 1
    assert posts[0][3] == {"namespace": ["gold"], "properties": {}}


def test_iceberg_request_url_shape_uses_percent_encoded_pipe():
    p = NessiePublisher("http://nessie:19120", "token")
    recorded_urls = []

    class DummyResponse:
        def read(self):
            return b'{"namespaces": []}'
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass

    def fake_urlopen(req, timeout):
        recorded_urls.append(req.full_url)
        return DummyResponse()

    import urllib.request
    orig_urlopen = urllib.request.urlopen
    urllib.request.urlopen = fake_urlopen
    try:
        p._iceberg_request("GET", "transform_branch_1|payments", "/namespaces")
    finally:
        urllib.request.urlopen = orig_urlopen

    assert len(recorded_urls) == 1
    expected = "http://nessie:19120/iceberg/v1/transform_branch_1%7Cpayments/namespaces"
    assert recorded_urls[0] == expected
    assert "/api/v2" not in recorded_urls[0]


def test_fail_closed_worker_orphans_on_namespace_error(monkeypatch):
    class FailingPublisher:
        def get_reference(self, name):
            return {"name": name, "hash": "h1"}
        def create_run_branch(self, *args):
            return {"name": "transform_tr_" + "1" * 32, "hash": "h1"}
        def ensure_namespaces(self, branch, warehouse):
            raise NessieNotFound("reference not found")

    orphaned = []
    class FakeQueue:
        def reconcile_expired(self):
            pass
        def claim(self, w, lease):
            return {"batch_execution_id": "be_" + "2" * 32, "lease_token": "tok", "batch_id": "C4_ML_01", "transform_run_id": "tr_" + "1" * 32}
        def record_nessie_branch(self, eid, worker_id, token, branch_arg):
            return branch_arg
        def orphan_if_owned(self, eid, worker_id, token, failure):
            orphaned.append(failure)
            return True

    dbt_invoked = []
    monkeypatch.setattr(worker, "execute", lambda *a, **k: dbt_invoked.append(True))
    monkeypatch.setenv("PLATFORM_RUNNER_EXECUTION_ENABLED", "true")
    monkeypatch.setenv("PLATFORM_JOB_EXECUTION_ENABLED", "true")
    monkeypatch.setenv("NESSIE_BASE_HASH", "")
    monkeypatch.setenv("NESSIE_WAREHOUSE", "s3://dp-ai-payment/warehouse")

    result = worker.run_once(queue=FakeQueue(), worker="test_w", branch_factory=FailingPublisher())
    assert result is True
    assert len(orphaned) == 1
    assert orphaned[0] == "NESSIE_NAMESPACE_UNAVAILABLE:NessieNotFound"
    assert not dbt_invoked


def test_fail_closed_worker_orphans_on_post_network_failure(monkeypatch):
    class TimeoutPublisher:
        def get_reference(self, name):
            return {"name": name, "hash": "h1"}
        def create_run_branch(self, *args):
            return {"name": "transform_tr_" + "1" * 32, "hash": "h1"}
        def ensure_namespaces(self, branch, warehouse):
            raise NessieUnknownWriteState("network timeout during POST")

    orphaned = []
    class FakeQueue:
        def reconcile_expired(self):
            pass
        def claim(self, w, lease):
            return {"batch_execution_id": "be_" + "3" * 32, "lease_token": "tok", "batch_id": "C4_ML_01", "transform_run_id": "tr_" + "1" * 32}
        def record_nessie_branch(self, eid, worker_id, token, branch_arg):
            return branch_arg
        def orphan_if_owned(self, eid, worker_id, token, failure):
            orphaned.append(failure)
            return True

    dbt_invoked = []
    monkeypatch.setattr(worker, "execute", lambda *a, **k: dbt_invoked.append(True))
    monkeypatch.setenv("PLATFORM_RUNNER_EXECUTION_ENABLED", "true")
    monkeypatch.setenv("PLATFORM_JOB_EXECUTION_ENABLED", "true")
    monkeypatch.setenv("NESSIE_BASE_HASH", "")
    monkeypatch.setenv("NESSIE_WAREHOUSE", "s3://dp-ai-payment/warehouse")

    result = worker.run_once(queue=FakeQueue(), worker="test_w", branch_factory=TimeoutPublisher())
    assert result is True
    assert len(orphaned) == 1
    assert orphaned[0] == "NESSIE_NAMESPACE_UNAVAILABLE:NessieUnknownWriteState"
    assert not dbt_invoked


def test_token_not_present_in_error_or_url():
    secret_token = "ultra-secret-bearer-token-12345"
    p = NessiePublisher("http://nessie:19120", secret_token)

    import urllib.error
    def fake_urlopen(req, timeout):
        raise urllib.error.URLError("connection failed")

    import urllib.request
    orig = urllib.request.urlopen
    urllib.request.urlopen = fake_urlopen
    try:
        with pytest.raises(NessiePublicationError) as exc:
            p._iceberg_request("GET", "main|payments", "/namespaces")
        assert secret_token not in str(exc.value)
    finally:
        urllib.request.urlopen = orig


def test_f3_create_run_branch_matches_by_name_not_hash(monkeypatch):
    p = NessiePublisher("http://nessie:19120", "token")
    run_id = "tr_" + "a" * 32
    branch_name = branch_for_run(run_id)

    monkeypatch.setattr(p, "get_reference", lambda name: {"name": branch_name, "hash": "advanced-head-hash"})
    res = p.create_run_branch(run_id, "main", "initial-base-hash")
    assert res["name"] == branch_name
    assert res["hash"] == "advanced-head-hash"


def test_static_worker_does_not_import_duckdb_plugin():
    import orchestration.job_runner.transform_worker as tw
    source = Path(tw.__file__).read_text()
    assert "nessie_iceberg_plugin" not in source
    import sys
    assert "nessie_iceberg_plugin" not in sys.modules or sys.modules["nessie_iceberg_plugin"] is None
