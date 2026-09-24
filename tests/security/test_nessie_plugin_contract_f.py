"""Part 6: branch lifecycle orchestration (worker-owned, plugin never creates)."""
from __future__ import annotations
import importlib, sys, types
import pytest
from test_nessie_plugin_contract_a import ROOT, PLUGIN_PATH, _STUBBED

@pytest.fixture(autouse=True)
def _restore_sys_modules_f():
    mods = {name: sys.modules.get(name) for name in list(_STUBBED) + ["transform_worker", "transform_execution"]}
    try:
        yield
    finally:
        for name, mod in mods.items():
            if mod is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = mod

def _worker(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "orchestration" / "job_runner"))
    monkeypatch.syspath_prepend(str(ROOT / "orchestration"))
    monkeypatch.syspath_prepend(str(ROOT))
    import transform_worker as worker
    return importlib.reload(worker)

def test_worker_ensures_branch_before_dbt(monkeypatch):
    worker = _worker(monkeypatch)
    run_id = "tr_" + "c" * 32
    branch = "transform_" + run_id
    order = []
    class FakePublisher:
        def get_reference(self, name):
            order.append(("get", name))
            return {"name": name, "hash": "base-hash"}
        def create_run_branch(self, rid, base_ref, base_hash):
            order.append(("create", rid, base_ref, base_hash))
            assert (rid, base_ref, base_hash) == (run_id, "main", "base-hash")
            return {"name": branch, "hash": "new-hash"}
    finished = {}
    class FakeQueue:
        def reconcile_expired(self):
            pass
        def claim(self, w, lease):
            return {"batch_execution_id": "be_" + "d" * 32, "lease_token": "tok", "batch_id": "C4_ML_01", "transform_run_id": run_id}
        def orphan_if_owned(self, *a):
            raise AssertionError("must not orphan on success")
        def finish(self, *a, **k):
            finished["ok"] = True
            return {}
    monkeypatch.setenv("PLATFORM_RUNNER_EXECUTION_ENABLED", "true")
    monkeypatch.setenv("PLATFORM_JOB_EXECUTION_ENABLED", "true")
    monkeypatch.setenv("NESSIE_BASE_HASH", "")
    monkeypatch.setattr(worker, "execute", lambda cmd, **k: types.SimpleNamespace(status="SUCCEEDED", returncode=0, output="", output_truncated=False, failure_class=None))
    # run_once builds the real dbt command (needs authority creds); supply
    # synthetic source creds by monkeypatching ambient env resolution.
    import transform_execution as texec
    real_ambient = texec._ambient_environment
    monkeypatch.setattr(texec, "_ambient_environment", lambda: {"ML_S3_ACCESS_KEY_ID": "k", "ML_S3_SECRET_ACCESS_KEY": "s", "NESSIE_TRANSFORM_TOKEN": "t", "NESSIE_ENDPOINT": "http://nessie:19120", "S3_ENDPOINT": "http://minio:9000", "S3_USE_SSL": "false", "OBJECT_STORE_REGION": "us-east-1", "OBJECT_STORE_BUCKET": "dp-ai-payment", "RAW_ROOT": "raw", "RAW_VERSION": "v2", "RAW_PREFIX": "raw/v2", "WAREHOUSE_PREFIX": "warehouse", "WAREHOUSE_URI": "s3://dp-ai-payment/warehouse", "DBT_S3_URL_STYLE": "path"})
    assert worker.ensure_nessie_branch(run_id, publisher_factory=FakePublisher()) == branch
    assert [o[0] for o in order] == ["get", "create"]
    assert worker.run_once(queue=FakeQueue(), worker="w", branch_factory=FakePublisher()) is True
    assert finished.get("ok")

def test_worker_be_fallback_branch_created_before_dbt(monkeypatch):
    worker = _worker(monkeypatch)
    run_id = "be_" + "e" * 32
    branch = "transform_" + run_id
    class FakePublisher:
        def get_reference(self, name):
            return {"name": name, "hash": "base-hash"}
        def create_run_branch(self, rid, base_ref, base_hash):
            assert (rid, base_ref, base_hash) == (run_id, "main", "base-hash")
            return {"name": branch, "hash": "new-hash"}
    monkeypatch.setenv("NESSIE_BASE_HASH", "")
    assert worker.ensure_nessie_branch(run_id, publisher_factory=FakePublisher()) == branch

def test_worker_branch_conflict_orphans_without_dbt(monkeypatch):
    worker = _worker(monkeypatch)
    from orchestration.transform_runtime.nessie_publication import NessieConflict
    class Boom:
        def get_reference(self, name):
            return {"name": name, "hash": "base-hash"}
        def create_run_branch(self, *a):
            raise NessieConflict("drift")
    emitted = {}
    class FakeQueue:
        def reconcile_expired(self):
            pass
        def claim(self, w, lease):
            return {"batch_execution_id": "be_" + "f" * 32, "lease_token": "tok", "batch_id": "C4_ML_01", "transform_run_id": "tr_" + "1" * 32}
        def orphan_if_owned(self, eid, w, tok, failure):
            emitted["failure"] = failure
            return True
    monkeypatch.setenv("PLATFORM_RUNNER_EXECUTION_ENABLED", "true")
    monkeypatch.setenv("PLATFORM_JOB_EXECUTION_ENABLED", "true")
    monkeypatch.setenv("NESSIE_BASE_HASH", "")
    assert worker.run_once(queue=FakeQueue(), worker="w", branch_factory=Boom()) is True
    assert emitted["failure"].startswith("NESSIE_BRANCH_UNAVAILABLE")

def test_plugin_has_no_main_fallback():
    assert "main" not in PLUGIN_PATH.read_text().replace("remain", "")
