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

def test_worker_ensures_branch_before_dbt(monkeypatch, caplog):
    worker = _worker(monkeypatch)
    run_id = "tr_" + "c" * 32
    branch = "transform_" + run_id
    order = []
    _seen = {}
    ns_calls = []
    class FakePublisher:
        def get_reference(self, name):
            order.append(("get", name))
            return {"name": name, "hash": "base-hash"}
        def create_run_branch(self, rid, base_ref, base_hash):
            order.append(("create", rid, base_ref, base_hash))
            assert (rid, base_ref, base_hash) == (run_id, "main", "base-hash")
            return {"name": branch, "hash": "new-hash"}
        def ensure_namespaces(self, branch_arg, warehouse):
            ns_calls.append((branch_arg, warehouse))
            return {"bronze", "silver", "silver_vault", "gold", "consumption"}
    finished = {}
    finished_args = {}
    class FakeQueue:
        def reconcile_expired(self):
            pass
        def claim(self, w, lease):
            return {"batch_execution_id": "be_" + "d" * 32, "lease_token": "tok", "batch_id": "C4_ML_01", "transform_run_id": run_id}
        def record_nessie_branch(self, eid, worker_id, token, branch_arg):
            _seen["persisted_branch"] = branch_arg
            return branch_arg
        def orphan_if_owned(self, *a):
            raise AssertionError("must not orphan on success")
        def finish(self, *a, **k):
            finished["ok"] = True
            finished_args["a"] = a
            finished_args["k"] = k
            return {}
    monkeypatch.setenv("PLATFORM_RUNNER_EXECUTION_ENABLED", "true")
    monkeypatch.setenv("PLATFORM_JOB_EXECUTION_ENABLED", "true")
    monkeypatch.setenv("NESSIE_BASE_HASH", "")
    monkeypatch.setenv("NESSIE_WAREHOUSE", "s3://dp-ai-payment/warehouse")
    def _fake_execute(cmd, **k):
        _seen["cmd"] = cmd
        return types.SimpleNamespace(status="SUCCEEDED", return_code=0, output="dbt build ok", output_truncated=False, failure_class=None)
    monkeypatch.setattr(worker, "execute", _fake_execute)
    # run_once builds the real dbt command (needs authority creds); supply
    # synthetic source creds by monkeypatching ambient env resolution.
    import transform_execution as texec
    real_ambient = texec._ambient_environment
    monkeypatch.setattr(texec, "_ambient_environment", lambda: {"ML_TRANSFORM_S3_ACCESS_KEY_ID": "k", "ML_TRANSFORM_S3_SECRET_ACCESS_KEY": "s", "NESSIE_TRANSFORM_TOKEN": "t", "NESSIE_ENDPOINT": "http://nessie:19120", "S3_ENDPOINT": "http://minio:9000", "S3_USE_SSL": "false", "OBJECT_STORE_REGION": "us-east-1", "OBJECT_STORE_BUCKET": "dp-ai-payment", "RAW_ROOT": "raw", "RAW_VERSION": "v2", "RAW_PREFIX": "raw/v2", "WAREHOUSE_PREFIX": "warehouse", "WAREHOUSE_URI": "s3://dp-ai-payment/warehouse", "NESSIE_WAREHOUSE": "s3://dp-ai-payment/warehouse", "DBT_TRINO_PASSWORD": "test-trino-password", "DBT_S3_URL_STYLE": "path"})
    assert worker.ensure_nessie_branch(run_id, publisher_factory=FakePublisher()) == branch
    assert [o[0] for o in order] == ["get", "create"]
    ns_calls.clear(); order.clear()
    import logging as _logging
    with caplog.at_level(_logging.INFO, logger=worker.__name__):
        assert worker.run_once(queue=FakeQueue(), worker="w", branch_factory=FakePublisher()) is True
    assert finished.get("ok")
    assert _seen.get("persisted_branch") == branch
    assert ns_calls == [(branch, "s3://dp-ai-payment/warehouse")]
    assert _seen["cmd"].environment["DBT_NESSIE_BRANCH"] == branch
    assert _seen["cmd"].trino_environment["DBT_NESSIE_BRANCH"] == branch
    assert finished_args["a"][3] == "SUCCEEDED"
    assert finished_args["k"].get("test_status") == "PASSED"
    branch_recs = [r for r in caplog.records if "transform nessie branch" in r.getMessage()]
    assert branch_recs
    branch_msg = branch_recs[0].getMessage()
    for identifier in ("be_" + "d" * 32, run_id, branch):
        assert identifier in branch_msg
    for secret in ("test-trino-password", "NESSIE_TRANSFORM_TOKEN", "lease_token"):
        assert secret not in branch_msg
    recs = [r for r in caplog.records if "transform dbt output" in r.getMessage()]
    assert recs
    _msg = recs[0].getMessage()
    for _f in ("be_" + "d" * 32, "C4_ML_01", "SUCCEEDED", "return_code=0", "output_truncated=False", "dbt build ok"):
        assert _f in _msg

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
        def record_nessie_branch(self, eid, worker_id, token, branch_arg):
            return branch_arg
        def orphan_if_owned(self, eid, w, tok, failure):
            emitted["failure"] = failure
            return True
    monkeypatch.setenv("PLATFORM_RUNNER_EXECUTION_ENABLED", "true")
    monkeypatch.setenv("PLATFORM_JOB_EXECUTION_ENABLED", "true")
    monkeypatch.setenv("NESSIE_BASE_HASH", "")
    assert worker.run_once(queue=FakeQueue(), worker="w", branch_factory=Boom()) is True
    assert emitted["failure"].startswith("NESSIE_BRANCH_UNAVAILABLE")


def test_worker_namespaces_on_branch_never_main_and_staging_excluded(monkeypatch):
    worker = _worker(monkeypatch)
    from orchestration.transform_runtime.nessie_publication import NessiePublicationError
    for bad in ("main", "", "staging", "bronze", "transform_tr_short"):
        try:
            worker.ensure_nessie_namespaces(bad, "payments", publisher_factory=object())
        except NessiePublicationError:
            pass
        else:
            raise AssertionError("must refuse " + repr(bad))
    src = open(worker.__file__).read()
    assert 'ensure_nessie_namespaces("main"' not in src
    assert "ensure_nessie_namespaces('main'" not in src
    assert "first E2E validation targets main" not in src


def test_worker_surfaces_redacted_output_preserves_failed_state(monkeypatch, caplog):
    worker = _worker(monkeypatch)
    run_id = "tr_" + "c" * 32
    branch = "transform_" + run_id
    class _Pub:
        def get_reference(self, name):
            return {"name": name, "hash": "base-hash"}
        def create_run_branch(self, rid, base_ref, base_hash):
            return {"name": branch, "hash": "new-hash"}
        def ensure_namespaces(self, branch_arg, warehouse):
            assert branch_arg == branch
            return {"bronze", "silver", "silver_vault", "gold", "consumption"}
    done = {}
    class _Q:
        def reconcile_expired(self):
            pass
        def claim(self, w, lease):
            return {"batch_execution_id": "be_" + "d" * 32, "lease_token": "tok", "batch_id": "C4_ML_01", "transform_run_id": run_id}
        def record_nessie_branch(self, eid, worker_id, token, branch_arg):
            return branch_arg
        def orphan_if_owned(self, *a):
            raise AssertionError("must not orphan")
        def finish(self, *a, **k):
            done["a"] = a
            done["k"] = k
            return {}
    monkeypatch.setenv("PLATFORM_RUNNER_EXECUTION_ENABLED", "true")
    monkeypatch.setenv("PLATFORM_JOB_EXECUTION_ENABLED", "true")
    monkeypatch.setenv("NESSIE_BASE_HASH", "")
    monkeypatch.setenv("NESSIE_WAREHOUSE", "s3://dp-ai-payment/warehouse")
    redacted = "dbt done [REDACTED]"
    monkeypatch.setattr(worker, "execute", lambda cmd, **k: types.SimpleNamespace(status="FAILED", return_code=1, output=redacted, output_truncated=True, failure_class="DBT_BUILD_FAILED"))
    import transform_execution as texec
    monkeypatch.setattr(texec, "_ambient_environment", lambda: {"ML_TRANSFORM_S3_ACCESS_KEY_ID": "k", "ML_TRANSFORM_S3_SECRET_ACCESS_KEY": "s", "NESSIE_TRANSFORM_TOKEN": "t", "NESSIE_ENDPOINT": "http://nessie:19120", "S3_ENDPOINT": "http://minio:9000", "S3_USE_SSL": "false", "OBJECT_STORE_REGION": "us-east-1", "OBJECT_STORE_BUCKET": "dp-ai-payment", "RAW_ROOT": "raw", "RAW_VERSION": "v2", "RAW_PREFIX": "raw/v2", "WAREHOUSE_PREFIX": "warehouse", "WAREHOUSE_URI": "s3://dp-ai-payment/warehouse", "NESSIE_WAREHOUSE": "s3://dp-ai-payment/warehouse", "DBT_TRINO_PASSWORD": "test-trino-password", "DBT_S3_URL_STYLE": "path"})
    import logging as _logging
    with caplog.at_level(_logging.INFO, logger=worker.__name__):
        assert worker.run_once(queue=_Q(), worker="w", branch_factory=_Pub()) is True
    assert done["a"][3] == "FAILED"
    assert done["k"].get("test_status") == "NOT_RUN"
    recs = [r for r in caplog.records if "transform dbt output" in r.getMessage()]
    assert recs
    _logged = recs[0].getMessage()
    assert redacted in _logged and "output_truncated=True" in _logged
    for _s in ("NESSIE_TRANSFORM_TOKEN", "TRANSFORM_S3_SECRET", "MINIO_ROOT_PASSWORD"):
        assert _s not in _logged

def test_plugin_has_no_main_fallback():
    assert "main" not in PLUGIN_PATH.read_text().replace("remain", "")


def test_worker_orphans_without_execution_when_branch_persistence_fails(monkeypatch):
    worker = _worker(monkeypatch)
    from orchestration.transform_runtime.durable_queue import LeaseLost
    run_id = "tr_" + "8" * 32
    branch = "transform_" + run_id
    class Publisher:
        def get_reference(self, name):
            return {"name": name, "hash": "base-hash"}
        def create_run_branch(self, rid, base_ref, base_hash):
            return {"name": branch, "hash": "branch-hash"}
    orphaned = []
    class Queue:
        def reconcile_expired(self):
            pass
        def claim(self, worker_id, lease):
            return {"batch_execution_id": "be_" + "9" * 32, "lease_token": "token", "batch_id": "C4_ML_01", "transform_run_id": run_id}
        def record_nessie_branch(self, *args):
            raise LeaseLost("worker lease lost")
        def orphan_if_owned(self, eid, worker_id, token, failure):
            orphaned.append(failure)
            return False
    executed = []
    monkeypatch.setattr(worker, "execute", lambda *args, **kwargs: executed.append(True))
    monkeypatch.setenv("PLATFORM_RUNNER_EXECUTION_ENABLED", "true")
    monkeypatch.setenv("PLATFORM_JOB_EXECUTION_ENABLED", "true")
    monkeypatch.setenv("NESSIE_BASE_HASH", "")
    assert worker.run_once(queue=Queue(), worker="worker", branch_factory=Publisher()) is True
    assert orphaned == ["NESSIE_BRANCH_PERSISTENCE_FAILED:LeaseLost"]
    assert executed == []
