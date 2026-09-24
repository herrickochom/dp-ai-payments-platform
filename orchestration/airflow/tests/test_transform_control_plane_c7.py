import asyncio, importlib, io, sys
from pathlib import Path
import httpx
import anyio.to_thread
import pytest
from orchestration.job_runner import transform_execution

JOB_RUNNER = str(Path('orchestration/job_runner').resolve())
if JOB_RUNNER not in sys.path:
    sys.path.insert(0, JOB_RUNNER)

EXEC='be_'+'a'*32

@pytest.fixture(autouse=True)
def working_test_threadpool(monkeypatch):
    # The local Python 3.14/AnyIO worker queue stalls on even a trivial
    # synchronous FastAPI route. These requests have no live/blocking work;
    # keep the ASGI request and auth path intact while avoiding that executor.
    async def run_sync(func, *args, **kwargs):
        return func(*args)
    monkeypatch.setattr(anyio.to_thread, 'run_sync', run_sync)

def post(app, path, **kwargs):
    async def request():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url='http://testserver',
        ) as client:
            return await client.post(path, **kwargs)
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(request())
    finally:
        loop.close()

def creds():
    return {'ML_S3_ACCESS_KEY_ID':'ml','ML_S3_SECRET_ACCESS_KEY':'secret','NESSIE_TRANSFORM_TOKEN':'token','S3_ENDPOINT':'http://minio:9000','S3_USE_SSL':'false','OBJECT_STORE_REGION':'us-east-1','OBJECT_STORE_BUCKET':'dp-ai-payment','RAW_ROOT':'raw','RAW_VERSION':'v2','RAW_PREFIX':'raw/v2','WAREHOUSE_PREFIX':'warehouse','WAREHOUSE_URI':'s3://dp-ai-payment/warehouse','S3_PATH_STYLE_ACCESS':'true','DBT_S3_URL_STYLE':'path','DBT_DATABASE':'lakehouse','DBT_STAGING_DATABASE':'staging','DBT_BRONZE_DATABASE':'bronze','DBT_SILVER_DATABASE':'silver','DBT_SILVER_VAULT_DATABASE':'silver_vault','DBT_GOLD_DATABASE':'gold','DBT_CONSUMPTION_DATABASE':'consumption','WAREHOUSE_BUCKET':'warehouse','ICEBERG_CATALOG':'nessie','NESSIE_ENDPOINT':'http://nessie:19120'}

def test_runner_requires_service_auth(monkeypatch):
    monkeypatch.setenv('TRANSFORM_RUNTIME_RUNNER_TOKEN','runner-secret')
    import orchestration.job_runner.api as api
    api=importlib.reload(api)
    r=post(api.app,'/v1/transforms',json={'batch_execution_id':EXEC,'batch_id':'C4_ML_01','model_fingerprint':'0'*64})
    assert r.status_code==401

def test_runner_rejects_fingerprint_mismatch(monkeypatch):
    monkeypatch.setenv('TRANSFORM_RUNTIME_RUNNER_TOKEN','runner-secret')
    import orchestration.job_runner.api as api
    api=importlib.reload(api)
    r=post(api.app,'/v1/transforms',json={'batch_execution_id':EXEC,'batch_id':'C4_ML_01','model_fingerprint':'0'*64},headers={'Authorization':'Bearer runner-secret'})
    assert r.status_code==409

def test_known_dbt_failure_is_not_orphaned(monkeypatch):
    monkeypatch.setenv('TRANSFORM_RUNTIME_RUNNER_TOKEN','runner-secret')
    monkeypatch.setenv('PLATFORM_RUNNER_EXECUTION_ENABLED','true')
    monkeypatch.setenv('PLATFORM_JOB_EXECUTION_ENABLED','true')
    import orchestration.job_runner.api as api
    api=importlib.reload(api)
    cmd=transform_execution.build_transform_command('C4_ML_01',EXEC,creds())
    monkeypatch.setattr(api,'build_transform_command',lambda *a,**k:cmd)
    monkeypatch.setattr(api,'execute',lambda *a,**k:transform_execution.ProcessResult('FAILED',2,'',False,'DBT_BUILD_FAILED'))
    r=post(api.app,'/v1/transforms',json={'batch_execution_id':EXEC,'batch_id':'C4_ML_01','model_fingerprint':cmd.model_fingerprint},headers={'Authorization':'Bearer runner-secret'})
    assert r.status_code==200 and r.json()['status']=='FAILED'

def test_timeout_is_orphaned_output_bounded_and_redacted(monkeypatch):
    cmd=transform_execution.build_transform_command('C4_ML_01',EXEC,creds())
    class P:
        pid=123; returncode=None
        stdout=io.BytesIO(b'token=supersecret\n'+b'x'*100)
        def poll(self): return self.returncode
        def wait(self,timeout=None): self.returncode=-15; return -15
    p=P(); monkeypatch.setattr(transform_execution.subprocess,'Popen',lambda *a,**k:p)
    monkeypatch.setattr(transform_execution.os,'killpg',lambda *a,**k:None)
    values=iter([0.0,2.0,2.0]); monkeypatch.setattr(transform_execution.time,'monotonic',lambda:next(values))
    monkeypatch.setattr(transform_execution.time,'sleep',lambda _:None)
    result=transform_execution.execute(cmd,timeout_seconds=1,termination_grace_seconds=1,output_cap_bytes=32)
    assert result.status=='ORPHANED'
    assert result.failure_class=='TIMEOUT_REQUIRES_RECONCILIATION'
    assert result.output_truncated
    assert 'supersecret' not in result.output

def test_cancel_is_cancelled(monkeypatch):
    cmd=transform_execution.build_transform_command('C4_ML_01',EXEC,creds())
    class P:
        pid=123; returncode=None; stdout=io.BytesIO(b'')
        def poll(self): return self.returncode
        def wait(self,timeout=None): self.returncode=-15; return -15
    p=P(); signals=[]
    monkeypatch.setattr(transform_execution.subprocess,'Popen',lambda *a,**k:p)
    monkeypatch.setattr(transform_execution.os,'killpg',lambda pid,sig:signals.append(sig))
    result=transform_execution.execute(cmd,timeout_seconds=10,termination_grace_seconds=1,output_cap_bytes=64,cancel_requested=lambda:True)
    assert result.status=='CANCELLED' and signals

def test_deployment_controls_are_wired():
    c=Path('docker-compose.yaml').read_text()
    assert 'transform-ledger-migrate:' in c
    assert 'service_completed_successfully' in c
    assert 'airflow pools set transform_execution 1' in c
    assert '/var/lib/platform-job-runner/work:uid=50001' in c
    assert './data/pdmis_ml:/app/data/pdmis_ml:ro' in c

def test_publication_remains_fail_closed():
    m=Path('transform/dbt/macros/iceberg_table.sql').read_text().lower()
    assert 'drop table' not in m
    assert 'unsafe iceberg replacement blocked' in m


def test_child_environment_maps_only_selected_authority_credentials():
    source=creds() | {
        'ORDINARY_S3_ACCESS_KEY_ID':'ordinary', 'ORDINARY_S3_SECRET_ACCESS_KEY':'ordinary-secret',
        'RESTRICTED_S3_ACCESS_KEY_ID':'restricted', 'RESTRICTED_S3_SECRET_ACCESS_KEY':'restricted-secret',
        'MINIO_ROOT_USER':'root', 'MINIO_ROOT_PASSWORD':'root-secret',
        'PLATFORM_RAW_READ_ACCESS_KEY':'raw', 'PLATFORM_RAW_READ_SECRET_KEY':'raw-secret',
    }
    cmd=transform_execution.build_transform_command('C4_ML_01',EXEC,source)
    env=cmd.environment
    assert env['TRANSFORM_S3_ACCESS_KEY_ID']=='ml'
    assert env['TRANSFORM_S3_SECRET_ACCESS_KEY']=='secret'
    assert env['NESSIE_TRANSFORM_TOKEN']=='token'
    for forbidden in ('ML_S3_ACCESS_KEY_ID','ML_S3_SECRET_ACCESS_KEY','ORDINARY_S3_ACCESS_KEY_ID','ORDINARY_S3_SECRET_ACCESS_KEY','RESTRICTED_S3_ACCESS_KEY_ID','RESTRICTED_S3_SECRET_ACCESS_KEY','MINIO_ROOT_USER','MINIO_ROOT_PASSWORD','PLATFORM_RAW_READ_ACCESS_KEY','PLATFORM_RAW_READ_SECRET_KEY','AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY'):
        assert forbidden not in env

def test_migration_set_has_one_version_two_owner():
    migrations=Path('orchestration/transform_runtime/migrations')
    owners=[p for p in migrations.glob('*.sql') if 'VALUES (2)' in p.read_text() or 'VALUES(2)' in p.read_text()]
    assert [p.name for p in owners]==['002_transform_execution_control.sql']
