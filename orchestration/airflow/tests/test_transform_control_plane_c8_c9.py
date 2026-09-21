from pathlib import Path
import pytest
from orchestration.job_runner import transform_execution
from orchestration.transform_runtime.nessie_publication import NessiePublisher,NessieNotFound,NessieConflict,branch_for_run
RUN='tr_'+'b'*32; EXEC='be_'+'a'*32
def creds(): return {'ML_S3_ACCESS_KEY_ID':'ml','ML_S3_SECRET_ACCESS_KEY':'secret','NESSIE_AUTH_TOKEN':'token','S3_ENDPOINT':'minio:9000','DBT_S3_URL_STYLE':'path','DBT_DATABASE':'lakehouse','NESSIE_ENDPOINT':'http://nessie:19120'}

def test_run_scoped_nessie_reference_is_consumed():
    c=transform_execution.build_transform_command('C4_ML_01',EXEC,creds(),transform_run_id=RUN)
    assert c.environment['DBT_NESSIE_BRANCH']==branch_for_run(RUN)
    assert "/iceberg/{{ env_var('DBT_NESSIE_BRANCH') }}" in Path('transform/dbt/profiles.yml').read_text()

def test_no_sync_dispatch_and_queue_is_fenced():
    assert 'dispatch_to_runner' not in Path('orchestration/transform_runtime/api.py').read_text()
    q=Path('orchestration/transform_runtime/durable_queue.py').read_text()
    assert 'FOR UPDATE SKIP LOCKED' in q and 'lease_token' in q and 'clock_timestamp()' in q
    assert "WHERE status='QUEUED'" in q and "status='ORPHANED'" in q

def test_worker_lease_loss_is_orphaned_not_operator_cancel():
    w=Path('orchestration/job_runner/transform_worker.py').read_text()
    assert 'WORKER_LEASE_LOST_REQUIRES_RECONCILIATION' in w and 'lease_lost' in w

def test_publication_remains_fail_closed():
    m=Path('transform/dbt/macros/iceberg_table.sql').read_text().lower()
    assert 'drop table' not in m and 'unsafe iceberg replacement blocked' in m

def test_branch_creation_only_treats_404_as_missing(monkeypatch):
    p=NessiePublisher('http://nessie:19120','token'); calls=[]
    def fake(method,path,payload=None):
        calls.append((method,path,payload))
        if method=='GET': raise NessieNotFound('missing')
        return {'reference':{'name':branch_for_run(RUN),'hash':'base1'}}
    monkeypatch.setattr(p,'_request',fake)
    r=p.create_run_branch(RUN,'main','base1')
    assert r['hash']=='base1'
    assert calls[-1][2]=={'type':'BRANCH','name':'main','hash':'base1'}

def test_existing_branch_must_match_recorded_base(monkeypatch):
    p=NessiePublisher('http://nessie:19120','token')
    monkeypatch.setattr(p,'get_reference',lambda name:{'name':name,'hash':'different'})
    with pytest.raises(NessieConflict): p.create_run_branch(RUN,'main','base1')

def test_promotion_rejects_target_drift(monkeypatch):
    p=NessiePublisher('http://nessie:19120','token')
    def ref(name):
        return {'name':name,'hash':'source2' if name==branch_for_run(RUN) else 'moved'}
    monkeypatch.setattr(p,'get_reference',ref)
    with pytest.raises(NessieConflict): p.promote_run(RUN,'main','base1')

def test_verified_nessie_v2_merge_shape(monkeypatch):
    p=NessiePublisher('http://nessie:19120','token'); calls=[]
    def ref(name): return {'name':name,'hash':'source2' if name==branch_for_run(RUN) else 'base1'}
    monkeypatch.setattr(p,'get_reference',ref)
    monkeypatch.setattr(p,'_request',lambda method,path,payload=None:(calls.append((method,path,payload)) or {'wasApplied':True}))
    p.promote_run(RUN,'main','base1',expected_source_hash='source2')
    assert 'main%40base1' in calls[-1][1]
    assert calls[-1][2]=={'fromRefName':branch_for_run(RUN),'fromHash':'source2'}

def test_gates_false():
    c=Path('docker-compose.yaml').read_text()
    assert all(x in c for x in ["LAKEHOUSE_TRANSFORM_EXECUTION_ENABLED: 'false'","PLATFORM_RUNNER_EXECUTION_ENABLED: 'false'","PLATFORM_JOB_EXECUTION_ENABLED: 'false'"])
