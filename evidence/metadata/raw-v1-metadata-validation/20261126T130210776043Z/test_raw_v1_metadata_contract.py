
import base64,hashlib,importlib.util,os
from pathlib import Path
from collections import Counter
import pytest
root=Path(os.environ['METADATA_TEST_ROOT'])
spec=importlib.util.spec_from_file_location('metadata',os.environ['METADATA_CANDIDATE'])
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
c=m.MetadataContract(root)
def fixture(topic):
    r=c.routes[topic]; wire=b'\x00\x00\x00\x00\x01x'
    e=dict(message_type='payments.source-event.v1',kafka_topic=topic,kafka_partition=0,kafka_offset=0,kafka_timestamp_ms=0,payload_base64=base64.b64encode(wire).decode(),payload_sha256=hashlib.sha256(wire).hexdigest(),schema_registry_id=1)
    p=f"raw/v1/{r['processing_mode']}/category={r.get('category','blocked')}/source_group={r.get('source_group','blocked')}/source_system={r.get('source_system','blocked')}/year=1970/month=01/day=01/topic={topic}/partition=0/offset=0/record.avro"
    return p,e
@pytest.mark.parametrize('topic',sorted(c.routes))
def test_topic(topic):
    assert c.classify_topic(topic) in c.contract['enterprise_domain_taxonomy'].values()
    p,e=fixture(topic)
    if c.routes[topic]['status']=='legacy_payment_event': assert len(c.derive(p,e))==11
    else:
        with pytest.raises(ValueError): c.derive(p,e)
def test_domains():
    assert Counter(map(c.classify_topic,c.routes))==dict(payments=19,programme_management=7,master_data_management=7,agent_management=3,platform_operations=2)
@pytest.mark.parametrize('old,new',[('day=01','day=32'),('offset=0','offset=1'),('partition=0','partition=4'),('category=agent_network','category=mdm'),('streaming','batch'),('source_system=agent','source_system=mtn')])
def test_path(old,new):
    p,e=fixture('agent.locations')
    with pytest.raises(ValueError): c.derive(p.replace(old,new),e)
def test_digest():
    p,e=fixture('agent.locations');e['payload_sha256']='bad'
    with pytest.raises(ValueError): c.derive(p,e)
def before():
    return dict(generation=1,metageneration=1,crc32c='x',md5_hash='y',size=3,content_type='application/avro',content_encoding=None,content_disposition=None,content_language=None,custom_time=None,metadata=None,cache_control=None)
@pytest.mark.parametrize('failure',[None,'generation','metageneration','payload','verification'])
def test_update(failure):
    b=before(); p,e=fixture('agent.locations'); proposed=c.derive(p,e)
    class Blob:
        def patch(self,**kw):
            assert kw==dict(if_generation_match=1,if_metageneration_match=1)
            if failure in ('generation','metageneration'): raise RuntimeError('precondition')
    blob=Blob()
    def fresh(): return dict(b,metadata=blob.metadata,cache_control=blob.cache_control,metageneration=2,generation=2 if failure=='verification' else 1)
    def payload(s): return b'bad' if failure=='payload' and s['metageneration']==2 else b'abc'
    if failure:
        with pytest.raises(RuntimeError): m.apply_metadata_update(blob,b,proposed,read_fresh=fresh,read_bytes=payload)
    else:
        assert m.apply_metadata_update(blob,b,proposed,read_fresh=fresh,read_bytes=payload)=='PASS'
        assert m.apply_metadata_update(blob,fresh(),proposed,read_fresh=fresh,read_bytes=payload)=='NO_CHANGE_REQUIRED'
def test_conflict():
    b=before(); b['metadata']={'data-domain':'payments'}; p,e=fixture('agent.locations')
    with pytest.raises(ValueError): m.plan_metadata_update(b,c.derive(p,e))
