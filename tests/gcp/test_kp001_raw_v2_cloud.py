import importlib.util
import pathlib
import unittest
from unittest.mock import patch

ROOT=pathlib.Path(__file__).resolve().parents[2]
P=ROOT/'platform/gcp/pubsub_raw_ingestion/raw_v2_cloud.py'
spec=importlib.util.spec_from_file_location('raw_v2_cloud',P)
cloud=importlib.util.module_from_spec(spec); spec.loader.exec_module(cloud)

class FakeBlob:
    def __init__(self): self.data=None; self.kwargs=None
    def upload_from_string(self,data,**kwargs):
        self.kwargs=kwargs
        if self.data is not None:
            raise FakePreconditionFailed('already exists')
        self.data=data
    def download_as_bytes(self): return self.data
class FakePreconditionFailed(Exception): pass
class FakeBucket:
    def __init__(self): self.objects={}
    def blob(self,name): return self.objects.setdefault(name,FakeBlob())
class FakeSubscriber:
    def __init__(self): self.calls=[]
    def acknowledge(self,request): self.calls.append(request)

class Tests(unittest.TestCase):
    def test_gcs_precondition_and_replay(self):
        import sys,types
        api=types.ModuleType('google.api_core.exceptions'); api.PreconditionFailed=FakePreconditionFailed
        core=types.ModuleType('google.api_core'); core.exceptions=api
        google=types.ModuleType('google'); google.api_core=core
        with patch.dict(sys.modules,{'google':google,'google.api_core':core,'google.api_core.exceptions':api}):
            bucket=FakeBucket(); store=cloud.GCSConditionalStore(bucket)
            self.assertTrue(store.create_if_absent('a',b'one'))
            self.assertEqual(bucket.blob('a').kwargs['if_generation_match'],0)
            self.assertFalse(store.create_if_absent('a',b'two'))
            self.assertEqual(store.read('a'),b'one')
    def test_ack_request(self):
        sub=FakeSubscriber(); cloud.PubSubAcknowledger(sub,'subscription').ack('id')
        self.assertEqual(sub.calls,[{'subscription':'subscription','ack_ids':['id']}])
    def test_failure_never_acks(self):
        sub=FakeSubscriber(); acker=cloud.PubSubAcknowledger(sub,'subscription')
        with self.assertRaises(cloud.raw.ContractError):
            cloud.process_message(b'not-json','id',FakeBucket(),acker,lambda w,t:{})
        self.assertEqual(sub.calls,[])

if __name__=='__main__': unittest.main()
