import importlib.util
import pathlib
import unittest
from google.api_core.exceptions import PreconditionFailed

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / 'platform/gcp/pubsub_raw_ingestion/gcs_adapter.py'
spec = importlib.util.spec_from_file_location('kp001w_gcs_adapter', PATH)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

class Blob:
    def __init__(self, bucket, name): self.bucket, self.name = bucket, name
    def upload_from_string(self, data, **kw):
        self.bucket.calls.append(kw)
        if self.bucket.fail_upload: raise OSError('unavailable')
        if self.name in self.bucket.objects: raise PreconditionFailed('exists')
        self.bucket.objects[self.name] = data
    def download_as_bytes(self):
        if self.bucket.fail_read: raise OSError('unavailable')
        return self.bucket.objects[self.name]

class Bucket:
    def __init__(self):
        self.objects={}; self.calls=[]; self.fail_upload=False; self.fail_read=False
    def blob(self,name): return Blob(self,name)

class AdapterTests(unittest.TestCase):
    def setUp(self): self.bucket=Bucket(); self.store=module.GCSImmutableStore(self.bucket)
    def test_new_object(self):
        self.assertTrue(self.store.create_if_absent('a',b'one'))
        self.assertEqual(self.store.read('a'),b'one')
    def test_generation_precondition(self):
        self.store.create_if_absent('a',b'one')
        self.assertEqual(self.bucket.calls[0]['if_generation_match'],0)
        self.assertIsNone(self.bucket.calls[0]['retry'])
    def test_duplicate_is_not_overwritten(self):
        self.store.create_if_absent('a',b'one')
        self.assertFalse(self.store.create_if_absent('a',b'two'))
        self.assertEqual(self.store.read('a'),b'one')
    def test_write_error_propagates(self):
        self.bucket.fail_upload=True
        with self.assertRaises(OSError): self.store.create_if_absent('a',b'one')
    def test_read_error_propagates(self):
        self.bucket.objects['a']=b'one'; self.bucket.fail_read=True
        with self.assertRaises(OSError): self.store.read('a')

if __name__ == '__main__': unittest.main()
