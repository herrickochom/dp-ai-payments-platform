import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'platform/gcp/payment_lineage'))
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'orchestration/gcp_composer/runtime'))
from beam_payment_lineage import make_rows,digest_payment
from billing_export import read_export_cost,BillingExportUnavailable
class Tests(unittest.TestCase):
 def test_valid_pair(self):
  output,lineage=make_rows({'event_id':'synthetic-1','amount_minor':12,'synthetic':True},execution_id='a'*64,spec_digest='b'*64,attempt=1,job_id='job1')
  self.assertEqual(lineage['output_sha256'],digest_payment(output))
 def test_non_synthetic_rejected(self):
  with self.assertRaises(ValueError):make_rows({'event_id':'e','amount_minor':1,'synthetic':False},execution_id='a'*64,spec_digest='b'*64,attempt=1,job_id='job1')
 def test_invalid_digest_rejected(self):
  with self.assertRaises(ValueError):make_rows({'event_id':'e','amount_minor':1,'synthetic':True},execution_id='x',spec_digest='b'*64,attempt=1,job_id='job1')
 def test_billing_fail_closed(self):
  with self.assertRaises(BillingExportUnavailable):read_export_cost(None,billing_project='bad',export_table='a.b.c',month_start=None,month_end=None)
if __name__=='__main__':unittest.main()
