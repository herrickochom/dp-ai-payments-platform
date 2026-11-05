import sys, unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'orchestration/gcp_composer/runtime'))
from dataflow_provider import _normalise_state
from bigquery_live_operations import BigQueryLineageVerifier, EvidenceUnavailable
class RepairTests(unittest.TestCase):
    def test_enum_state(self):
        class Enum: name='JOB_STATE_DONE'
        self.assertEqual(_normalise_state(Enum()),'JOB_STATE_DONE')
    def test_unknown_state_not_done(self):
        self.assertEqual(_normalise_state(3),'JOB_STATE_UNKNOWN')
    def test_no_completion_from_unattested_rows(self):
        class Row(dict): pass
        class Job:
            def result(self): return [Row(event_id='e1',output_sha256='a'*64)]
        class Client:
            def query(self,*args,**kwargs): return Job()
        claim={'execution_id':'a'*64,'attempt_number':1,'digest':'b'*64}
        job={'state':'JOB_STATE_DONE','id':'j1'}
        with patch.dict(sys.modules, {'google.cloud.bigquery':type('BQ',(),{'QueryJobConfig':lambda **k:None,'ScalarQueryParameter':lambda *a:None})}):
            with self.assertRaises(EvidenceUnavailable):
                BigQueryLineageVerifier(Client(),project='proj',dataset='payments').verify(claim,job)
if __name__=='__main__': unittest.main()
