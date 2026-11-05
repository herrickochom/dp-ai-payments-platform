import unittest
from datetime import datetime, timedelta, timezone
from orchestration.gcp_composer.runtime.cost_guard import CostGateBlocked, require_approval

NOW=datetime(2026,10,29,12,tzinfo=timezone.utc)
BASE=dict(project='project-e48f194e-f44f-458d-b4f',region='europe-west2',workers=1,
          runtime_minutes=20,estimated_launch_gbp=1.0,reported_spend_gbp=2.0,
          spend_observed_at=NOW,approval=True,now=NOW)
class CostGateTests(unittest.TestCase):
    def test_planning_only(self):
        self.assertEqual(require_approval(**BASE)['decision'],'ALLOW_PLANNING_ONLY')
    def test_approval_required(self):
        with self.assertRaises(CostGateBlocked): require_approval(**(BASE|{'approval':False}))
    def test_workers_limited(self):
        with self.assertRaises(CostGateBlocked): require_approval(**(BASE|{'workers':2}))
    def test_runtime_limited(self):
        with self.assertRaises(CostGateBlocked): require_approval(**(BASE|{'runtime_minutes':31}))
    def test_stale_spend_blocked(self):
        with self.assertRaises(CostGateBlocked): require_approval(**(BASE|{'spend_observed_at':NOW-timedelta(hours=3)}))
    def test_budget_threshold(self):
        with self.assertRaises(CostGateBlocked): require_approval(**(BASE|{'reported_spend_gbp':14.5}))
    def test_missing_spend_blocked(self):
        with self.assertRaises(CostGateBlocked): require_approval(**(BASE|{'reported_spend_gbp':None}))
    def test_wrong_project(self):
        with self.assertRaises(CostGateBlocked): require_approval(**(BASE|{'project':'another-project'}))
    def test_estimate_ceiling(self):
        with self.assertRaises(CostGateBlocked): require_approval(**(BASE|{'estimated_launch_gbp':4.0}))
if __name__=='__main__': unittest.main()
