import unittest
from plan import CONTRACT

class ContractTests(unittest.TestCase):
    def test_no_endpoint_assumption(self): self.assertIn('UNVERIFIED',CONTRACT['catalogue_endpoint'])
    def test_no_commit_authorization(self): self.assertFalse(CONTRACT['commit_authorized'])
    def test_isolated_prefix(self): self.assertEqual(CONTRACT['proposed_storage_prefix'],'gs://dp-pdm-ai-platform-dev/iceberg/kp014/')
    def test_namespace(self): self.assertEqual(CONTRACT['namespace'],'payments_pilot')

if __name__=='__main__': unittest.main()
