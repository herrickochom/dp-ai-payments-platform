import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]
P = ROOT / 'pipelines/gcp/raw_to_bronze/pipeline_orchestration.py'
spec = importlib.util.spec_from_file_location('pipeline_orchestration', P)
mod = importlib.util.module_from_spec(spec)
import sys
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def row(offset, **extra):
    return {'_kafka_topic': 'payments.test', '_kafka_partition': 0,
            '_kafka_offset': offset, **extra}


class Stage14E4Tests(unittest.TestCase):
    def plan(self, **changes):
        args = dict(run_id='test-run-1', input_count=3, quarantine_count=1,
                    accepted_rows=[row(1), row(2)], staging_rows=[row(1), row(2)],
                    existing_identities=[('payments.test', 0, 1)],
                    inserted_count=1, final_bronze_count=2)
        args.update(changes)
        return mod.verify_plan(**args)

    def test_balanced_run(self):
        self.assertEqual(self.plan().inserted_count, 1)

    def test_retry_no_new_inserts(self):
        p = self.plan(existing_identities=[('payments.test', 0, 1), ('payments.test', 0, 2)],
                      inserted_count=0, final_bronze_count=2)
        self.assertEqual(p.inserted_count, 0)

    def test_quarantine_count_mismatch_fails(self):
        with self.assertRaises(mod.ContractError): self.plan(quarantine_count=0)

    def test_staging_missing_identity_fails(self):
        with self.assertRaises(mod.ContractError): self.plan(staging_rows=[row(1)])

    def test_conflicting_duplicate_fails(self):
        with self.assertRaises(mod.ContractError):
            self.plan(input_count=4, accepted_rows=[row(1), row(1, payload='changed'), row(2)])

    def test_identical_duplicate_accounted(self):
        p = self.plan(input_count=4, accepted_rows=[row(1), row(1), row(2)])
        self.assertEqual(p.duplicate_count, 1)

    def test_merge_mismatch_fails(self):
        with self.assertRaises(mod.ContractError): self.plan(inserted_count=2)

    def test_final_sink_mismatch_fails(self):
        with self.assertRaises(mod.ContractError): self.plan(final_bronze_count=3)

    def test_duplicate_run_id_fails(self):
        with self.assertRaises(mod.ContractError): mod.verify_distinct_run_ids(['run1', 'run1'])

    def test_bad_kafka_identity_fails(self):
        with self.assertRaises(mod.ContractError): self.plan(staging_rows=[row(-1), row(2)])

    def test_negative_or_bool_count_fails(self):
        for count in (-1, True):
            with self.assertRaises(mod.ContractError): self.plan(inserted_count=count)


if __name__ == '__main__': unittest.main()
