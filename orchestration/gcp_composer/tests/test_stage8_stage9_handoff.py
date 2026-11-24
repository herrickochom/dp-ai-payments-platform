import unittest

from stage8_stage9_handoff import (
    Stage89Handoff,
    HandoffError,
    HandoffFenced,
    HandoffConflict,
)


EID = "a" * 64
DIGEST = "b" * 64


class Cursor:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class FakeDB:
    autocommit = False

    def __init__(self, rows, autocommit=False):
        self.rows = iter(rows)
        self.calls = []
        self.autocommit = autocommit
        self.committed = False
        self.rolled_back = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.committed = True
        else:
            self.rolled_back = True
        return False

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        return Cursor(next(self.rows))


def invoke(db, **overrides):
    args = dict(
        execution_id=EID,
        attempt_number=1,
        worker="worker-1",
        lease_token="lease-1",
        job_name="pdm-test-job",
        project="test-project",
        region="europe-west2",
        spec_digest=DIGEST,
    )
    args.update(overrides)
    return Stage89Handoff(lambda: db).prepare(**args)


class HandoffTests(unittest.TestCase):
    def test_new_handoff(self):
        db = FakeDB([(EID, 1), None, (EID,)])
        result = invoke(db)

        self.assertEqual(result["decision"], "NEW")
        self.assertEqual(result["state"], "PREPARED")
        self.assertTrue(db.committed)
        self.assertIn("FOR UPDATE", db.calls[0][0])
        self.assertIn("INSERT INTO", db.calls[2][0])

    def test_duplicate_handoff(self):
        db = FakeDB([
            (EID, 1),
            ("pdm-test-job", "test-project",
             "europe-west2", DIGEST, "PREPARED"),
        ])
        result = invoke(db)

        self.assertEqual(result["decision"], "DUPLICATE")
        self.assertEqual(len(db.calls), 2)

    def test_conflicting_identity(self):
        db = FakeDB([
            (EID, 1),
            ("other-job", "test-project",
             "europe-west2", DIGEST, "PREPARED"),
        ])
        with self.assertRaises(HandoffConflict):
            invoke(db)
        self.assertTrue(db.rolled_back)

    def test_expired_lease(self):
        db = FakeDB([None])
        with self.assertRaises(HandoffFenced):
            invoke(db)
        self.assertEqual(len(db.calls), 1)
        self.assertTrue(db.rolled_back)

    def test_duplicate_job_name(self):
        db = FakeDB([(EID, 1), None, None])
        with self.assertRaises(HandoffConflict):
            invoke(db)
        self.assertTrue(db.rolled_back)

    def test_invalid_attempt(self):
        db = FakeDB([])
        with self.assertRaises(ValueError):
            invoke(db, attempt_number=0)
        self.assertFalse(db.calls)

    def test_invalid_digest(self):
        db = FakeDB([])
        with self.assertRaises(ValueError):
            invoke(db, spec_digest="invalid")
        self.assertFalse(db.calls)

    def test_autocommit_rejected(self):
        db = FakeDB([], autocommit=True)
        with self.assertRaises(HandoffError):
            invoke(db)
        self.assertFalse(db.calls)

    def test_lease_fencing_sql(self):
        db = FakeDB([None])
        with self.assertRaises(HandoffFenced):
            invoke(db)

        sql = db.calls[0][0]
        self.assertIn("lease_token=%s", sql)
        self.assertIn("attempts=%s", sql)
        self.assertIn("clock_timestamp()", sql)
        self.assertIn("FOR UPDATE", sql)

    def test_no_external_submission(self):
        db = FakeDB([(EID, 1), None, (EID,)])
        invoke(db)
        self.assertEqual(len(db.calls), 3)
        self.assertTrue(
            all("gcp_" in sql for sql, _ in db.calls)
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
