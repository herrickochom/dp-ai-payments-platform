import unittest

from stage89_handoff_controller import (
    HandoffRequest,
    Stage89HandoffController,
)

from stage8_stage9_handoff import (
    HandoffConflict,
    HandoffFenced,
)


EID = "a" * 64
DIGEST = "b" * 64


def request(eid=EID, attempt=1):
    return HandoffRequest(
        execution_id=eid,
        attempt_number=attempt,
        worker="worker-1",
        lease_token="lease-1",
        job_name="pdm-stage89-test",
        project="test-project",
        region="europe-west2",
        spec_digest=DIGEST,
    )


class FakeHandoff:
    def __init__(self, decision="NEW", state="PREPARED", error=None):
        self.decision = decision
        self.state = state
        self.error = error
        self.calls = []

    def prepare(self, **kwargs):
        self.calls.append(kwargs)

        if self.error is not None:
            raise self.error

        return {
            "execution_id": kwargs["execution_id"],
            "attempt_number": kwargs["attempt_number"],
            "decision": self.decision,
            "state": self.state,
        }


class ControllerTests(unittest.TestCase):

    def controller(self, fake):
        return Stage89HandoffController(
            connection_factory=None,
            handoff=fake,
        )

    def test_new_handoff(self):
        fake = FakeHandoff()
        result = self.controller(fake).run_once(request())
        self.assertEqual(result.decision, "NEW")
        self.assertEqual(result.state, "PREPARED")

    def test_duplicate_handoff(self):
        fake = FakeHandoff(decision="DUPLICATE")
        result = self.controller(fake).run_once(request())
        self.assertEqual(result.decision, "DUPLICATE")

    def test_fenced_lease_propagates(self):
        fake = FakeHandoff(error=HandoffFenced("expired"))
        with self.assertRaises(HandoffFenced):
            self.controller(fake).run_once(request())

    def test_identity_conflict_propagates(self):
        fake = FakeHandoff(error=HandoffConflict("conflict"))
        with self.assertRaises(HandoffConflict):
            self.controller(fake).run_once(request())

    def test_invalid_request_type(self):
        fake = FakeHandoff()
        with self.assertRaises(TypeError):
            self.controller(fake).run_once({})

    def test_unexpected_decision_rejected(self):
        fake = FakeHandoff(decision="UNKNOWN")
        with self.assertRaises(RuntimeError):
            self.controller(fake).run_once(request())

    def test_unexpected_state_rejected(self):
        fake = FakeHandoff(state="UNKNOWN")
        with self.assertRaises(RuntimeError):
            self.controller(fake).run_once(request())

    def test_batch_success(self):
        fake = FakeHandoff()
        result = self.controller(fake).run_batch([
            request(EID, 1),
            request("c" * 64, 1),
        ])
        self.assertEqual(len(result), 2)
        self.assertEqual(len(fake.calls), 2)

    def test_batch_limit(self):
        fake = FakeHandoff()
        with self.assertRaises(ValueError):
            self.controller(fake).run_batch(
                [request(str(i).zfill(64), 1) for i in range(11)]
            )
        self.assertEqual(len(fake.calls), 0)

    def test_duplicate_batch_identity(self):
        fake = FakeHandoff()
        with self.assertRaises(ValueError):
            self.controller(fake).run_batch([
                request(),
                request(),
            ])
        self.assertEqual(len(fake.calls), 0)

    def test_invalid_batch_limit(self):
        fake = FakeHandoff()
        with self.assertRaises(ValueError):
            self.controller(fake).run_batch(
                [request()],
                max_items=11,
            )

    def test_empty_batch(self):
        fake = FakeHandoff()
        result = self.controller(fake).run_batch([])
        self.assertEqual(result, [])
        self.assertEqual(len(fake.calls), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
