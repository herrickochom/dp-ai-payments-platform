import unittest

from stage89_scheduler_integration import (
    Stage89SchedulerIntegration,
)

from stage89_handoff_controller import (
    HandoffRequest,
    Stage89HandoffController,
)

from stage8_stage9_handoff import HandoffFenced


def request(n=1):
    return HandoffRequest(
        execution_id=f"{n:064x}",
        attempt_number=1,
        worker="test-worker",
        lease_token="test-token",
        job_name=f"test-job-{n}",
        project="test-project",
        region="europe-west2",
        spec_digest="b" * 64,
    )


class FakeHandoff:
    def __init__(self):
        self.seen = set()
        self.calls = 0
        self.fenced = False

    def prepare(self, **kwargs):
        self.calls += 1

        if self.fenced:
            raise HandoffFenced("expired lease")

        key = (
            kwargs["execution_id"],
            kwargs["attempt_number"],
        )

        decision = (
            "DUPLICATE" if key in self.seen else "NEW"
        )

        self.seen.add(key)

        return {
            "execution_id": key[0],
            "attempt_number": key[1],
            "decision": decision,
            "state": "PREPARED",
        }


class IntegrationTests(unittest.TestCase):

    def make_runner(self, source, fake=None):
        fake = fake or FakeHandoff()

        controller = Stage89HandoffController(
            None,
            handoff=fake,
        )

        return (
            Stage89SchedulerIntegration(
                None,
                claim_source=source,
                controller=controller,
            ),
            fake,
        )

    def test_new_claim(self):
        runner, fake = self.make_runner(
            lambda: [request()]
        )
        result = runner.run_once()
        self.assertEqual(result[0].decision, "NEW")
        self.assertEqual(fake.calls, 1)

    def test_repeated_claim(self):
        runner, fake = self.make_runner(
            lambda: [request()]
        )
        self.assertEqual(
            runner.run_once()[0].decision,
            "NEW",
        )
        self.assertEqual(
            runner.run_once()[0].decision,
            "DUPLICATE",
        )

    def test_empty_claims(self):
        runner, fake = self.make_runner(lambda: [])
        self.assertEqual(runner.run_once(), [])
        self.assertEqual(fake.calls, 0)

    def test_batch_bound(self):
        runner, fake = self.make_runner(
            lambda: [request(i) for i in range(11)]
        )
        with self.assertRaises(ValueError):
            runner.run_once()
        self.assertEqual(fake.calls, 0)

    def test_duplicate_claims(self):
        runner, fake = self.make_runner(
            lambda: [request(), request()]
        )
        with self.assertRaises(ValueError):
            runner.run_once()
        self.assertEqual(fake.calls, 0)

    def test_fenced_claim(self):
        fake = FakeHandoff()
        fake.fenced = True
        runner, _ = self.make_runner(
            lambda: [request()],
            fake=fake,
        )
        with self.assertRaises(HandoffFenced):
            runner.run_once()

    def test_invalid_source(self):
        with self.assertRaises(TypeError):
            Stage89SchedulerIntegration(
                None,
                claim_source=None,
            )

    def test_invalid_claim(self):
        runner, _ = self.make_runner(
            lambda: [{"execution_id": "bad"}]
        )
        with self.assertRaises(TypeError):
            runner.run_once()


if __name__ == "__main__":
    unittest.main(verbosity=2)
