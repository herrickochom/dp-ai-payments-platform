import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / (
    "platform/gcp/kafka_pubsub_bridge/"
    "source_operational_runtime_v1.py"
)

class SubscriberResultReportingAcceptance(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        tree = ast.parse(RUNTIME.read_text())
        fn = next(
            n for n in tree.body
            if isinstance(n, ast.FunctionDef)
            and n.name == "run_subscriber"
        )
        cls.fn = fn

    def test_routing_result_is_captured(self):
        assignments = [
            n for n in ast.walk(self.fn)
            if isinstance(n, ast.Assign)
            and isinstance(n.value, ast.Call)
            and isinstance(n.value.func, ast.Name)
            and n.value.func.id == "route_pulled_message"
        ]
        self.assertEqual(len(assignments), 1)
        self.assertEqual(
            [ast.unparse(t) for t in assignments[0].targets],
            ["result"]
        )

    def test_unacknowledged_delivery_fails_closed(self):
        source = ast.unparse(self.fn)
        self.assertIn("if not result.acknowledged:", source)
        self.assertIn("subscriber delivery unacknowledged", source)
        self.assertIn("result.route", source)
        self.assertIn("result.reason", source)

    def test_counter_follows_acknowledgement_check(self):
        source = ast.unparse(self.fn)
        self.assertLess(
            source.index("if not result.acknowledged:"),
            source.index("done += 1")
        )
