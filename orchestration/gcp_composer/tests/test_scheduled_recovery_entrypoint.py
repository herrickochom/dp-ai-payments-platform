import os
import sys
import types
import unittest
from unittest.mock import patch

from scheduled_recovery_entrypoint import (
    SchedulerConfigurationError,
    load_callable,
    run_scheduled_handoff,
)


class ScheduledEntrypointTests(unittest.TestCase):

    def test_disabled_by_default(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(
                SchedulerConfigurationError
            ):
                run_scheduled_handoff()

    def test_missing_connection_factory(self):
        with patch.dict(
            os.environ,
            {"PDM_STAGE89_ENABLED": "true"},
            clear=True,
        ):
            with self.assertRaises(
                SchedulerConfigurationError
            ):
                run_scheduled_handoff()

    def test_invalid_callable_reference(self):
        with patch.dict(
            os.environ,
            {"PDM_STAGE89_CLAIM_SOURCE": "invalid"},
            clear=True,
        ):
            with self.assertRaises(
                SchedulerConfigurationError
            ):
                load_callable("PDM_STAGE89_CLAIM_SOURCE")

    def test_empty_tick(self):
        module = types.ModuleType("_stage89_test_provider")
        module.connection = lambda: None
        module.claims = lambda: []

        with patch.dict(
            sys.modules,
            {"_stage89_test_provider": module},
        ):
            with patch.dict(
                os.environ,
                {
                    "PDM_STAGE89_ENABLED": "true",
                    "PDM_STAGE89_CONNECTION_FACTORY":
                        "_stage89_test_provider:connection",
                    "PDM_STAGE89_CLAIM_SOURCE":
                        "_stage89_test_provider:claims",
                },
                clear=True,
            ):
                result = run_scheduled_handoff()

        self.assertEqual(result["processed"], 0)
        self.assertEqual(result["new"], 0)
        self.assertEqual(result["duplicate"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
