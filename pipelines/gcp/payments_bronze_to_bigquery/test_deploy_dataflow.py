"""Contract checks for the Dataflow submission launcher."""

import unittest

from deploy_dataflow import build_command


class DataflowDeploymentTests(unittest.TestCase):

    def setUp(self):
        self.command = build_command()
        self.args = " ".join(self.command)

    def test_runner(self):
        self.assertIn("--runner=DataflowRunner", self.command)

    def test_region(self):
        self.assertIn("--region=europe-west2", self.command)

    def test_project(self):
        self.assertIn(
            "--project=project-e48f194e-f44f-458d-b4f",
            self.command,
        )

    def test_dedicated_staging(self):
        self.assertIn(
            "/staging/",
            self.args,
        )

    def test_dedicated_temp(self):
        self.assertIn(
            "/temp/",
            self.args,
        )

    def test_worker_identity(self):
        self.assertIn(
            "pdm-dataflow-worker@",
            self.args,
        )

    def test_packaging(self):
        self.assertIn("--setup_file=", self.args)
        self.assertIn("--requirements_file=", self.args)

    def test_bounded_workers(self):
        self.assertIn("--max_num_workers=2", self.command)
        self.assertIn("--num_workers=1", self.command)


if __name__ == "__main__":
    unittest.main()
