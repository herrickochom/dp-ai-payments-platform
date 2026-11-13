"""GCP-native runtime must not import local/on-prem execution engines."""
import ast
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[2]
FORBIDDEN = ("trino", "duckdb", "nessie", "minio", "orchestration.job_runner")

class GcpRuntimeBoundary(unittest.TestCase):
    def test_native_runtime_imports(self):
        problems = []
        for base in (ROOT / "platform/gcp", ROOT / "orchestration/gcp_composer"):
            for path in sorted(base.rglob("*.py")):
                if "tests" in path.parts or path.name.startswith("test_"):
                    continue
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
                for node in ast.walk(tree):
                    modules = ([alias.name for alias in node.names] if isinstance(node, ast.Import)
                               else [node.module or ""] if isinstance(node, ast.ImportFrom) else [])
                    for module in modules:
                        if any(module == bad or module.startswith(bad + ".") or
                               ("." + bad + ".") in ("." + module + ".") for bad in FORBIDDEN):
                            problems.append(f"{path.relative_to(ROOT)}:{node.lineno}: {module}")
                for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                    if "/app/job_runner/" in line and not line.lstrip().startswith("#"):
                        problems.append(f"{path.relative_to(ROOT)}:{number}: legacy job runner")
        self.assertFalse(problems, "\n".join(problems))

    def test_gcp_source_compose_excludes_onprem_engines(self):
        content = (ROOT / "docker-compose.yaml").read_text(encoding="utf-8")
        for service in ("trino", "minio", "nessie", "duckdb", "superset"):
            self.assertIsNone(re.search(r"^  " + service + r":\s*$", content, re.M), service)

if __name__ == "__main__":
    unittest.main()
