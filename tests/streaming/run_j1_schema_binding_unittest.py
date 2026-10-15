"""Unittest bridge for the J1 schema-binding regression (no pytest required).

The Flink container has PyFlink 1.18.1 but no pytest, and must not gain test
dependencies. This file imports the SAME test module and runs the SAME
pytest-style test methods (every test_* method on every Test* class) through
unittest, so coverage is identical: same assertions, same order, no skips.
Exit code is 0 only when every discovered test passes.

Usage (inside the Flink container):
    python /tmp/run_j1_schema_binding_unittest.py -v
"""

import os
import sys
import types
import unittest

# The test module has a top-level `import pytest` for host-side runs. The
# Flink container must not gain pytest, so stub the name: no pytest API is
# used at import time (no fixtures, marks, or parametrize), and the bridge
# below executes the SAME test_* methods directly, so no assertion is lost.
sys.modules.setdefault("pytest", types.ModuleType("pytest"))

REPO_ROOT = "/home/hochom/projects/dp-ai-payments-platform"
J1_JOBS_DIR = os.path.join(REPO_ROOT, "platform", "flink", "jobs")
# Inside the container the repo is NOT mounted; the job lives baked-in at
# /opt/flink/jobs and the test files are copied to /tmp by the operator.
# Prefer /opt/flink/jobs when it holds the module so the bridge validates
# the deployed code, and also add /tmp (test-file location).
CONTAINER_JOBS_DIR = "/opt/flink/jobs"
TESTS_DIR = os.path.join(REPO_ROOT, "tests", "streaming")
for _p in (J1_JOBS_DIR, TESTS_DIR, "/tmp"):
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)
if os.path.isdir(CONTAINER_JOBS_DIR) and CONTAINER_JOBS_DIR not in sys.path:
    sys.path.insert(0, CONTAINER_JOBS_DIR)

import test_j1_schema_binding as _m  # noqa: E402


def _iter_tests(mod):
    for cls_name in sorted(dir(mod)):
        if not cls_name.startswith("Test"):
            continue
        cls = getattr(mod, cls_name)
        if not isinstance(cls, type):
            continue
        for meth_name in sorted(dir(cls)):
            if meth_name.startswith("test_"):
                yield cls_name, meth_name


class TestJ1SchemaBindingBridge(unittest.TestCase):
    pass


def _make(case_mod, cls_name, meth_name):
    cls = getattr(case_mod, cls_name)

    def _t(self):
        getattr(cls(), meth_name)()

    _t.__name__ = f"{cls_name}__{meth_name}"
    _t.__doc__ = f"{cls_name}.{meth_name} (same assertions as pytest file)"
    return _t


for _cls_name, _meth_name in _iter_tests(_m):
    setattr(
        TestJ1SchemaBindingBridge,
        f"test_{_cls_name}__{_meth_name}",
        _make(_m, _cls_name, _meth_name),
    )

print(f"discovered={len(list(_iter_tests(_m)))} pytest-equivalent tests")

if __name__ == "__main__":
    unittest.main(verbosity=2)
