"""Test inventory and last-known results.

A test file existing in the repository proves a suite was written. It does
NOT prove the suite passes. This collector therefore records two distinct
facts per suite:

* discovery  - the suite file exists (technical_status DISCOVERED)
* last run   - a recorded result, when one is committed in the repository

No test suite is executed by the updater. Running the suite is a separate,
explicit action because test execution is a platform workload.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Iterable
from xml.etree import ElementTree

from .base import (
    KIND_TEST,
    PRESENT,
    PRESENT_EMPTY,
    CollectorResult,
    EvidenceItem,
    file_status,
    register,
    utc_now_iso,
)

# Test roots as declared in programme/config/programme.yaml.
DEFAULT_TEST_ROOTS = (
    "tests",
    "orchestration/airflow/tests",
    "orchestration/job_runner/tests",
    "programme/tests",
)

# Committed JUnit XML locations, if the repository records any.
JUNIT_GLOBS = (
    "programme/evidence/test-results/*.xml",
    "evidence/test-results/*.xml",
    "junit/*.xml",
)

_STATUS_MAP = {
    "tests": "PASS",
    "failures": "FAIL",
    "errors": "FAIL",
    "skipped": "SKIPPED",
}


def _suite_level(path: Path) -> str:
    parts = {part.lower() for part in path.parts}
    if "security" in parts:
        return "SECURITY"
    if "terraform" in parts:
        return "STATIC"
    if "cdc" in parts or "mdm" in parts or "contract" in path.name.lower():
        return "CONTRACT"
    if "acceptance" in parts or "gate" in parts:
        return "ACCEPTANCE"
    if "orchestration" in parts or "airflow" in parts:
        return "INTEGRATION"
    if "regression" in path.name.lower():
        return "REGRESSION"
    return "UNIT"


def _test_functions(path: Path) -> int:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return 0
    return len(re.findall(r"^\s*def test_", text, re.MULTILINE))


def _collect_junit(root: Path) -> tuple[dict[str, Any], list[EvidenceItem]]:
    """Parse committed JUnit XML. Absent XML is NOT_RUN, never PASS."""
    facts: dict[str, Any] = {"junit_files": 0, "total_tests": 0}
    items: list[EvidenceItem] = []
    suites: dict[str, Any] = {}
    for pattern in JUNIT_GLOBS:
        for path in sorted(root.glob(pattern)):
            status, size, digest = file_status(path)
            if status != PRESENT:
                continue
            try:
                tree = ElementTree.fromstring(path.read_bytes())
            except ElementTree.ParseError as exc:
                items.append(
                    EvidenceItem(
                        evidence_id=f"EV-TESTXML-{path.stem[:40]}",
                        kind=KIND_TEST,
                        title=f"JUnit XML unreadable: {path.name}",
                        location=str(path.relative_to(root)),
                        status="PRESENT_EMPTY",
                        source_path=str(path.relative_to(root)),
                        observed_at=utc_now_iso(),
                        detail={"error": str(exc)},
                    )
                )
                continue
            facts["junit_files"] += 1
            facts["total_tests"] += int(tree.get("tests", 0) or 0)
            for case in tree.iter("testcase"):
                key = f"{case.get('classname', '')}::{case.get('name', '')}"
                outcome = "PASS"
                for child in case:
                    tag = child.tag.lower()
                    if tag in ("failure", "error"):
                        outcome = "FAIL"
                        break
                    if tag == "skipped":
                        outcome = "SKIPPED"
                suites[key] = outcome
            items.append(
                EvidenceItem(
                    evidence_id=f"EV-TESTXML-{path.stem[:40]}",
                    kind=KIND_TEST,
                    title=f"Committed JUnit results: {path.name}",
                    location=str(path.relative_to(root)),
                    status="PRESENT",
                    source_path=str(path.relative_to(root)),
                    observed_at=utc_now_iso(),
                    detail={"sha256": digest, "bytes": size},
                )
            )
    facts["suites"] = suites
    return facts, items


@register("tests")
def collect_tests(root: Path) -> CollectorResult:
    result = CollectorResult(name="tests")
    observed = utc_now_iso()

    discovered: list[dict[str, Any]] = []
    seen: set[Path] = set()
    for root_rel in DEFAULT_TEST_ROOTS:
        base = root / root_rel
        if not base.is_dir():
            continue
        for path in sorted(base.rglob("test_*.py")):
            if path in seen or "__pycache__" in path.parts:
                continue
            seen.add(path)
            relative = str(path.relative_to(root))
            status, size, digest = file_status(path)
            discovered.append(
                {
                    "name": path.stem,
                    "location": relative,
                    "level": _suite_level(path),
                    "test_count": _test_functions(path),
                    "status": status,
                    "bytes": size,
                    "sha256": digest,
                }
            )

    junit_facts, junit_items = _collect_junit(root)
    result.items.extend(junit_items)

    with_results = sorted(junit_facts.get("suites", {}))
    result.facts = {
        "suite_count": len(discovered),
        "test_function_count": sum(item["test_count"] for item in discovered),
        "junit_file_count": junit_facts["junit_files"],
        "junit_test_count": junit_facts["total_tests"],
        "junit_case_count": len(with_results),
        "test_result": "PASS" if junit_items else "NOT_RUN",
        "observed_at": observed,
        "suites": discovered,
    }

    if not junit_items:
        result.facts["test_result"] = "NOT_RUN"

    for index, suite in enumerate(discovered, start=1):
        result.add(
            EvidenceItem(
                evidence_id=f"EV-TESTSUITE-{index:03d}",
                kind=KIND_TEST,
                title=f"{suite['level']} suite {suite['name']}",
                location=suite["location"],
                status=suite["status"],
                source_path=suite["location"],
                observed_at=observed,
                detail={
                    "level": suite["level"],
                    "test_count": suite["test_count"],
                    "technical_status": (
                        "DISCOVERED" if suite["status"] == PRESENT else "EMPTY_FILE"
                    ),
                    "sha256": suite["sha256"],
                },
            )
        )

    result.add(
        EvidenceItem(
            evidence_id="EV-TESTRUN-LAST",
            kind=KIND_TEST,
            title="Last committed test run result",
            location="programme/evidence/test-results",
            status=PRESENT if junit_items else "MISSING",
            source_path="programme/evidence/test-results",
            observed_at=observed,
            detail={
                "test_result": result.facts["test_result"],
                "test_count": junit_facts["total_tests"],
                "note": (
                    "No committed JUnit XML: the repository records no machine "
                    "test outcome. Suite discovery is not a passing result."
                ),
            },
        )
    )
    return result
