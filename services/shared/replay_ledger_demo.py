"""Gate 3.6C synthetic replay-control demonstration.

Offline only. No Kafka or infrastructure connectivity.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from services.shared.replay_ledger import (
    ReplayLedger,
    ReplayRequest,
    ReplayStatus,
)


def make_request(request_id: str, **overrides) -> ReplayRequest:
    values = {
        "replay_batch_id": "SYNTHETIC-BATCH",
        "replay_request_id": request_id,
        "source_topic": "synthetic.source",
        "source_partition": 0,
        "source_offset": 100,
        "target_topic": "synthetic.target",
        "replay_reason_code": "SYNTHETIC_DEMO",
        "requested_by_service_or_role": "synthetic-demo-role",
        "approved": True,
        "policy_resolved": True,
        "source_event_reference": None,
    }
    values.update(overrides)
    return ReplayRequest(**values)


def main() -> int:
    passed = 0
    failed = 0

    def result(name: str, expected: str, actual: str) -> None:
        nonlocal passed, failed
        ok = expected == actual
        if ok:
            passed += 1
        else:
            failed += 1

        print(
            f"{name} | expected={expected} | "
            f"actual={actual} | {'PASS' if ok else 'FAIL'}"
        )

    with tempfile.TemporaryDirectory(prefix="gate3-replay-demo-") as tmp:
        ledger = ReplayLedger(Path(tmp) / "synthetic-ledger.db")

        # 1. Valid reservation.
        identity = ledger.reserve(make_request("SYNTHETIC-REQ-001"))
        result(
            "valid reservation",
            ReplayStatus.REQUESTED.value,
            ledger.get(identity)["replay_status"],
        )

        # 2. Approval transition.
        ledger.transition(identity, ReplayStatus.APPROVED)
        result(
            "approval transition",
            ReplayStatus.APPROVED.value,
            ledger.get(identity)["replay_status"],
        )

        # 3. Execution-start state only; no Kafka execution occurs.
        ledger.transition(identity, ReplayStatus.IN_PROGRESS)
        result(
            "in-progress transition",
            ReplayStatus.IN_PROGRESS.value,
            ledger.get(identity)["replay_status"],
        )

        # 4. Ledger is explicitly told synthetic execution succeeded.
        ledger.transition(
            identity,
            ReplayStatus.SUCCEEDED,
            "SYNTHETIC_SUCCESS",
        )
        result(
            "synthetic success transition",
            ReplayStatus.SUCCEEDED.value,
            ledger.get(identity)["replay_status"],
        )

        # 5. Duplicate replay is suppressed and audited.
        try:
            ledger.reserve(make_request("SYNTHETIC-REQ-002"))
            duplicate_actual = "NOT_SUPPRESSED"
        except RuntimeError:
            duplicate_actual = ReplayStatus.SUPPRESSED_DUPLICATE.value

        result(
            "duplicate reservation",
            ReplayStatus.SUPPRESSED_DUPLICATE.value,
            duplicate_actual,
        )

        attempts = ledger.attempts(identity)

        # 6. Duplicate attempt persists in audit.
        result(
            "duplicate audit",
            ReplayStatus.SUPPRESSED_DUPLICATE.value,
            attempts[-1]["attempt_status"],
        )

        # 7. Missing approval fails closed.
        try:
            ledger.reserve(
                make_request(
                    "SYNTHETIC-REQ-003",
                    source_offset=101,
                    approved=False,
                )
            )
            approval_actual = "ALLOWED"
        except PermissionError:
            approval_actual = "BLOCKED"

        result("missing approval", "BLOCKED", approval_actual)

        # 8. Unresolved policy fails closed.
        try:
            ledger.reserve(
                make_request(
                    "SYNTHETIC-REQ-004",
                    source_offset=102,
                    policy_resolved=False,
                )
            )
            policy_actual = "ALLOWED"
        except PermissionError:
            policy_actual = "BLOCKED"

        result("unresolved policy", "BLOCKED", policy_actual)

        # 9. Protected metadata fails closed.
        try:
            ledger.reserve(
                make_request(
                    "SYNTHETIC-REQ-005",
                    source_offset=103,
                ),
                {"payload": "SYNTHETIC-PROHIBITED"},
            )
            privacy_actual = "ALLOWED"
        except ValueError:
            privacy_actual = "BLOCKED"

        result("protected metadata", "BLOCKED", privacy_actual)

        # 10. Terminal SUCCEEDED cannot reopen.
        try:
            ledger.transition(identity, ReplayStatus.IN_PROGRESS)
            terminal_actual = "REOPENED"
        except ValueError:
            terminal_actual = "BLOCKED"

        result("terminal success protection", "BLOCKED", terminal_actual)

        ledger.close()

    print(f"TOTAL_SCENARIOS={passed + failed}")
    print(f"PASSED={passed}")
    print(f"FAILED={failed}")
    print("KAFKA_MESSAGES_REPLAYED=0")
    print("OFFSETS_RESET=0")
    print("LIVE_INFRASTRUCTURE_CONNECTIONS=0")
    print("DURABLE_LIVE_LEDGER_CREATED=0")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
