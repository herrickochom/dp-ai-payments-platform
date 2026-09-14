from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from uuid import uuid4

from governance_models import GovernanceAuditEvent, GovernanceRequest, PolicyDecision


class JsonlAuditStore:
    """Append-only local audit persistence containing decisions, not sensitive rows."""
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._lock = threading.Lock()

    def append(self, event: GovernanceAuditEvent) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(event.model_dump(mode="json"), separators=(",", ":")) + "\n"
        with self._lock, self.path.open("a", encoding="utf-8") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())

    def record_decision(self, request: GovernanceRequest, decision: PolicyDecision,
                        request_id: str | None = None) -> GovernanceAuditEvent:
        approval = request.approval
        event = GovernanceAuditEvent(
            audit_event_id=f"audit-{uuid4()}", actor=request.identity.subject_id,
            roles=sorted(request.identity.roles), agent=request.agent, tool=request.tool,
            action=request.action, resource=request.resource.dataset,
            fields=sorted(request.resource.fields), classification=request.resource.classification,
            decision=decision.decision, matched_policies=decision.matched_policies,
            masked_fields=decision.masked_fields, purpose=request.identity.purpose,
            policy_reason=decision.reasons,
            geography_scope=request.resource.parish or request.resource.district,
            request_id=request_id, correlation_id=request_id,
            approval_reference=approval.approval_id if approval else None,
            timestamp=decision.timestamp,
        )
        self.append(event)
        return event
