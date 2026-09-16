"""Gate 2 restricted identity resolver (additive design seam, non-destructive).

Architecture: TOKEN -> controlled resolver -> clear identity. Ordinary
Gold/Consumption paths must never reverse tokens; only this explicitly
authorised path resolves, and every attempt is auditable. This module stores
no clear identity itself: it is the enforcement point that a future vault
backing store would sit behind after the destructive-table approval lands.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


class IdentityAccessDenied(PermissionError):
    """Fail-closed denial for unauthorised or unaudited resolution attempts."""


@dataclass
class IdentityResolver:
    authorised_subjects: set[str] = field(default_factory=set)
    audit_log: list[dict] = field(default_factory=list)

    def resolve(self, *, subject: str, token: str, purpose: str = "") -> str:
        event = {
            "subject": subject,
            "token_fingerprint": f"token:{len(token)}chars",
            "purpose": purpose,
            "at": datetime.now(timezone.utc).isoformat(),
            "decision": "DENY",
        }
        if subject not in self.authorised_subjects:
            self.audit_log.append(event)
            raise IdentityAccessDenied(f"subject {subject!r} is not authorised for identity resolution")
        event["decision"] = "REQUIRES_VAULT"
        self.audit_log.append(event)
        # Fail closed: no vault is wired yet (that needs the approved table
        # replacement), so even authorised callers get no clear identity.
        raise IdentityAccessDenied("identity vault is not provisioned; fail closed")
