"""Independent approval boundary for consequential data operations.

Security invariant enforced here::

    Approval supplied by the requester is NEVER independent approval.

`GovernanceRequest.approval` is caller-controlled request data.  After
hardening it is treated strictly as an APPROVAL REFERENCE: an identifier the
requester points at.  The approval decision itself, and its binding, are read
from a server-side registry that the requester cannot reach or influence.

An approval is honoured only when ALL of the following hold:

* the registry is configured;
* the reference resolves to a known approval record;
* the record status is ``approved``;
* the record subject matches the requesting subject;
* the record action matches the requested action;
* the record dataset matches the requested resource.

Any missing condition leaves the operation at ``REQUIRE_APPROVAL``.

With no registry configured - the default - no approval can ever be
satisfied, which is the intended fail-closed behaviour.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from classification import normalize_dataset


APPROVED = "approved"


@dataclass(frozen=True)
class ApprovalRecord:
    """A server-held approval decision and the subject it is bound to."""

    approval_id: str
    subject_id: str
    action: str
    dataset: str
    status: str = APPROVED
    approver: str = ""
    reason: str = ""


class ApprovalRegistry(Protocol):
    """Server-side lookup for approval records."""

    def get(self, approval_id: str) -> ApprovalRecord | None:
        ...


class InMemoryApprovalRegistry:
    """Deterministic registry used by trusted fixtures and local deployments.

    Records are inserted server-side.  Request payloads can only reference an
    ``approval_id``; they can never create, widen or modify a record.
    """

    def __init__(self, records=()):
        self._records: dict[str, ApprovalRecord] = {}

        for record in records:
            self._records[record.approval_id] = record

    def add(self, record: ApprovalRecord) -> ApprovalRecord:
        self._records[record.approval_id] = record
        return record

    def get(self, approval_id: str) -> ApprovalRecord | None:
        return self._records.get(approval_id)


def resolve_approval(
    registry: ApprovalRegistry | None,
    request,
) -> ApprovalRecord | None:
    """Resolve a trusted approval for ``request``, or ``None``.

    ``None`` means the approval gate remains unsatisfied.
    """

    if registry is None:
        return None

    reference = getattr(request, "approval", None)

    if reference is None or not reference.approval_id:
        return None

    record = registry.get(reference.approval_id)

    if record is None or record.status != APPROVED:
        return None

    identity = getattr(request, "identity", None)
    resource = getattr(request, "resource", None)

    if record.subject_id != getattr(identity, "subject_id", None):
        return None

    if record.action != getattr(request, "action", None):
        return None

    dataset = normalize_dataset(getattr(resource, "dataset", "") or "")

    if normalize_dataset(record.dataset or "") != dataset:
        return None

    return record