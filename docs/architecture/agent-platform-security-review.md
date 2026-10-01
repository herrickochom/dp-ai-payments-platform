# AI Agent Platform — Security Review and Authority Boundary

Status: **HARDENED** (JOB A1 discovery, JOB A2 proofs, JOB A3-CORE,
JOB A3-MASKING, JOB A4-FINAL closure).

This document records the security architecture of the agent platform, the
vulnerabilities that were proven, the controls that now enforce the boundary,
and the limitations that remain. No credentials or secret values appear here.

---

## 1. Final authority flow

There is exactly one authority flow. Every security decision resolves to
server-side state; caller payload is only ever a *request*.

```
HTTP request
   |
   +- authentication_hook (api.py)
   |     records an UNVERIFIED subject hint and
   |     request.state.authority = unauthenticated()
   |
   v
trusted authentication boundary           <-- F1: no cryptographic provider yet
   |
   v
ServerAuthorityResolver (authority.py)
     - role_authorities : subject -> server-authorised roles   (server input)
     - server_capabilities                                  (server ceiling)
     - authorised roles = claimed roles INTERSECT server role registry
   |
   v
Orchestrator._authoritative_governance
     - replaces the claimed identity with the authorised identity
     - discards IdentityContext.permissions entirely
   |
   v
narrow_permissions
     effective_capability is a SUBSET OF server_authorised_capability
   |
   v
ToolRegistry.execute_query
     - governance is MANDATORY: no governance -> GovernanceDenied
   |
   v
TrustedClassificationResolver
     - resolves projection lineage (output label -> source field)
     - rejects unresolvable SELECT * (fail closed)
   |
   v
GovernancePolicyEngine
     - effective authority is ROLE-DERIVED only
     - approval resolved from the server-side ApprovalRegistry
   |
   v
enforcement
     DENY / REQUIRE_APPROVAL stop execution; ALLOW/MASK execute
   |
   v
_mask_query_rows - masking follows FIELD LINEAGE, not the returned label
   |
   v
result + JsonlAuditStore evidence
```

Publication follows a stricter path: `SupersetAdapter.publish` requires an
`authority.Authority` issued by the server, and that authority must be
`authenticated`. A `models.Permissions` value can never satisfy it.

---

## 2. A1 discovery summary

A1 identified seven exploitable authority and data-protection boundaries. A2
converted each into an executable, deterministic proof. Each proof was written
to *fail* on insecure behaviour, so the failures were themselves the evidence.

---

## 3. Vulnerabilities proven (A2) and controls implemented (A3)

| ID | Vulnerability | Before | Control now |
|---|---|---|---|
| A2.1 | Governance omission — `context.governance` absent, classification/policy/masking skipped, Trino still queried | VULNERABILITY_PROVEN | `ToolRegistry.execute_query` denies without governance; `Orchestrator` fails governed agents up front (`GovernanceContextRequired`) |
| A2.2 | Caller-supplied `AgentRequest.permissions` acted as capability authority | VULNERABILITY_PROVEN | `ServerAuthorityResolver` + `narrow_permissions`; capabilities are an intersection, caller may only narrow |
| A2.3 | `effective_permissions()` unioned caller `IdentityContext.permissions` with role permissions | VULNERABILITY_PROVEN | role-derived only; the raw permission list grants nothing |
| A2.4 | Self-asserted `approval.status = "approved"` satisfied `REQUIRE_APPROVAL` | VULNERABILITY_PROVEN | `approvals.py` server registry; a payload approval is a *reference id* bound to subject/action/dataset |
| A2.5 | SQL alias `beneficiary_name AS bn` defeated masking | VULNERABILITY_PROVEN | `projection_lineage_from_sql` + `ResourceContext.field_lineage`; masking resolves the returned label back to its source field |
| A2.6 | `SELECT *` produced no field classification, so sensitive values passed unmasked | VULNERABILITY_PROVEN | star projections fail closed unless the platform holds a *complete* trusted column list (`DATASET_COLUMNS`, intentionally empty) |
| A2.7 | Anonymous caller with `can_publish_bi_assets=true` reached the Superset login with service authority | VULNERABILITY_PROVEN | publication authority resolved server-side and requires an authenticated identity; `/publish/superset` returns 403 before the adapter is constructed |
| A4.4 | Caller could claim a privileged **role** in its own governance context | Found during A4 review | `Orchestrator._authoritative_governance` intersects claimed roles with the server role registry (empty by default) |
---

## 4. CLOSED vulnerabilities

All seven A2 proofs plus the A4.4 role-claim finding are closed and protected
by permanent regression tests:

- `tests/security/test_agent_governance_omission.py`
- `tests/security/test_agent_caller_authority.py`
- `tests/security/test_agent_permission_spoofing.py`
- `tests/security/test_agent_self_approval.py`
- `tests/security/test_agent_masking_bypass.py`
- `tests/security/test_agent_superset_publication_authority.py`
- `tests/security/test_agent_masking_regressions.py`

The Phase 5 test that previously preserved governance-free execution
(`test_query_without_governance_remains_backward_compatible`) was **replaced**
(not deleted) by `test_query_without_governance_is_denied_before_execution`,
which asserts the fail-closed contract.

---

## 5. REMAINING limitations

These are real and are NOT solved by this work.

1. **No cryptographic authentication.** `x-subject-id` is caller-supplied and
   spoofable. `authentication_hook` records it only as an unverified hint and
   never as authority. Until a trusted identity provider is wired in,
   `request.state.subject_verified` is always `False`, so **privileged
   publication always fails closed**.
2. **No server role registry is configured in production.** The default
   `ServerAuthorityResolver.role_authorities` is empty, so governed agent
   requests receive no roles and are denied. The platform therefore serves
   metadata-only discovery but **no governed data** until a trusted identity
   source supplies role authorities. This is deliberate fail-closed behaviour,
   not an oversight.
3. **`SELECT *` is unavailable for governed datasets** until a dataset is
   registered in `DATASET_COLUMNS` with a provably complete column list. The
   dbt schema yml files document only test columns, so they are not a safe
   source.
4. **Multi-dataset queries are still unsupported** and fail closed.
5. **RAG / multi-agent runtime paths** were not runtime-reachable and were not
   re-enabled. `LocalKnowledgeRetriever` authorisation was hardened to
   role-derived authority, but `MultiAgentOrchestrator` has not had a dedicated
   security gate.
6. **Audit is application-level only.** Decisions are persisted to the JSONL
   audit store, but there is no Trino query event listener correlating
   request id, subject, query id and decision at the data layer.

---

## 6. FUTURE enhancements (backlog)

| ID | Item |
|---|---|
| F1 | Production authentication: OIDC/JWT or mTLS identity, issuer/audience verification, removal of spoofable subject trust |
| F2 | Superset least-privilege service account for publication |
| F3 | BI least privilege over identity-bearing consumption datasets |
| F4 | Superset hardening: dashboard role bindings, RLS, HTML sanitisation, secure cookies, SQL row limits, Redis result-cache security |
| F5 | Trino per-user identity propagation / impersonation |
| F6 | Trino query event listener and durable data-layer audit correlation |
| F7 | Transport hardening: Trino HTTPS for BI clients; remove obsolete auth config where proven unused |
| F8 | Network segmentation and reduced host-published attack surface |
| F9 | Secrets management: move runtime credentials out of `.env` into the secret-provider contract and rotate affected credentials |
| F10 | Dedicated security gate for multi-agent and RAG runtime paths before enabling them |

---

## 7. Test evidence

Latest verified results:

```
A2 security proofs + masking regressions   21 passed, 0 failed, 0 errors
Agent regression (phase1-9 + Trino security) 142 passed, 15 subtests passed, 0 failed, 0 errors
```

Baseline before hardening was `142 passed, 15 subtests passed`; the regression
surface is unchanged. The A2 proof files were intentionally failing at the
start of A3 and are now green.