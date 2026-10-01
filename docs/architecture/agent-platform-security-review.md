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
---

## 4A. F1 — TRUSTED IDENTITY

JOB F1 replaced the honest fail-closed posture with a real cryptographic
identity boundary. A1–A4 history above is unchanged and still accurate for the
authority model; F1 only supplies a trustworthy *subject* for it.

### 4A.1 F1 discovery

Discovery established that the platform had **no** identity infrastructure for
the agent API: no IdP contract (`services/shared/identity_resolver.py` is a
beneficiary-tokenisation resolver, not a user IdP), no cryptographic library in
the agent-api runtime, no issuer/audience/JWKS configuration, and no trusted
gateway. `AGENT_AUTH_ENABLED` existed only in `config.py` and was never wired
through deployment. The documented intent was explicitly "deployment behind an
upstream identity gateway". A design decision was therefore required and was
supplied by the platform owner.

### 4A.2 Architecture

```
Authorization: Bearer <access token>
        |
        v
authentication_hook (api.py)
        |  health endpoints exempt (container healthcheck)
        v
TokenVerifier (authentication.py)
        |  1. Bearer scheme required, exactly one credential
        |  2. algorithm must be server-pinned (RS256); alg=none and the HMAC
        |     family are rejected before any key is resolved
        |  3. signing key resolved from JWKS by kid
        |  4. signature, iss, aud, exp verified; nbf verified when present;
        |     sub required
        |  5. any failure -> AuthenticationError -> 401
        v
TrustedIdentity(subject_id, issuer, authentication_method, authenticated)
        |  carries NO token, signature, key material, or role/permission claim
        v
ServerAuthorityResolver.resolve_verified(subject_id)
        |  roles come from the SERVER-OWNED binding file only
        v
Authority -> A3 governance / capability / policy
```

`authentication.py` performs **authentication only**. It never reads
`roles`, `permissions`, `groups`, `scope` or `can_publish_bi_assets` from a
token. `authority.py` performs **authorisation** from server state. The two are
separate decisions and tests prove they stay separate.

### 4A.3 Verification rules

| Rule | Enforcement |
|---|---|
| Allowed algorithms | server configuration, default `RS256`; symmetric and `none` refused at construction |
| `iss`, `aud`, `exp`, `sub` | required via PyJWT `options.require` |
| `nbf` | verified when present |
| Key selection | `kid` via JWKS; unknown `kid` never falls back |
| Key rotation | `PyJWKClient` refreshes the key set on unknown `kid`; retired keys stop verifying |
| JWKS outage / malformed JWKS | authentication failure; **never** an unverified fallback |
| Clock skew | `AGENT_AUTH_CLOCK_SKEW_SECONDS`, default 0 |

### 4A.4 Role binding

`AGENT_AUTH_ROLE_BINDINGS_FILE` points at a server-owned YAML file:

```yaml
subjects:
### 4A.6 Dependency

`PyJWT[crypto]==2.9.0` added to `services/agent-api/requirements.txt`; installed
into the image through the existing `Dockerfile.agent-api` path
(`COPY services/agent-api/requirements.txt` → `pip install -r`). No manual
container installs, no unrelated dependency changes, no overlapping JWT
libraries.

### 4A.7 Failure semantics

| Condition | Response |
|---|---|
| missing / malformed Authorization | 401 `AuthenticationRequired` |
| invalid signature, expired, wrong issuer/audience, `alg=none`, algorithm confusion, unknown `kid`, JWKS outage | 401 `InvalidToken` / `SigningKeyUnavailable` |
| authenticated but no server-bound authority | 403 `PublicationAuthorityRequired` |

Client-facing messages are generic. Bearer tokens, expected issuer, expected
audience and key ids are never returned to the caller.

### 4A.8 Audit

Authentication failures are logged with `event`, `path` and a coarse
`error_category` only. Bearer tokens, `Authorization` header values, signatures
and key material are never logged. A verified identity contributes only
`subject_id`, `issuer` and `authentication_method` via
`TrustedIdentity.audit_fields()`.

### 4A.9 Deployment prerequisites

A real deployment must supply, or governed access stays denied:

```
AGENT_AUTH_ENABLED=true
AGENT_AUTH_ISSUER=https://<your-idp>/
AGENT_AUTH_AUDIENCE=<agent-api-audience>
AGENT_AUTH_JWKS_URL=https://<your-idp>/.well-known/jwks.json
AGENT_AUTH_ROLE_BINDINGS_FILE=/path/to/server-owned-roles.yaml
```

plus HTTPS connectivity from the agent-api container to the JWKS endpoint.

**IMPLEMENTATION COMPLETE is not PRODUCTION IDP CONFIGURED.** F1 ships a
verified mechanism; it does not provision an external identity provider.

### 4A.10 Test evidence

```
tests/security/test_agent_jwt_validation.py             34 passed
tests/security/test_agent_identity_authority_binding.py  9 passed
tests/security/test_agent_trusted_identity.py           12 passed
A2 security proofs (unchanged)                          21 passed
Agent regression (phase1-9 + Trino security)           142 passed, 15 subtests
```

All key material in the F1 suite is ephemeral and generated in-process. No key
is written to disk, to `.env`, or to any deployed configuration.
  <subject-id>:
    roles:
      - programme_analyst
```

* unknown role name → configuration error (fail closed);
* unknown subject → authenticates with **no roles** (authenticated, unprivileged);
* never sourced from a request body, an HTTP header, or a token claim.

### 4A.5 Configuration

| Name | Required when | Default | Effect |
|---|---|---|---|
| `AGENT_AUTH_ENABLED` | — | `false` | `true` requires a verified bearer token on non-health routes |
| `AGENT_AUTH_ISSUER` | auth enabled | *(none)* | expected `iss`; **no default** |
| `AGENT_AUTH_AUDIENCE` | auth enabled | *(none)* | expected `aud`; **no default** |
| `AGENT_AUTH_JWKS_URL` | auth enabled | *(none)* | key endpoint; must be `https` in production mode |
| `AGENT_AUTH_ALLOWED_ALGORITHMS` | auth enabled | `RS256` | server-pinned verification algorithms |
| `AGENT_AUTH_CLOCK_SKEW_SECONDS` | optional | `0` | explicit, minimal leeway |
| `AGENT_AUTH_JWKS_CACHE_SECONDS` | optional | `300` | JWKS key cache/rotation lifetime |
| `AGENT_AUTH_ROLE_BINDINGS_FILE` | optional | *(none)* | server-owned subject→role bindings |

Enabling authentication without issuer, audience and JWKS URL is a **startup
error**, not a silent downgrade.

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

---

## 4B. F2/F3 - BI AND SUPERSET LEAST-PRIVILEGE BOUNDARY

F1 secured WHO may request a privileged operation. F2/F3 secure the authority
of the BI services that act downstream.

### 4B.1 Discovery findings

| ID | Finding |
|---|---|
| F2-1 | The publication adapter authenticated with `${SUPERSET_ADMIN_USERNAME}`/`${SUPERSET_ADMIN_PASSWORD}` - the Superset **administrator** created by `superset fab create-admin`. |
| F2-2 | `/publish/superset` accepted an arbitrary caller-supplied `DashboardSpec`; only the classification gate bounded it, so any INTERNAL dataset could be registered. |
| F3-1 | `rules.json` granted `trino` and `metabase` `consumption.*` SELECT, including the identity-bearing datasets GATE-2 identified. |
| F3-2 | `expose_in_sqllab: true` on the automated BI connection. |
| F3-3 | The curated BI manifest referenced `trino://pdm@...`, an identity never provisioned in `password.db` or access control. |
| F3-4 | No repository-controlled dashboard exposure policy. |

### 4B.2 Controls implemented

**F2-1 - dedicated publication identity.** `SUPERSET_PUBLISHER_USERNAME` /
`SUPERSET_PUBLISHER_PASSWORD` (via the SecretProvider contract). The adapter
reads only these; `SupersetAdapter._require_publisher_identity()` fails closed
when absent and **never** falls back to administrator credentials. The
agent-api compose block no longer receives `SUPERSET_ADMIN_*`.

**F2-2 - server-owned publication allowlist.** `services/agent-api/bi_policy.py`
holds the approved BI dataset policy. Publication is refused unless the dataset
is on it. Holding `bi_publisher` plus an INTERNAL classification is explicitly
**not** sufficient. The RESTRICTED classification gate keeps precedence.

**F3-1 - explicit BI table grants.** `rules.json` now grants
`superset_bi` and `metabase_bi` SELECT on exactly the 19 approved BI tables.
Trino file access control is deny-by-default, so bronze, silver,
silver_vault, gold and every non-approved consumption table are denied.

**F3-2 - SQL Lab disabled** on the controlled BI connection.

**F3-3 - canonical BI identity.** The manifest and compose now use
`superset_bi`; the unprovisioned `pdm` reference is gone. No credential is
embedded in any committed file.

**F3-4 - dashboard exposure policy.** Automated publication creates dashboards
with `published: False` and `owners: []`, so repository-controlled publication
can never make an asset anonymously reachable.

### 4B.3 Trino BI access matrix

| Identity | Approved BI tables | Identity-bearing | bronze/silver/silver_vault/gold | Write |
|---|---|---|---|---|
| `superset_bi` | ALLOW (19) | DENY | DENY | none |
| `metabase_bi` | ALLOW (19) | DENY | DENY | none |
| `trino`, `metabase` (legacy) | preserved unchanged (D6) | unchanged | DENY | none |
| `agent-api` | unchanged: 1 table | DENY | DENY | none |
| `dbt` | unchanged: full | unchanged | unchanged | preserved |

### 4B.4 Runtime provisioning still outstanding

Repository controls are complete, but these external states are NOT claimed:

```
F2_RUNTIME_SUPERSET_PUBLISHER_PROVISIONING=OUTSTANDING
F2_RUNTIME_ROLE_PROVISIONING=OUTSTANDING
F3_RUNTIME_TRINO_IDENTITY_PROVISIONING=OUTSTANDING
F3_METABASE_RUNTIME_IDENTITY_MIGRATION=OUTSTANDING
F3_RUNTIME_DASHBOARD_ACCESS_REVIEW=OUTSTANDING
```

Superset permission identifiers could not be proven without running Superset
6.1.0, so no role/permission guess was made; only the REST endpoint surface
traced from the adapter is enforced.

### 4B.5 Test evidence

```
tests/security/test_bi_publication_least_privilege.py   17 passed
tests/security/test_bi_trino_access_control.py          38 passed
F1 suite                                               55 passed
A2/A3/A4 security suite                                21 passed
Agent regression                                  142 passed + 15 subtests
```

## 5. REMAINING limitations

These are real and are NOT solved by this work.

1. **F1 SUPERSEDES THIS ITEM.** Cryptographic OIDC/JWT verification is now
   implemented (section 4A). What remains true: the platform ships no
   production IdP. Until an operator configures issuer/audience/JWKS and a
   subject-role binding, `AGENT_AUTH_ENABLED` stays false, no trusted
   identity exists, and privileged publication fails closed.
   `x-subject-id` survives only as an untrusted diagnostic hint; it can
   never set `subject_verified` or grant authority.
2. **No server role registry is configured in production.** F1 provides the
   mechanism (`AGENT_AUTH_ROLE_BINDINGS_FILE`), but no bindings ship with the
   repository, so governed requests receive no roles and are denied. The
   platform serves metadata-only discovery but **no governed data** until an
   operator supplies bindings. Deliberate fail-closed behaviour.
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
| ~~F1~~ | **CLOSED** - cryptographic OIDC/JWT + JWKS verification implemented (section 4A) |
| ~~F2~~ | **CODE CLOSED** - dedicated publication identity + allowlist (section 4B); runtime provisioning outstanding |
| ~~F3~~ | **CODE CLOSED** - explicit BI Trino grants + SQL Lab off (section 4B); runtime identity provisioning outstanding |
| F1a | Provision a production IdP; configure issuer/audience/JWKS and subject-role bindings |
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