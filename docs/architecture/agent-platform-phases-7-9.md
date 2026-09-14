# Agent Platform — Phases 7–9 Architecture

**Scope:** RAG / knowledge layer (Phase 7), multi-agent orchestration (Phase 8),
production hardening (Phase 9) for the `services/agent-api` service.

**Status:** Implemented. This is a **pilot-ready architecture** with a
**production-minded local implementation**, not a claim of production
readiness. It is a controlled pilot candidate for a real government pilot.

**Related records:** [ADR-001](./adr/PDM-Payments-Data-Platform.md),
[ADR-002](./adr/ADR-002-PDM-Dimensional-Gold-Analytics.md).

## 1. Where this sits

```text
User / API
   ↓
Multi-Agent Orchestrator          Phase 8 (bounded, observable)
   ↓
Governance / Policy               Phase 5 (deterministic)
   ↓
┌──────────────────────────────────────────────┐
│                                              │
│ Live Data Tools           Knowledge         │
│ Trino / Metadata          RAG (Phase 7)     │
│ Builders                  Documents         │
│ DQ / Insights             Policies          │
│                                              │
└──────────────────────────────────────────────┘
   ↓
Evidence aggregation
   ↓
Answer / Analysis / Dashboard / Action
   ↓
Audit evidence (Phase 9 durable store)
```

Architecture rules that must not be weakened:

- **Agents reason, plan and coordinate.**
- **Builders validate and execute deterministically.**
- **Live data comes from governed analytical tools such as Trino.**
- **Knowledge comes from the RAG layer.**
- **Governance and policy enforcement remain deterministic.**
  **An LLM must never become the security boundary.**

## 2. Agents

The platform keeps the established specialist agents, each with a fixed
capability contract (`agents.py`, `phase3_agents.py`, `phase4_agents.py`,
`phase5_agents.py`):

| Agent | Responsibility |
| --- | --- |
| Data Discovery | schema/table/column metadata, join inference |
| Analytics | bounded read-only analytical queries |
| Visualization | deterministic chart recommendation |
| Dashboard | deterministic dashboard composition |
| Data Quality | dbt-backed rules, bounded profiling |
| Insight / Monitoring | period comparison, anomaly flags |
| Governance | policy explanation only — decisions stay in the engine |

New in Phase 8: the **knowledge router** routes policy/definition/document
questions to RAG. No agent can instantiate another agent type; the agent
catalogue in `multi_agent.py` is fixed.

## 3. Builders

`DataSourceBuilder`, `VisualizationBuilder`, `DashboardBuilder` remain the
only permitted constructors of analytical artifacts. They validate against
trusted metadata, enforce permissions, and produce read-only specs consumed
by a replaceable BI adapter (`SupersetAdapter`). Superset publication is
**dry-run by default**; `publish=True` requires explicit permission and the
adapter refuses RESTRICTED datasets/fields.

## 4. Governance

Phase 5 deterministic engine (`governance.py`, `governance_models.py`) remains
the single security boundary. Phase 7 reuses the same classification scheme:

```text
PUBLIC < INTERNAL < CONFIDENTIAL < RESTRICTED     DataClassification
```

There is no second classification system in the knowledge layer. RAG filtering
consults the same `DataClassification` and role model before any chunk content,
title, snippet, or derived summary is exposed. The orchestrator calls the
policy guard at four points:

- before planning sensitive tasks (`before_plan`)
- before each protected tool action (`before_tool`)
- before returning sensitive evidence (`before_return`)
- plus tool-level enforcement before every governed query (`tools.py`)

Denials cannot be overridden by orchestration; approval requirements remain
## 5. RAG / knowledge layer (Phase 7)

- Typed models live in `knowledge_models.py`.
- `KnowledgeRetriever` is the replaceable retrieval boundary. The current
  implementation is `LocalKnowledgeRetriever` (`knowledge.py`): deterministic
  lexical matching over bounded chunks, stable document/chunk IDs,
  content hashing, and idempotent re-ingestion. It is intentionally not a
  vector store; the seam exists so a governed vector/embedding backend can
  be introduced without touching the agent/tool layer.
- Allowed `source_type` values are explicit (`policy`, `guidance`,
  `data_dictionary`, `technical_documentation`, `iso20022_reference`,
  `runbook`, `governance`, `architecture`, `procedure`).
- Retrieval is governance-aware **before** content exposure: classification,
  roles, and permissions filter hits. A chunk a user cannot access is absent
  from hits, citations, and metadata alike.
- Retrieved text is **untrusted data**: prompt-injection patterns are
  redacted, `content_is_untrusted=True`, `instructions_allowed` is always
  false at the model level, and retrieved documents can never grant
  permissions, alter governance, or enable unrestricted SQL.
- Every hit carries `KnowledgeEvidence` (citation, classification, retrieval
  score, content hash). Citations are only produced from chunks that actually
  contributed to the result — the backend never fabricates them.
- RAG answers questions about documents and policy. Live quantitative
  questions go to governed Trino. Hybrid requests (knowledge + live data)
  keep two distinct evidence streams (`HybridEvidence`): knowledge evidence
  and live query evidence.

## 6. Orchestration (Phase 8)

`MultiAgentOrchestrator` (`multi_agent.py`) with typed models in
`orchestration_models.py`:

- **Routing** — `DeterministicPlanner` maps task classes to fixed agent
  sequences (analytics, dashboard pipeline, hybrid knowledge+data,
  governance, DQ, insight, RAG). No free-form agent creation.
- **Bounded planning** — max steps, max tool calls, max depth, max retries,
  and an elapsed execution budget are enforced; circular plans are rejected
  before execution.
- **DAG execution** — steps execute only after their dependencies succeed;
  failed branches skip dependants and preserve successful evidence as a
  truthful `PARTIAL` result.
- **Failure semantics** — `FAILED`, `PARTIAL`, `DENIED`,
  `REQUIRES_APPROVAL`, `COMPLETED`; failed substeps are never hidden.
- **Evidence** — final results expose only structured `AgentEvidence`
  (agent, tool, reference, structured request). Model reasoning is never
  part of the result contract.
## 7. Production hardening (Phase 9)

### Configuration (`config.py`)

Environment-driven settings, local defaults, and fail-fast `__post_init__`
validation (port/scheme ranges, limits, timeouts). No secrets are committed
(`.gitignore` covers `.env*`); anything secret is read from the environment
and never logged.

### API hardening (`api.py`)

- Request IDs / correlation IDs (`x-request-id`) set on every response.
- Health endpoints: `/health/live` (liveness), `/health/ready`
  (dependency-aware 503 when required Trino health fails), `/agents/health`.
- Bounded request body size (413), request timeout (504), generic safe 500
  messages with no stack traces, and an optional auth hook
  (`AGENT_AUTH_ENABLED`, default **false**) for deployment behind an upstream
  identity gateway. Authorisation remains the governance engine's job.

### Tool execution (`tools.py`)

Every tool call has a bounded execution deadline (`ToolTimeout`), bounded
retries for transient network/timeout failures (`tool_max_retries`), a tool
call budget, structured `ToolCall` evidence (status, duration, error
category, attempts), and validated read-only SQL. There is no shell
execution, no arbitrary SQL, no arbitrary Python execution, and no arbitrary
URL fetching.

### Observability (`observability.py`)

Structured JSON operational events for `request_received`,
`orchestration_started`, `agent_selected`, `policy_decision`,
`tool_invoked`, `rag_retrieval`, `request_timeout`, `request_failed`,
`request_completed`, `orchestration_completed`, plus the pre-existing
`agent_request_completed`/`agent_request_failed` events. No passwords,
tokens, full restricted beneficiary records, or secret configuration are
logged; query/objective text is truncated or unlogged.

### Audit persistence (`audit.py`)

`JsonlAuditStore` persists `GovernanceAuditEvent` rows (actor, roles, action,
resource, classification, decision, matched policies, policy reasons, masked
fields, purpose, geography scope, request/correlation IDs, approval
reference, timestamp) without sensitive payloads. Recorded by
`/governance/evaluate`, `/agents/governance`, and tool-level enforcement for
consequential (non-ALLOW) protected-action decisions.

### Resilience

Trino connections carry request timeouts and `query_max_execution_time`;
the Superset adapter uses HTTP timeouts for all calls; RAG and agent failures
propagate as structured tool events. Kafka invariants are untouched
(at-least-once consumption, idempotent sink, `(topic, partition, offset)`
identity, bounded retries, DLQ, no poison skip, deterministic
`failure_id`). MinIO/Nessie are not invoked by `agent-api`.

### Data safety

There is no agent/exposed path that can drop tables, delete Kafka topics,
wipe MinIO, reset Nessie, or delete dashboards/datasets. Read-only SQL
validation rejects DDL/DML/privilege statements deterministically. No
destructive admin capability has been added because none is required.
## 8. Payment observability and BX

The Phase 6 PMN (pain.001 technical lifecycle) and PLM (pain.002 technical
lifecycle) streams remain governed analytical inputs. Their silver tables are
registered in the trusted classification registry as INTERNAL operational
data, so oversight queries are policy-ruled rather than silently denied.
Technical event correlation tables remain readable to authorised internal
analysts; beneficiary-level identity remains RESTRICTED. "BX" business
evidence follows the same governed analytical path — no synthetic or
beneficiary payload data is ever exposed through RAG.

## 9. Deployment boundaries and local limitations

Production-like:

- deterministic governance and classification
- RAG safety and evidence/citations
- bounded orchestration and failure semantics
- durable structured audit
- structured operational events
- health/readiness split
- configuration fail-fast + optional auth hook
- read-only SQL enforcement and publication dry-run default

Deliberately local:

- `LocalKnowledgeRetriever` is in-process and lexical; a real pilot should
  use a governed vector store behind the same `KnowledgeRetriever` seam and
  a managed ingestion pipeline with versioned document buckets.
- `JsonlAuditStore` is an append-only local file store; a pilot should point
  the same event contract at governed log shipping (e.g. an append-only
## 10. Test coverage

- `tests/test_agent_phase7.py` — RAG IDs, idempotency, relevance,
  classification/role enforcement, injection isolation, evidence, hybrid
  separation.
- `tests/test_agent_phase8.py` — routing, DAG, limits, loop protection,
  failure/denial/approval, evidence aggregation.
- `tests/test_agent_phase9.py` — config fail-fast, health/readiness,
  audit durability, timeouts, retries, request limits, auth hook, and the
  bounded local production smoke suite (metadata, safe Trino query, RAG,
  DQ, dashboard dry-run, PLM technical lifecycle query, beneficiary
  denial/masking, audit creation, orchestration denial).
- Phase 1–6 suites remain green and are the regression gate for agent,
  builder, DQ, insight, governance and Kafka behaviour.
  remote log sink).
- The transport auth hook assumes an upstream identity gateway; real
  authentication, session management and credential rotation are deployment
  concerns.
- Trino is the only analytical engine exercised; resiliency for distributed
  environments (partial network partitions, cluster failover) is not
  simulated locally.

What would change for a real government pilot:

1. hosted retrieval backend with embeddings + document lifecycle/approval
2. managed identity provider integration (OIDC/SAML) with the same
   `can_view_*` permission model
3. durable audit shipping with retention, export and evidential integrity
4. deployment-time secret management (no local defaults for credentials)
5. load/performance characterisation and hardening budgets
6. formal data-sharing agreements driving the classification registry

Until those are in place, the platform should be described as
**pilot-ready architecture** and a **production-minded local
implementation**, not as production-ready.
explicit (`REQUIRES_APPROVAL`).