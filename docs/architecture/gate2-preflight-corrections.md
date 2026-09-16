# GATE 2 CORRECTIONS (non-destructive; pre-execution) — 2026-09-15
1. Canonical beneficiary_token:
- Existing Bronze beneficiary_token = sha256(NIN) truncated, TOKEN-<16 hex>,
  unkeyed, NIN-derived (NOT beneficiary_id-derived). Verdict: NOT keyed, NOT
  suitable as canonical Gate 2 token. Must be treated as legacy
  `source_beneficiary_token`, never the analytical beneficiary_token.
- Canonical Gate 2 token: versioned keyed HMAC-SHA256 of canonical
  beneficiary_id via DP_TOKEN_KEY/DP_TOKEN_KEY_VERSION (services/shared/
  data_protection.py:keyed_token, format v1:<hex>). Generated at the Silver
  privacy boundary; Gold/Consumption propagate ONLY this token under the name
  beneficiary_token. Legacy source token renamed source_beneficiary_token and
  dropped downstream.
- Gold beneficiary_sk/creditor joins re-derived from canonical token, never
  clear id. Correlation IDs untouched (Phase 6 intact).
- dbt HMAC constraint (verified DuckDB 1.5.5): no hmac()/getenv() scalar.
  Therefore canonical tokenisation must be implemented either (a) as a dbt
  Python model calling the approved Python keyed_token (preferred, no secret
  in SQL), or (b) with the key injected ONLY as a bound dbt var/session
  parameter with fail-closed missing-key handling and no key in logs/target.
  Plain sha256(value) in SQL is BANNED. No dbt model edited yet.
2. Explicit selector (NOT run): silver: slv_pdm_beneficiaries; new silver_vault
  identity model; gold: gld_dim_pdm_beneficiary, gld_fct_pdm_loans,
  gld_fct_pdm_payment_lifecycle, gld_fct_pdm_payments (+ gld_fct_pdm_agent_
  cashouts via parent); consumption: cns_pdm_beneficiary_insights,
  cns_pdm_payment_operations, cns_pdm_loan_intervention_dashboard,
  cns_pdm_ai_default_risk, cns_pdm_end_to_end_traceability,
  cns_pdm_beneficiary_identity_alerts (relocate), cns_pdm_duplicate_
  fragmentation_alerts, cns_pdm_fraud_risk_insights, cns_pdm_lifecycle_
  exceptions, cns_pdm_lifecycle_exception_cases, cns_pdm_executive_geographic_
  drilldown. EXCLUDED: Phase 6 PMN/PLM lifecycle, technical_events,
  event_correlation, Raw, Staging, Bronze. Baseline 476 untouched.
3. Table-scoped recovery: record Nessie main HEAD + create pre-gate2-<ts>
  branch/tag reference (no main switch, no global reset); record per-table
  content key/metadata location/row count/schema/snapshot; check MinIO backup
  manifest for affected prefixes. All captured pre-execution.
Age band: date_of_birth -> age_band in ordinary outputs (dob vault-only).
Geography: village/parish removed from row-level beneficiary outputs; retained
via aggregated geo models (parish/village risk, drilldowns on SKs).
Credit risk: beneficiary-level pseudonymous, token only, semantics unchanged.
PREFLIGHT (recorded): Gate2 9 pass/4 gap-fail; Phase6 11 pass; Kafka/event 20
pass; git diff --check PASS. Destructive execution NOT started: no live
recovery reference created, no table metadata snapshot captured, no MinIO
manifest, no Trino syntax validation — shell output capture unavailable, so
preflight items 9/Trino-checks cannot be evidenced yet. STOP before dbt build.
