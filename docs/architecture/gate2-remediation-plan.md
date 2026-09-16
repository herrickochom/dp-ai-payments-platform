# GATE 2 REMEDIATION PLAN - persisted analytical boundary
Status: PLAN ONLY. No rebuild executed. No live Iceberg data changed.
Baseline: clean commit 500b95e. Phase 6 SQL untouched.
Concept: beneficiary_id -> beneficiary_token via approved keyed HMAC.
Row-level linkage preserved; clear identity removed/vaulted.
Tiers: (1) direct real-world identity (name/nin/phone/email) = vault-only;
(2) internal beneficiary_id = restricted join key, removed from outputs;
(3) beneficiary_token = stable HMAC analytical identifier.
A. silver.slv_pdm_beneficiaries -> analytical privacy boundary.
CURRENT: beneficiary_id, beneficiary_token, nin, nin_hashed, nin_verified,
beneficiary_name, date_of_birth, gender, phone, alternative_phone, email,
phone_verified, household_id, special_group_code, village/parish/sub_county/
county/district/region + audit cols.
PROPOSED: beneficiary_token (keyed HMAC of beneficiary_id), nin_token,
phone_token, wallet linkage tokens, nin_verified, phone_verified, gender,
special_group_code, coarse geo (sub_county/county/district/region),
registration_date, is_active + audit cols.
REMOVED: beneficiary_name, nin, date_of_birth, phone, alternative_phone,
email, beneficiary_id, household_id clear, village/parish fine-grained.
TOKENISED: id->token; nin->nin_token; phone->phone_token; wallet/account->token.
MASKED: none persisted (read-time concern); vault holds clear.
RETAINED: flags + coarse geo + status for portfolio/risk joins.
DOWNSTREAM: gold dim/facts + all consumption via gold. ROW COUNT: unchanged
(1 row per beneficiary; key becomes token). REBUILD: full-refresh + downstream.
B. gold.gld_dim_pdm_beneficiary. CURRENT: SKs + beneficiary_id/token +
nin_hashed/phone_hashed (plain sha256) + name/dob/gender/flags/household.
PROPOSED: beneficiary_sk from beneficiary_token (not clear id); beneficiary_
token, nin_token, phone_token, household_token; flags/geo/status kept.
REMOVED: beneficiary_id, name, dob, household clear. TOKENISED: plain sha256
-> keyed HMAC. DOWNSTREAM: 11 models (insights, drilldown, social_impact,
payment_operations, loan_intervention, identity/dup/fraud/lifecycle alerts,
traceability, gold facts). ROW COUNT: unchanged + 1 Unknown row.
C. gold.gld_fct_pdm_payments. CURRENT: payment/entity/sacco/agent/geo SKs +
correlation IDs + creditor_account_hashed (plain sha256) + flags +
is_account_substituted (clear account vs phone compare).
PROPOSED: same grain (source_system, transaction_id); beneficiary_sk from
entity token join; creditor_account_token keyed; token-equality substitution
check. RETAINED: correlation IDs (technical, correlation-preserved).
DOWNSTREAM: agent_cashouts, payment_operations, duplicate/identity alerts,
executive_overview, traceability. ROW COUNT: unchanged.
D. cns_pdm_beneficiary_insights: beneficiary_id -> token; keep SKs +
loan/cashout aggregates. Grain 1 row/beneficiary_sk.
E. cns_pdm_payment_operations: drop beneficiary_id, add token via gold dim
join on beneficiary_sk; grain 1 row/payment; keeps technical IDs.
F. cns_pdm_loan_intervention_dashboard: beneficiary_id -> token; financial/
operational semantics unchanged. Grain 1 row/loan.
G. cns_pdm_ai_default_risk: beneficiary_id -> beneficiary_token; RETAIN
probability_default_90d, ai_risk_band, risk_rank, requires_priority_review,
observation_date, model_name/version, interpretation
PREDICTIVE_DEFAULT_RISK_NOT_FRAUD_DETERMINATION UNCHANGED.
H. cns_pdm_end_to_end_traceability: beneficiary_id -> token; household_id ->
household_token (or drop); keep loan_id + payment IDs. Grain 1 row/loan.
I. cns_pdm_beneficiary_identity_alerts -> RELOCATE to RESTRICTED vault product
keyed by token with keyed nin/phone/account tokens; no clear identity/id/geo.
2. IDENTITY VAULT: new silver.slv_pdm_beneficiary_identity_vault (RESTRICTED,
silver_vault), 1 row/token, envelope-encrypted clears (KMS prod), resolver-only
reads; plus restricted resolution-cases model with masked contact +
approval_reference + audit. Ordinary paths never reverse tokens.
3. TOKEN PROPAGATION: canonical token = HMAC(beneficiary_id) in silver; gold
beneficiary_sk = md5(token); facts/consumption join on token/SKs. Preserves
cross-model joins, portfolio, credit-risk, journey, intervention, longitudinal.
4. DEPENDENCY IMPACT: must-change = gold dim + facts (loans, payment
lifecycle, payments), listed consumption + duplicate_alerts, fraud_insights,
lifecycle_exceptions/cases, executive_drilldown (project beneficiary_id).
Unaffected = executive_overview (SK counts), parish aggregates, silver
loan-grain outputs. Agent DATASET_POLICIES + DQ rules need token updates.
No producer/consumer/Raw changes. Phase6/kafka/event tests unaffected.
5. TRINO LEAST-PRIVILEGE (design only, NOT applied; default deny):
platform_service = SELECT/INSERT/UPDATE/DELETE on staging/bronze/silver/gold/
consumption (job only, no OWN); restricted_identity_service = SELECT vault +
cases only + audit INSERT; analyst = SELECT gold tokenised + operational
consumption (no vault/identity_alerts/silver/bronze/raw), read-only;
bi_dashboard = SELECT aggregated leadership/operations only; agent =
allow-listed governed reads via gateway, no vault/writes; governance_audit =
audit/DQ/policy metadata only; admin = OWN + all, break-glass audited.
No '.*' user; system catalog admin read-only.
6. REBUILD SET: silver slv_pdm_beneficiaries (+ new vault model); gold dim
beneficiary, fct_loans, fct_payment_lifecycle, fct_payments (+ cashouts via
parent); consumption beneficiary_insights, payment_operations,
loan_intervention, ai_default_risk, traceability, identity_alerts (relocate),
duplicate_alerts, fraud_insights, lifecycle_exceptions, exception_cases,
executive_drilldown. NOT rebuilt: raw, staging, bronze, Phase 6 PMN/PLM silver.
7. PROPOSED COMMAND (DO NOT RUN): DP_TOKEN_KEY=<injected> dbt build --select
silver.slv_pdm_beneficiaries silver_vault gold consumption --full-refresh +
dbt test --select <same>; Phase 6 excluded; 476-event baseline untouched.
8. DESTRUCTIVE IMPACT: full-refresh recreates listed tables only (their
history lost); Nessie branch + MinIO prefix backup first; brief
unavailability; no Raw/bronze/Kafka/topic/bucket/catalog loss.
9. EVIDENCE REQUIRED: Nessie pre-snapshot hash; MinIO backup manifest;
row-count parity (same counts, keys id->token); dbt build/test logs; Trino
post-checks (no '.*', deny default, vault blocked for analyst/BI); resolver
audit log. Rollback = restore Nessie snapshot / re-run pre-change refresh.
10. TESTS TO PASS POST-APPROVAL: the 4 gap guards (silver/gold/consumption/
trino); 9 control tests + 11 phase6 + 20 kafka/event stay green; fraud vs
predictive-risk interpretation strings byte-identical. WAIT FOR APPROVAL.
No rebuild, no live data change, no Gate 3.
