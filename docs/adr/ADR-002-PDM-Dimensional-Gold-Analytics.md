# ADR-002: PDM Dimensional Gold and Consumption Analytics

**Version:** 1.2.0  
**Date:** 2026-08-29  
**Status:** Proposed  
**Deciders:** Project Team, PDM Secretariat, Technical Architects  
**Supersedes:** The Silver/Gold model mappings in ADR-001

## Context

ADR-001 established the Uganda PDM payment ecosystem and source-system mapping.
The implemented pipeline now separates concerns as follows:

- Raw preserves immutable Kafka/MinIO evidence.
- Staging and Bronze decode source-shaped ISO 20022 and JSON records.
- Silver creates deduplicated, reconciled, canonical 3NF entities using natural keys.
- Gold provides reusable dimensional facts and conformed dimensions.
- Consumption provides audience-specific metrics, risk indicators and dashboard views
  without embedding presentation logic in Silver or Gold facts.

The original Gold layer contained one daily aggregate and did not define dimensions,
fact grains, surrogate keys, conformed measures or Uganda PDM insight use cases.

## Decision

Gold uses a hybrid star/snowflake dimensional model. Gold may use deterministic
surrogate keys; Raw, Bronze and Silver must not adopt them. Natural identifiers such
as `loan_id`, `beneficiary_id` and `transaction_id` remain on facts as traceability
and drill-through attributes.

Surrogate keys are MD5 hashes of canonical natural-key values with explicit null
handling. They are integration keys, not security controls. Every conformed dimension
contains an Unknown member so incomplete relationships do not remove facts.

### Conformed dimensions

| Model | Grain | Purpose |
| --- | --- | --- |
| `dim_pdm_date` | One calendar date | Shared calendar attributes |
| `dim_pdm_geography` | Region, district, parish, village | Uganda administrative drill-down |
| `dim_pdm_special_group` | PDM special-group code | Inclusion analysis |
| `dim_pdm_beneficiary` | One beneficiary | Beneficiary and verification attributes |
| `dim_pdm_sacco` | One SACCO | Parish SACCO oversight |
| `dim_pdm_agent` | One agent | Agent identity, network and primary location |

Geography and special group are snowflaked from beneficiary, SACCO and agent
dimensions because they are shared analytical axes.

### Atomic facts

| Model | Grain | Additive measures |
| --- | --- | --- |
| `fct_pdm_payments` | Source system + transaction | Payment amount/count, status-match counts |
| `fct_pdm_loans` | Loan | Requested, approved, disbursed, repaid, interest, outstanding |
| `fct_pdm_agent_cashouts` | Agent cash-out transaction | Cash-out amount/count and monitoring flags |
| `fct_pdm_payment_lifecycle` | Loan | Accumulating approval-to-repayment control stages and amount variances |

Facts join to conformed dimensions through Gold surrogate keys. They retain source
natural keys for audit traversal back through Silver to Kafka and MinIO lineage.

### Consumption metrics and insight marts

Stakeholder-facing outputs are materialized in the `consumption` schema and use the
`cns_` prefix. They consume Gold facts and dimensions; they do not reimplement source
integration or canonical entity matching.

| Model | Grain | Uganda PDM use case |
| --- | --- | --- |
| `cns_pdm_payments_daily_summary` | Date, payment source, currency | Payment volumes, values and reconciliation coverage |
| `cns_pdm_parish_performance` | Geography | Approved versus disbursed funds and parish repayment performance |
| `cns_pdm_agent_risk_indicators` | Agent and date | Rapid cash-out, amount excess, unmatched entities and unreconciled events |
| `cns_pdm_beneficiary_insights` | Beneficiary | Multiple-loan, verification, repayment and cash-out oversight |
| `cns_pdm_beneficiary_identity_alerts` | Beneficiary | Shared NIN/phone, multiple-loan and creditor-account substitution triage |
| `cns_pdm_lifecycle_exceptions` | Loan | Approved-to-repayment evidence, missing stages and amount variances |
| `cns_pdm_duplicate_fragmentation_alerts` | Beneficiary, loan, date, currency | Duplicate, cross-channel and fragmented instruction patterns |
| `cns_pdm_geographic_risk` | Geography | Peer-relative parish performance and identity-risk concentration |

Metric definitions:

- **Disbursement rate** = total amount disbursed / total amount approved.
- **Principal repayment rate** = total amount repaid / total amount disbursed.
- **Outstanding amount** = max(disbursed + interest charged - repaid, 0).
- **Rapid cash-out** = an agent cash-out zero or one day after loan approval.
- **Excess amount cash-out** = cash-out amount above the approved or disbursed loan amount.
- **Unreconciled payment** = a transaction with an unmatched status event.
- **Entity mismatch** = payment identifiers do not resolve consistently to canonical
  loan, beneficiary, SACCO, business plan or agent entities.
- **Account substitution** = the instructed creditor account differs from the registered
  beneficiary phone/account linked through the canonical loan.
- **Shared identity** = the same hashed NIN or phone appears against multiple canonical
  beneficiaries; cross-geography counts provide additional investigative context.

## PDM insight scenarios

The model supports the following initial Uganda PDM questions:

1. Which parishes have low disbursement or repayment rates despite high approvals?
2. Which SACCOs have material approved-to-disbursed gaps?
3. Are women, youth, PWD and other special groups receiving intended participation?
4. Which agents perform unusually rapid, excessive, unreconciled or identity-unmatched cash-outs?
5. Which payments fail to reconcile across PDMIS, ICMN, CPO, Wendi, MTN or Airtel?
6. Where do beneficiary, loan, SACCO and agent identifiers disagree across systems?

Risk bands are operational triage indicators. They must not be presented as proof of
fraud or used for adverse action without investigation, source evidence and appropriate
PDM governance review.

## Slowly changing dimensions

Version 1 uses Type-1 dimensions because Silver currently represents the latest trusted
entity state. A future Type-2 version requires source-effective timestamps, `valid_from`,
`valid_to`, `is_current`, and a separately versioned ADR. Historical Type-2 behavior must
not be inferred from Kafka ingestion time alone.

## Testing and governance

- Dimension surrogate keys are non-null and unique.
- Atomic fact keys are non-null and unique at their declared grains.
- Fact foreign keys are tested against conformed dimensions.
- Metric ratios use `nullif` to prevent division by zero.
- Personally identifiable fields are minimized; beneficiary NIN is exposed only as its
  Silver-provided hash in Gold.
- Indicator thresholds must be versioned when policy owners approve changes.
- Controls unsupported by an authoritative source must return `NOT_OBSERVABLE`; absence
  of evidence must never be presented as a passed control.

### Control coverage in version 1.2

| Control area | Implemented evidence | Remaining authoritative source requirement |
| --- | --- | --- |
| Identity and eligibility | Shared hashed NIN/phone, verification, multiple loans | Death registry, eligibility rules, programme membership and change history |
| Diversion | Registered beneficiary versus instructed creditor account | Intermediary ownership and destination-account history |
| Duplicate/fragmentation | Same entitlement/date, channel, count and amount patterns | Approved policy thresholds and exception approvals |
| Collusion | Shared identity/account, agent and geographic concentrations | Official assignments, device IDs and registration audit trail |
| Rapid movement | Approval-to-agent-cash-out interval | Wallet transfer graph and downstream destination ownership |
| Repayment/recovery | Loan-level repaid and outstanding totals | Repayment schedule/events, payer identity and recovery ledger |
| Geographic anomaly | Region peer z-scores and parish performance | Approved peer cohorts and policy thresholds |
| Lifecycle reconciliation | Approval, PAIN, PACS, CAMT, cash-out and repayment stages | Authoritative recovery and grievance events |

## Consequences

### Positive

- Consistent Consumption metrics across Metabase, reporting and authorised consumers.
- Fast drill-down from national to region, district, parish and village.
- Traceable decisions from Gold facts to canonical Silver records.
- Unknown members preserve completeness while exposing integration gaps.

### Negative

- Type-1 dimensions do not retain historical attribute changes.
- Hash surrogate keys are less readable than integer sequences.
- Risk thresholds require ongoing governance, calibration and false-positive review.
- Pre-aggregated marts add storage and require metric-contract discipline.

## Version history

| Version | Date | Change |
| --- | --- | --- |
| 1.2.0 | 2026-08-29 | Added stable lifecycle reconciliation, control coverage and explicit observability states |
| 1.1.0 | 2026-08-29 | Separated stakeholder metrics and risk views into Consumption with `cns_` names |
| 1.0.0 | 2026-08-29 | Initial dimensional Gold decision and Uganda PDM insight catalog |
