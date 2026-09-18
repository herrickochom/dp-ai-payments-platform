# ADR-002: PDM Dimensional Gold and Consumption Analytics

**Version:** 1.3.0
**Date:** 2026-09-04
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
| `gld_dim_pdm_date` | One calendar date | Shared calendar attributes |
| `gld_dim_pdm_geography` | Region, district, parish, village (target: region → district → county → sub-county → parish → village) | Uganda administrative drill-down |
| `gld_dim_pdm_special_group` | PDM special-group code | Inclusion analysis |
| `gld_dim_pdm_beneficiary` | One beneficiary | Beneficiary and verification attributes |
| `gld_dim_pdm_sacco` | One SACCO | Parish SACCO oversight |
| `gld_dim_pdm_agent` | One agent | Agent identity, network and primary location |

Geography and special group are snowflaked from beneficiary, SACCO and agent
dimensions because they are shared analytical axes. The geography chain follows
the Uganda administrative hierarchy defined in the next section.

### Geography snowflake hierarchy

The core warehouse models Uganda administrative geography as a normalised
snowflake. Each level is its own conformed dimension carrying a surrogate key,
a stable administrative code, a display name and its parent foreign key:

```text
dim_region        region_key, region_code, region_name
dim_district      district_key, district_code, district_name, region_key
dim_county        county_key, county_code, county_name, district_key
dim_sub_county    sub_county_key, sub_county_code, sub_county_name, county_key
dim_parish        parish_key, parish_code, parish_name, sub_county_key
dim_village       village_key, village_code, village_name, parish_key
```

The snowflake stays in Gold because it gives clean governance over
administrative hierarchy changes, stable identity across renames and
reorganisations, and no duplicated geography attributes. It is not exposed to
dashboards directly; Superset reads the flattened Consumption marts below so
no query pays six joins to answer "total disbursement by region".

```text
             CORE WAREHOUSE (snowflake)

dim_region
    └ dim_district
        └ dim_county
            └ dim_sub_county
                └ dim_parish
                    └ dim_village
                        │
        gld_fct_pdm_payments, gld_fct_pdm_loans,
        gld_fct_pdm_agent_cashouts, gld_fct_pdm_payment_lifecycle
                        │
                        ▼
                dbt Consumption marts
                        │
                        ▼
          flattened analytical datasets
                        │
                        ▼
                    Superset
```

Target state versus current implementation: version 1 ships the single flat
`gld_dim_pdm_geography` at region, district, parish and village grain, sourced
from Silver beneficiary, SACCO and agent rows. The six-table hierarchy above is
the target Gold design, introduced level by level as authoritative
administrative codes land in the sources. County is a legal administrative
level but no platform source currently carries a county attribute (see
`docs/architecture/future-models/pdm-county-geographic-risk.md`), so its dimension
is defined now and populated when a county attribute lands. Sub-county is
carried only by the SACCO source today.

#### Codes are identity, names are attributes

Administrative identity lives in codes, never in names. Names are display
attributes: they get renamed, duplicated, misspelled and reassigned over time.
Every geography join in Gold and Consumption resolves through surrogate keys
and administrative codes; name-based joins such as `district_name = 'Soroti'`
are forbidden. Source extracts must carry stable administrative codes
(`region_code`, `district_code`, `county_code`, `sub_county_code`,
`parish_code`, `village_code`) and Consumption marts must expose codes
alongside names so Superset filters bind to codes.

### Atomic facts

| Model | Grain | Additive measures |
| --- | --- | --- |
| `gld_fct_pdm_payments` | Source system + transaction | Payment amount/count, status-match counts |
| `gld_fct_pdm_loans` | Loan | Requested, approved, disbursed, repaid, interest, outstanding |
| `gld_fct_pdm_agent_cashouts` | Agent cash-out transaction | Cash-out amount/count and monitoring flags |
| `gld_fct_pdm_payment_lifecycle` | Loan | Accumulating approval-to-repayment control stages and amount variances |

Facts join to conformed dimensions through Gold surrogate keys. They retain source
natural keys for audit traversal back through Silver to Kafka and MinIO lineage.

#### Geography foreign keys on facts

Facts carry the lowest valid geography foreign key rather than one column per
level. `gld_fct_pdm_payments` and `gld_fct_pdm_loans` resolve `geography_sk`
at the finest geography known for the event — village when known, else parish,
else district, else the Unknown member — and every higher level rolls up
through the hierarchy:

```text
village → parish → sub-county → county → district → region
```

The hierarchy is not denormalised onto facts; facts stay lean and the
Consumption marts carry the flattened geography columns dashboards need.

The wider fact catalogue anticipates dedicated repayment, reconciliation, risk
and intervention grains. Version 1 keeps repayment measures on
`gld_fct_pdm_loans`, reconciliation coverage on `cns_pdm_payments_daily_summary`
and risk indicators in Consumption until event-level sources justify separate
atomic facts.

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
| `cns_pdm_parish_geographic_risk` | Geography | Peer-relative parish performance, identity-risk concentration and map coordinates |
| `cns_pdm_district_geographic_risk` | Geography (district) | District rollup of parish risk driving the Superset Uganda district map |
| `cns_pdm_subcounty_geographic_risk` | Geography (sub-county) | SACCO-office attributed sub-county risk; the only platform source carrying sub_county |
| `cns_pdm_village_geographic_risk` | Geography (village) | Village rows inheriting parish risk until village-grain facts exist |
| `cns_pdm_geographic_risk_summary` | Snapshot | National KPI row: district, sub-county and parish counts by severity band |
| `cns_pdm_geographic_risk_drivers` | Geography (parish) | Dominant risk driver per parish: disbursement, repayment or identity |
| `cns_pdm_geographic_risk_trend` | Snapshot, geography (district) | Daily district risk snapshot for movement-over-time analysis |
| `cns_pdm_geographic_coverage` | Geography (district) | ISO, coordinate and agent GPS mapping coverage |
| `cns_pdm_geographic_alerts` | Geography (parish) | Prioritised parish exception list for operational triage |

### Flattened Superset marts

Superset never queries the snowflake directly. The Consumption layer generates
flattened, star-style reporting datasets so dashboard questions such as total
disbursement by region are answered without hierarchy joins. The deliberate
split is: snowflake in the warehouse for integrity, flattened marts for
analytics and dashboard speed.

A conformed flattened geography dimension is generated by dbt from the
snowflake. It is analytics-friendly and does not replace the normalised
tables:

```text
cns_pdm_geography
-----------------
geography_key
geography_level
region_code, region_name
district_code, district_name
county_code, county_name
sub_county_code, sub_county_name
parish_code, parish_name
village_code, village_name
latitude, longitude, geometry
effective_from, effective_to, is_current
```

`effective_from`, `effective_to` and `is_current` are reserved for the Type-2
geography evolution under Slowly changing dimensions; version 1 materialises
the dimension Type-1 with `is_current = true`. Superset native filters and
drill-downs bind to exactly these fields:

```text
Region → District → County → Sub County → Parish → Village → Beneficiary
```

The conceptual Superset mart catalogue maps to implemented Consumption models
as follows. Consumption keeps the `cns_` prefix; the `mart_pdm_*` names are
the dashboard datasets each group of models feeds:

| Reporting mart | Consumption models |
| --- | --- |
| `mart_pdm_executive` | `cns_pdm_executive_overview`, `cns_pdm_executive_monthly_trend`, `cns_pdm_social_impact` |
| `mart_pdm_geography` | `cns_pdm_district_geographic_risk`, `cns_pdm_subcounty_geographic_risk`, `cns_pdm_parish_geographic_risk`, `cns_pdm_village_geographic_risk`, `cns_pdm_geographic_risk_summary`, `cns_pdm_geographic_risk_drivers`, `cns_pdm_geographic_risk_trend`, `cns_pdm_geographic_alerts`, `cns_pdm_geographic_coverage` |
| `mart_pdm_fund_flow` | `cns_pdm_financial_fund_flow`, `cns_pdm_fund_flow_funnel`, `cns_pdm_payments_daily_summary`, `cns_pdm_payment_operations`, `cns_pdm_lifecycle_exceptions`, `cns_pdm_end_to_end_traceability` |
| `mart_pdm_repayment` | `cns_pdm_parish_performance`, `cns_pdm_local_government_performance` |
| `mart_pdm_sacco` | `cns_pdm_sacco_portfolio`, `cns_pdm_channel_agent_performance` |
| `mart_pdm_risk` | `cns_pdm_agent_risk_indicators`, `cns_pdm_fraud_risk_insights`, `cns_pdm_duplicate_fragmentation_alerts`, `cns_pdm_loan_intervention_dashboard` |
| `mart_pdm_beneficiary` | `cns_pdm_beneficiary_insights`, `cns_pdm_beneficiary_identity_alerts` |

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

Geography is the first Type-2 candidate because administrative reorganisations
(renames, boundary changes, level moves) are routine. The flattened geography
dimension already reserves `effective_from`, `effective_to` and `is_current`
for that migration.

## Testing and governance

- Dimension surrogate keys are non-null and unique.
- Atomic fact keys are non-null and unique at their declared grains.
- Fact foreign keys are tested against conformed dimensions.
- Geography joins resolve through surrogate keys and administrative codes;
  name-based geography joins fail review.
- Each snowflake geography level keeps a unique key and a tested parent
  foreign key once that level is implemented.
- Flattened geography marts reconcile to the snowflake on level counts and
  measure totals so the two representations cannot drift.
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
- Snowflake core keeps administrative geography governable while flattened
  Consumption marts keep Superset response times low.
- Traceable decisions from Gold facts to canonical Silver records.
- Unknown members preserve completeness while exposing integration gaps.

### Negative

- Type-1 dimensions do not retain historical attribute changes.
- Hash surrogate keys are less readable than integer sequences.
- Risk thresholds require ongoing governance, calibration and false-positive review.
- Pre-aggregated marts add storage and require metric-contract discipline.
- Geography exists twice by design — normalised snowflake in Gold and
  flattened marts in Consumption — so dbt tests must keep the two
  representations consistent.

## Version history

| Version | Date | Change |
| --- | --- | --- |
| 1.3.0 | 2026-09-04 | Added six-level geography snowflake, lowest-geography fact FK rule, flattened Superset marts and code-first geography identity |
| 1.2.0 | 2026-08-29 | Added stable lifecycle reconciliation, control coverage and explicit observability states |
| 1.1.0 | 2026-08-29 | Separated stakeholder metrics and risk views into Consumption with `cns_` names |
| 1.0.0 | 2026-08-29 | Initial dimensional Gold decision and Uganda PDM insight catalog |
