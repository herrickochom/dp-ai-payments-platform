{{ config(
    materialized='iceberg_table',
    tags=['consumption', 'executive', 'dashboard', 'geography', 'drilldown', 'privacy-boundary']
) }}

-- One row per beneficiary/loan combination. This atomic grain lets Superset
-- aggregate the same measures at every level without joining separate rollups.
-- GATE 2 PRIVACY BOUNDARY: pseudonymous by default (beneficiary_token; only
-- the approved coarse geography levels region/district/county/sub_county are
-- carried from the pseudonymous beneficiary dimension).
with identity_risk as (
    select
        beneficiary_token,
        identity_alert_count,
        identity_risk_band
    from {{ ref('vlt_pdm_beneficiary_identity_signals') }}
), drill_rows as (
    select
        beneficiary.region,
        beneficiary.district,
        beneficiary.county,
        beneficiary.sub_county,
        sacco.sacco_sk,
        sacco.sacco_id,
        sacco.sacco_name,
        beneficiary.beneficiary_sk,
        beneficiary.beneficiary_token,
        loan.loan_sk,
        loan.loan_id,
        coalesce(loan.amount_approved, 0) as approved_amount,
        coalesce(loan.amount_disbursed, 0) as disbursed_amount,
        coalesce(loan.amount_repaid, 0) as repaid_amount,
        coalesce(loan.principal_repaid, 0) as principal_repaid_amount,
        coalesce(loan.outstanding_amount, 0) as outstanding_amount,
        -- Principal-based repayment (decisions 1-2): principal repaid against
        -- principal disbursed; amount_repaid includes interest and is never
        -- divided by the principal denominator.
        coalesce(loan.principal_repaid, 0)
            / nullif(coalesce(loan.amount_disbursed, 0), 0) as principal_repayment_rate,
        1 as beneficiary_count,
        case when loan.loan_id is null then 0 else 1 end as loan_count,
        coalesce(identity_risk.identity_alert_count, 0) as risk_indicator_count,
        coalesce(identity_risk.identity_risk_band, 'LOW') as risk_band
    from {{ ref('gld_dim_pdm_beneficiary') }} beneficiary
    left join {{ ref('gld_fct_pdm_loans') }} loan using (beneficiary_sk)
    left join {{ ref('gld_dim_pdm_sacco') }} sacco
      on loan.sacco_sk = sacco.sacco_sk
    left join identity_risk
      on beneficiary.beneficiary_token = identity_risk.beneficiary_token
    where beneficiary.beneficiary_token is not null
)

select * from drill_rows
