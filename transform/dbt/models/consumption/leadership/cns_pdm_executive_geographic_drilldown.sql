{{ config(
    materialized='iceberg_table',
    tags=['consumption', 'executive', 'dashboard', 'geography', 'drilldown']
) }}

-- One row per beneficiary/loan combination. This atomic grain lets Superset
-- aggregate the same measures at every level without joining separate rollups.
with identity_risk as (
    select
        beneficiary_sk,
        identity_alert_count,
        identity_risk_band
    from {{ ref('cns_pdm_beneficiary_identity_alerts') }}
), drill_rows as (
    select
        geography.geography_sk,
        geography.district_sk,
        geography.county_sk,
        geography.sub_county_sk,
        geography.parish_sk,
        geography.village_sk,
        geography.country_name,
        geography.region,
        geography.district,
        geography.county,
        geography.sub_county,
        geography.parish,
        geography.village,
        sacco.sacco_sk,
        sacco.sacco_id,
        sacco.sacco_name,
        beneficiary.beneficiary_sk,
        beneficiary.beneficiary_id,
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
    inner join {{ ref('gld_dim_pdm_geography') }} geography using (geography_sk)
    left join {{ ref('gld_fct_pdm_loans') }} loan using (beneficiary_sk)
    left join {{ ref('gld_dim_pdm_sacco') }} sacco
      on loan.sacco_sk = sacco.sacco_sk
    left join identity_risk using (beneficiary_sk)
    where beneficiary.beneficiary_id is not null
)

select *
from drill_rows
