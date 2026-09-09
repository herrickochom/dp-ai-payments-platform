{{ config(
    materialized='iceberg_table',
    tags=['consumption', 'leadership', 'executive', 'intervention', 'ai']
) }}

with geographic as (

    select
        region,
        district,

        parish_count,
        assessed_parish_count,
        severe_parish_count,
        high_parish_count,
        medium_parish_count,
        low_parish_count,

        loan_count,
        beneficiary_count,

        approved_amount,
        disbursed_amount,
        repaid_amount,
        outstanding_amount,

        geographic_risk_score,
        map_risk_score,
        geographic_risk_band,

        avg_disbursement_rate,
        avg_principal_repayment_rate,

        high_identity_alert_count,
        account_substitution_amount,
        mapped_agent_count

    from {{ ref('cns_pdm_district_geographic_risk') }}

),

ai as (

    select
        observation_date,
        region,
        district,

        ai_scored_loan_count,
        avg_probability_default_90d,
        max_probability_default_90d,

        ai_low_risk_count,
        ai_medium_risk_count,
        ai_high_risk_count,
        ai_severe_risk_count,

        ai_priority_review_count,
        ai_priority_review_rate,

        highest_ai_risk_rank,
        model_name,
        model_version

    from {{ ref('cns_pdm_district_ai_risk') }}

),

combined as (

    select
        coalesce(g.region, a.region) as region,
        coalesce(g.district, a.district) as district,

        a.observation_date,

        g.parish_count,
        g.assessed_parish_count,
        g.severe_parish_count,
        g.high_parish_count,
        g.medium_parish_count,
        g.low_parish_count,

        g.loan_count,
        g.beneficiary_count,

        g.approved_amount,
        g.disbursed_amount,
        g.repaid_amount,
        g.outstanding_amount,

        g.avg_disbursement_rate,
        g.avg_principal_repayment_rate,

        g.geographic_risk_score,
        g.map_risk_score,
        g.geographic_risk_band,

        g.high_identity_alert_count,
        g.account_substitution_amount,
        g.mapped_agent_count,

        coalesce(a.ai_scored_loan_count, 0)
            as ai_scored_loan_count,

        coalesce(a.avg_probability_default_90d, 0.0)
            as avg_probability_default_90d,

        coalesce(a.max_probability_default_90d, 0.0)
            as max_probability_default_90d,

        coalesce(a.ai_low_risk_count, 0)
            as ai_low_risk_count,

        coalesce(a.ai_medium_risk_count, 0)
            as ai_medium_risk_count,

        coalesce(a.ai_high_risk_count, 0)
            as ai_high_risk_count,

        coalesce(a.ai_severe_risk_count, 0)
            as ai_severe_risk_count,

        coalesce(a.ai_priority_review_count, 0)
            as ai_priority_review_count,

        coalesce(a.ai_priority_review_rate, 0.0)
            as ai_priority_review_rate,

        a.highest_ai_risk_rank,
        a.model_name,
        a.model_version

    from geographic g

    full outer join ai a
        on g.region = a.region
       and g.district = a.district

),

scored as (

    select
        *,

        case
            when ai_priority_review_rate >= 0.50 then 'CRITICAL'
            when ai_priority_review_rate >= 0.25 then 'HIGH'
            when ai_priority_review_rate >= 0.10 then 'MEDIUM'
            else 'LOW'
        end as ai_intervention_band,

        (
            coalesce(map_risk_score, 0) * 20.0 * 0.50
            +
            avg_probability_default_90d * 100.0 * 0.50
        ) as intervention_priority_score

    from combined

)

select
    *,

    case
        when intervention_priority_score >= 75
            then 'IMMEDIATE INTERVENTION'

        when intervention_priority_score >= 50
            then 'PRIORITY REVIEW'

        when intervention_priority_score >= 25
            then 'MONITOR'

        else 'NORMAL'
    end as recommended_action,

    row_number() over (
        order by
            intervention_priority_score desc,
            ai_priority_review_count desc,
            outstanding_amount desc nulls last
    ) as intervention_rank,

    'COMBINED_GEOGRAPHIC_AND_PREDICTIVE_DEFAULT_RISK'
        as interpretation

from scored
