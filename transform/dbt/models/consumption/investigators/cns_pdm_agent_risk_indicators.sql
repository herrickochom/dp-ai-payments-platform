{{ config(materialized='iceberg_table', tags=['consumption', 'risk']) }}

select
    cashout.agent_sk,
    cashout.cashout_date_sk,
    agent.agent_id,
    geography.region,
    geography.district,
    count(*) as cashout_count,
    count(distinct beneficiary_sk) as beneficiary_count,
    sum(cashout_amount) as total_cashout_amount,
    avg(cashout_amount) as average_cashout_amount,
    max(cashout_amount) as maximum_cashout_amount,
    sum(case when not is_reconciled then 1 else 0 end) as unreconciled_cashout_count,
    sum(case when not is_reconciled then 1 else 0 end) as reconciliation_exception_count,
    sum(case when not is_entity_matched then 1 else 0 end) as unmatched_entity_cashout_count,
    sum(case when exceeds_approved_amount then 1 else 0 end) as excess_amount_cashout_count,
    sum(case when days_from_loan_approval between 0 and 1 then 1 else 0 end) as rapid_cashout_count,
    bool_or(coalesce(not agent_verified, true)) as has_unverified_agent,
    (case when bool_or(coalesce(not agent_verified, true)) then 25 else 0 end
     + case when sum(case when exceeds_approved_amount then 1 else 0 end) > 0 then 25 else 0 end
     + case when sum(case when not is_reconciled then 1 else 0 end) > 0 then 25 else 0 end
     + case when sum(case when not is_entity_matched then 1 else 0 end) > 0 then 15 else 0 end
     + case when sum(case when days_from_loan_approval between 0 and 1 then 1 else 0 end) > 0 then 10 else 0 end
    ) as risk_score,
    (case when bool_or(coalesce(not agent_verified, true)) then 1 else 0 end
     + case when sum(case when exceeds_approved_amount then 1 else 0 end) > 0 then 1 else 0 end
     + case when sum(case when not is_reconciled then 1 else 0 end) > 0 then 1 else 0 end
     + case when sum(case when not is_entity_matched then 1 else 0 end) > 0 then 1 else 0 end
     + case when sum(case when days_from_loan_approval between 0 and 1 then 1 else 0 end) > 0 then 1 else 0 end
    ) as indicator_count,
    concat_ws('; ',
        case when bool_or(coalesce(not agent_verified, true)) then 'UNVERIFIED_AGENT' end,
        case when sum(case when exceeds_approved_amount then 1 else 0 end) > 0 then 'AMOUNT_EXCEEDS_APPROVAL' end,
        case when sum(case when not is_reconciled then 1 else 0 end) > 0 then 'RECONCILIATION_EXCEPTION' end,
        case when sum(case when not is_entity_matched then 1 else 0 end) > 0 then 'ENTITY_MATCH_EXCEPTION' end,
        case when sum(case when days_from_loan_approval between 0 and 1 then 1 else 0 end) > 0 then 'RAPID_CASHOUT' end
    ) as indicator_reason,
    case
        when bool_or(coalesce(not agent_verified, true))
          or sum(case when exceeds_approved_amount then 1 else 0 end) > 0
          or sum(case when not is_reconciled then 1 else 0 end) > 0 then 'HIGH'
        when sum(case when days_from_loan_approval between 0 and 1 then 1 else 0 end) > 0 then 'MEDIUM'
        else 'LOW'
    end as risk_band,
    (bool_or(coalesce(not agent_verified, true))
      or sum(case when exceeds_approved_amount then 1 else 0 end) > 0
      or sum(case when not is_reconciled then 1 else 0 end) > 0
      or sum(case when not is_entity_matched then 1 else 0 end) > 0
      or sum(case when days_from_loan_approval between 0 and 1 then 1 else 0 end) > 0) as requires_review,
    'INDICATOR_NOT_FRAUD_DETERMINATION' as interpretation
from {{ ref('gld_fct_pdm_agent_cashouts') }} cashout
left join {{ ref('gld_dim_pdm_agent') }} agent using (agent_sk)
left join {{ ref('gld_dim_pdm_geography') }} geography
  on cashout.geography_sk = geography.geography_sk
group by 1, 2, 3, 4, 5
