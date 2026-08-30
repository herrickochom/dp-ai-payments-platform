{{ config(materialized='iceberg_table', tags=['consumption', 'risk']) }}

select
    agent_sk, cashout_date_sk,
    count(*) as cashout_count,
    count(distinct beneficiary_sk) as beneficiary_count,
    sum(cashout_amount) as total_cashout_amount,
    avg(cashout_amount) as average_cashout_amount,
    max(cashout_amount) as maximum_cashout_amount,
    sum(case when not is_reconciled then 1 else 0 end) as unreconciled_cashout_count,
    sum(case when not is_entity_matched then 1 else 0 end) as unmatched_entity_cashout_count,
    sum(case when exceeds_approved_amount then 1 else 0 end) as excess_amount_cashout_count,
    sum(case when days_from_loan_approval between 0 and 1 then 1 else 0 end) as rapid_cashout_count,
    bool_or(coalesce(not agent_verified, true)) as has_unverified_agent,
    case
        when bool_or(coalesce(not agent_verified, true))
          or sum(case when exceeds_approved_amount then 1 else 0 end) > 0
          or sum(case when not is_reconciled then 1 else 0 end) > 0 then 'HIGH'
        when sum(case when days_from_loan_approval between 0 and 1 then 1 else 0 end) > 0 then 'MEDIUM'
        else 'LOW'
    end as risk_band
from {{ ref('gld_fct_pdm_agent_cashouts') }}
group by 1, 2
