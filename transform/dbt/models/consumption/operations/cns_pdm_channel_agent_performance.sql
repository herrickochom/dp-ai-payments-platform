{{ config(materialized='iceberg_table', tags=['consumption', 'channel', 'agent']) }}

select
    agent.agent_sk,
    agent.geography_sk,
    agent.agent_id,
    agent.agent_code,
    agent.agent_name,
    agent.network_provider,
    agent.verified,
    agent.is_active,
    count(cashout.cashout_sk) as cashout_count,
    count(distinct cashout.beneficiary_sk) as beneficiary_count,
    sum(cashout.cashout_amount) as total_cashout_amount,
    avg(cashout.cashout_amount) as average_cashout_amount,
    count(*) filter (where not cashout.is_reconciled) as unreconciled_cashout_count,
    count(*) filter (where cashout.days_from_loan_approval between 0 and 1) as rapid_cashout_count,
    count(*) filter (where cashout.exceeds_approved_amount) as excess_amount_cashout_count,
    case when not agent.verified or not agent.is_active then 'HIGH'
         when count(*) filter (where not cashout.is_reconciled) > 0
           or count(*) filter (where cashout.exceeds_approved_amount) > 0 then 'HIGH'
         when count(*) filter (where cashout.days_from_loan_approval between 0 and 1) > 0 then 'MEDIUM'
         else 'LOW' end as channel_risk_band
from {{ ref('gld_dim_pdm_agent') }} agent
left join {{ ref('gld_fct_pdm_agent_cashouts') }} cashout using (agent_sk)
where agent.agent_id is not null
group by 1, 2, 3, 4, 5, 6, 7, 8
