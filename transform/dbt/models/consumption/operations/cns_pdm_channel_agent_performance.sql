{{ config(materialized='iceberg_table', tags=['consumption', 'channel', 'agent']) }}

-- Grain: one row per mapped agent. Only AGENT-source cash-outs have a
-- supported agent assignment, so no synthetic channel/agent mapping is made.
select
    agent.agent_sk,
    agent.geography_sk,
    agent.agent_id,
    agent.agent_code,
    agent.agent_name,
    agent.network_provider,
    agent.verified,
    agent.is_active,
    'AGENT' as source_system,
    count(cashout.cashout_sk) as payment_count,
    count(distinct cashout.beneficiary_sk) as beneficiary_count,
    sum(cashout.cashout_amount) as payment_amount,
    avg(cashout.cashout_amount) as average_payment_amount,
    count(cashout.cashout_sk) filter (where cashout.transaction_status in ('ACSC', 'COMPLETED', 'SUCCESSFUL')) as successful_payment_count,
    count(cashout.cashout_sk) filter (where cashout.transaction_status in ('RJCT', 'FAILED')) as failed_payment_count,
    count(cashout.cashout_sk) filter (where cashout.is_reconciled) as reconciled_payment_count,
    count(cashout.cashout_sk) filter (where not cashout.is_reconciled) as unreconciled_payment_count,
    cast(count(cashout.cashout_sk) filter (where cashout.transaction_status in ('ACSC', 'COMPLETED', 'SUCCESSFUL')) as double)
        / nullif(cast(count(cashout.cashout_sk) as double), 0.0) as payment_success_rate,
    cast(count(cashout.cashout_sk) filter (where cashout.is_reconciled) as double)
        / nullif(cast(count(cashout.cashout_sk) as double), 0.0) as reconciliation_rate,
    count(cashout.cashout_sk) filter (
        where cashout.transaction_status in ('RJCT', 'FAILED') or not cashout.is_reconciled
           or not cashout.is_entity_matched or cashout.exceeds_approved_amount
    ) as exception_count,
    count(cashout.cashout_sk) filter (where cashout.days_from_loan_approval between 0 and 1) as rapid_cashout_count,
    count(cashout.cashout_sk) filter (where cashout.exceeds_approved_amount) as excess_amount_cashout_count,
    case
        when not agent.verified or not agent.is_active then 'HIGH'
        when count(cashout.cashout_sk) filter (where not cashout.is_reconciled or cashout.exceeds_approved_amount) > 0 then 'HIGH'
        when count(cashout.cashout_sk) filter (where cashout.days_from_loan_approval between 0 and 1) > 0 then 'MEDIUM'
        else 'LOW'
    end as channel_risk_band
from {{ ref('gld_dim_pdm_agent') }} agent
left join {{ ref('gld_fct_pdm_agent_cashouts') }} cashout using (agent_sk)
where agent.agent_id is not null
group by 1, 2, 3, 4, 5, 6, 7, 8
