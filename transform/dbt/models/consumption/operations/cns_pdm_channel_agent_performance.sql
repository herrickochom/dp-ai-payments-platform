{{ config(materialized='pdm_iceberg_guarded', tags=['consumption', 'channel', 'agent']) }}

-- Grain: one row per mapped agent. Only AGENT-source cash-outs have a
-- supported agent assignment, so no synthetic channel/agent mapping is made.
select
    agent_sk,
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
    COUNT(IF(cashout.transaction_status in ('ACSC', 'COMPLETED', 'SUCCESSFUL'), cashout.cashout_sk, NULL)) as successful_payment_count,
    COUNT(IF(cashout.transaction_status in ('RJCT', 'FAILED'), cashout.cashout_sk, NULL)) as failed_payment_count,
    COUNT(IF(cashout.is_reconciled, cashout.cashout_sk, NULL)) as reconciled_payment_count,
    COUNT(IF(not cashout.is_reconciled, cashout.cashout_sk, NULL)) as unreconciled_payment_count,
    cast(COUNT(IF(cashout.transaction_status in ('ACSC', 'COMPLETED', 'SUCCESSFUL'), cashout.cashout_sk, NULL)) as float64)
        / nullif(cast(count(cashout.cashout_sk) as float64), 0.0) as payment_success_rate,
    cast(COUNT(IF(cashout.is_reconciled, cashout.cashout_sk, NULL)) as float64)
        / nullif(cast(count(cashout.cashout_sk) as float64), 0.0) as reconciliation_rate,
    COUNT(IF(cashout.transaction_status in ('RJCT', 'FAILED') or not cashout.is_reconciled
           or not cashout.is_entity_matched or cashout.exceeds_approved_amount, cashout.cashout_sk, NULL)) as exception_count,
    COUNT(IF(cashout.days_from_loan_approval between 0 and 1, cashout.cashout_sk, NULL)) as rapid_cashout_count,
    COUNT(IF(cashout.exceeds_approved_amount, cashout.cashout_sk, NULL)) as excess_amount_cashout_count,
    case
        when not agent.verified or not agent.is_active then 'HIGH'
        when COUNT(IF(not cashout.is_reconciled or cashout.exceeds_approved_amount, cashout.cashout_sk, NULL)) > 0 then 'HIGH'
        when COUNT(IF(cashout.days_from_loan_approval between 0 and 1, cashout.cashout_sk, NULL)) > 0 then 'MEDIUM'
        else 'LOW'
    end as channel_risk_band
from {{ ref('gld_dim_pdm_agent') }} agent
left join {{ ref('gld_fct_pdm_agent_cashouts') }} cashout using (agent_sk)
where agent.agent_id is not null
group by 1, 2, 3, 4, 5, 6, 7, 8
