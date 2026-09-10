{{ config(materialized='iceberg_table', tags=['consumption', 'finance', 'operations']) }}

with fund_flow as (
    select
        sum(approved_amount) as approved_amount,
        sum(instructed_amount) as instructed_amount,
        sum(settled_amount) as settled_amount,
        sum(credited_amount) as credited_amount,
        sum(cashout_amount) as cashout_amount,
        count(*) filter (where approved_control_status = 'OBSERVED') as approved_count,
        count(*) filter (where instructed_control_status = 'OBSERVED') as instructed_count,
        count(*) filter (where settled_control_status = 'OBSERVED') as settled_count,
        count(*) filter (where credited_control_status = 'OBSERVED') as credited_count,
        count(*) filter (where cashout_control_status = 'OBSERVED') as cashout_count
    from {{ ref('cns_pdm_financial_fund_flow') }}
)
select 1 as stage_order, 'Approved' as stage, approved_amount as amount, approved_count as count from fund_flow
union all
select 2, 'Instructed', instructed_amount, instructed_count from fund_flow
union all
select 3, 'Settled', settled_amount, settled_count from fund_flow
union all
select 4, 'Credited', credited_amount, credited_count from fund_flow
union all
select 5, 'Cash-out', cashout_amount, cashout_count from fund_flow
