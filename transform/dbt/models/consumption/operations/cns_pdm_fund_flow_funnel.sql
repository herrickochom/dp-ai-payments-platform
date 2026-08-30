{{ config(materialized='iceberg_table') }}

with fund_flow as (

    select
        sum(approved_amount) as approved_amount,
        sum(instructed_amount) as instructed_amount,
        sum(settled_amount) as settled_amount,
        sum(credited_amount) as credited_amount,
        sum(cashout_amount) as cashout_amount
    from {{ ref('cns_pdm_financial_fund_flow') }}

)

select 1 as stage_order, 'Approved' as stage, approved_amount as amount
from fund_flow

union all

select 2, 'Instructed', instructed_amount
from fund_flow

union all

select 3, 'Settled', settled_amount
from fund_flow

union all

select 4, 'Credited', credited_amount
from fund_flow

union all

select 5, 'Cash-out', cashout_amount
from fund_flow
