{{ config(materialized='iceberg_table') }}

with dates as (
    select cast(occurred_at as date) as calendar_date from {{ ref('slv_pdm_payments_transactions') }}
    union
    select cast(application_date as date) from {{ ref('slv_pdm_loans') }}
    union
    select cast(approval_date as date) from {{ ref('slv_pdm_loans') }}
    union
    select cast(registration_date as date) from {{ ref('slv_pdm_beneficiaries') }}
)
select
    {{ gold_surrogate_key(['calendar_date']) }} as date_sk,
    calendar_date,
    year(calendar_date) as calendar_year,
    quarter(calendar_date) as calendar_quarter,
    month(calendar_date) as month_number,
    monthname(calendar_date) as month_name,
    weekofyear(calendar_date) as iso_week_number,
    day(calendar_date) as day_of_month,
    dayofweek(calendar_date) as day_of_week,
    dayname(calendar_date) as day_name,
    dayofweek(calendar_date) in (0, 6) as is_weekend
from dates
where calendar_date is not null
union all
select
    {{ gold_surrogate_key(["cast(null as date)"]) }}, null, null, null, null,
    'Unknown', null, null, null, 'Unknown', false
