{{ config(materialized='iceberg_table', tags=['consumption', 'risk', 'investigation', 'lifecycle']) }}

-- Grain: one row per loan, lifecycle stage and exception type. Dates are only
-- populated where a genuine source event date exists.
with base as (
    select * from {{ ref('cns_pdm_lifecycle_exceptions') }}
), cases as (
    select *, 'PAYMENT_INSTRUCTION' as lifecycle_stage,
        'MISSING_STAGE' as exception_type,
        'No payment instruction observed' as exception_reason,
        'MEDIUM' as exception_severity,
        cast(null as timestamp) as event_date,
        instructed_control_status as current_status
    from base where instructed_control_status = 'MISSING'
    union all
    select *, 'PAYMENT_SETTLEMENT', 'SETTLEMENT_EXCEPTION',
        case when settled_control_status = 'REJECTED' then 'Settlement rejected'
             else 'No successful settlement observed' end,
        case when settled_control_status = 'REJECTED' then 'HIGH' else 'MEDIUM' end,
        cast(null as timestamp), settled_control_status
    from base where settled_control_status in ('MISSING', 'REJECTED')
    union all
    select *, 'CREDITING', 'MISSING_STAGE', 'No credit notification observed',
        'MEDIUM', cast(first_credited_at as timestamp), credited_control_status
    from base where credited_control_status = 'MISSING'
    union all
    select *, 'REPAYMENT', 'MISSING_STAGE', 'No repayment observed',
        'MEDIUM', cast(null as timestamp), repaid_control_status
    from base where repaid_control_status = 'MISSING'
    union all
    select *, 'PAYMENT_INSTRUCTION', 'AMOUNT_VARIANCE',
        'Instructed amount differs from approved amount', 'HIGH',
        cast(null as timestamp), instructed_control_status
    from base where coalesce(approved_to_instructed_variance <> 0, false)
    union all
    select *, 'PAYMENT_SETTLEMENT', 'AMOUNT_VARIANCE',
        'Settled amount differs from instructed amount', 'HIGH',
        cast(null as timestamp), settled_control_status
    from base where coalesce(instructed_to_settled_variance <> 0, false)
    union all
    select *, 'CREDITING', 'AMOUNT_VARIANCE',
        'Credited amount differs from settled amount', 'HIGH',
        cast(first_credited_at as timestamp), credited_control_status
    from base where coalesce(settled_to_credited_variance <> 0, false)
    union all
    select *, 'DISBURSEMENT', 'AMOUNT_VARIANCE',
        'Credited amount differs from recorded disbursement', 'HIGH',
        cast(first_credited_at as timestamp), credited_control_status
    from base where coalesce(disbursed_to_credited_variance <> 0, false)
    union all
    select *, 'CASHOUT', 'AMOUNT_VARIANCE',
        'Cash-out amount exceeds credited amount', 'HIGH',
        cast(first_cashout_at as timestamp), cashout_control_status
    from base where coalesce(credited_to_cashout_variance > 0, false)
)
select
    lifecycle_sk,
    loan_id,
    beneficiary_token,
    sacco_id,
    region,
    district,
    lifecycle_stage,
    exception_type,
    exception_reason,
    exception_severity,
    event_date,
    current_status,
    true as requires_review,
    'INDICATOR_NOT_FRAUD_DETERMINATION' as interpretation
from cases
