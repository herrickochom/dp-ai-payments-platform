{{ config(materialized='iceberg_table', tags=['consumption', 'risk', 'payments']) }}

with entitlements as (
    select
        payment.beneficiary_sk,
        payment.loan_id,
        payment.payment_date_sk,
        payment.currency,
        count(*) as instruction_count,
        count(distinct payment.source_system) as instruction_channel_count,
        count(distinct payment.payment_amount) as distinct_amount_count,
        sum(payment.payment_amount) as total_instructed_amount,
        min(payment.payment_amount) as minimum_instruction_amount,
        max(payment.payment_amount) as maximum_instruction_amount
    from {{ ref('fct_pdm_payments') }} payment
    where payment.source_system in ('ICMN_VPM', 'WENDI_PAIN001')
    group by 1, 2, 3, 4
)
select
    {{ gold_surrogate_key(['entitlements.beneficiary_sk', 'entitlements.loan_id', 'entitlements.payment_date_sk', 'entitlements.currency']) }} as payment_pattern_sk,
    entitlements.*,
    loan.amount_approved,
    entitlements.instruction_count > 1 as has_duplicate_entitlement,
    entitlements.instruction_channel_count > 1 as has_cross_channel_submission,
    entitlements.instruction_count > 1
        and entitlements.maximum_instruction_amount < loan.amount_approved
        and entitlements.total_instructed_amount >= loan.amount_approved as has_fragmented_payment,
    entitlements.instruction_count > 1
        and entitlements.distinct_amount_count = 1 as has_repeated_equal_amount,
    case
        when entitlements.instruction_count > 1
         and (entitlements.instruction_channel_count > 1
           or entitlements.total_instructed_amount > loan.amount_approved) then 'HIGH'
        when entitlements.instruction_count > 1 then 'MEDIUM'
        else 'LOW'
    end as payment_pattern_risk_band
from entitlements
left join {{ ref('slv_pdm_loans') }} loan using (loan_id)
