{{ config(materialized='iceberg_table', tags=['consumption', 'risk', 'payments', 'privacy-boundary']) }}

-- GATE 2 PRIVACY BOUNDARY: pseudonymous by default (beneficiary_token; coarse
-- geography only). Identity-theft signals come from the pseudonymous
-- per-token signal feed; the clear-identity correlators live only in the
-- RESTRICTED identity vault.

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
    from {{ ref('gld_fct_pdm_payments') }} payment
    -- Wendi PAIN.001 is a routed copy of ICMN VPM, not a second entitlement.
    where payment.source_system = 'ICMN_VPM'
    group by 1, 2, 3, 4
)
select
    {{ gold_surrogate_key(['entitlements.beneficiary_sk', 'entitlements.loan_id', 'entitlements.payment_date_sk', 'entitlements.currency']) }} as payment_pattern_sk,
    entitlements.beneficiary_sk,
    beneficiary.beneficiary_token,
    entitlements.loan_id,
    loan.sacco_sk,
    sacco.sacco_id,
    loan.region,
    loan.district,
    entitlements.payment_date_sk,
    entitlements.currency,
    entitlements.instruction_count,
    entitlements.instruction_channel_count,
    entitlements.distinct_amount_count,
    entitlements.total_instructed_amount,
    entitlements.minimum_instruction_amount,
    entitlements.maximum_instruction_amount,
    loan.amount_approved,
    entitlements.instruction_count > 1 as has_duplicate_entitlement,
    entitlements.instruction_count > 1 as duplicate_indicator,
    entitlements.instruction_channel_count > 1 as has_cross_channel_submission,
    entitlements.instruction_count > 1
        and entitlements.maximum_instruction_amount < loan.amount_approved
        and entitlements.total_instructed_amount >= loan.amount_approved as has_fragmented_payment,
    entitlements.instruction_count > 1
        and entitlements.maximum_instruction_amount < loan.amount_approved
        and entitlements.total_instructed_amount >= loan.amount_approved as fragmentation_indicator,
    entitlements.instruction_count > 1
        and entitlements.distinct_amount_count = 1 as has_repeated_equal_amount,
    coalesce(identity.has_shared_nin, false) as shared_identity_indicator,
    coalesce(identity.has_shared_account, false) as shared_account_indicator,
    (case when entitlements.instruction_count > 1 then 1 else 0 end
     + case when entitlements.instruction_count > 1
          and entitlements.maximum_instruction_amount < loan.amount_approved
          and entitlements.total_instructed_amount >= loan.amount_approved then 1 else 0 end
     + case when coalesce(identity.has_shared_nin, false) then 1 else 0 end
     + case when coalesce(identity.has_shared_account, false) then 1 else 0 end
    ) as alert_count,
    concat_ws('; ',
        case when entitlements.instruction_count > 1 then 'DUPLICATE_ENTITLEMENT' end,
        case when entitlements.instruction_count > 1
          and entitlements.maximum_instruction_amount < loan.amount_approved
          and entitlements.total_instructed_amount >= loan.amount_approved then 'FRAGMENTED_PAYMENT' end,
        case when coalesce(identity.has_shared_nin, false) then 'SHARED_IDENTITY' end,
        case when coalesce(identity.has_shared_account, false) then 'SHARED_ACCOUNT_TOKEN' end
    ) as alert_reason,
    case
        when entitlements.instruction_count > 1
         and (entitlements.instruction_channel_count > 1
           or entitlements.total_instructed_amount > loan.amount_approved) then 'HIGH'
        when entitlements.instruction_count > 1 then 'MEDIUM'
        else 'LOW'
    end as payment_pattern_risk_band,
    case
        when entitlements.instruction_count > 1
         and (entitlements.instruction_channel_count > 1
           or entitlements.total_instructed_amount > loan.amount_approved) then 'HIGH'
        when entitlements.instruction_count > 1
          or coalesce(identity.has_shared_nin, false)
          or coalesce(identity.has_shared_account, false) then 'MEDIUM'
        else 'LOW'
    end as alert_severity,
    (entitlements.instruction_count > 1
      or coalesce(identity.has_shared_nin, false)
      or coalesce(identity.has_shared_account, false)) as requires_review,
    'INDICATOR_NOT_FRAUD_DETERMINATION' as interpretation
from entitlements
left join {{ ref('gld_fct_pdm_loans') }} loan using (loan_id)
left join {{ ref('gld_dim_pdm_beneficiary') }} beneficiary
  on entitlements.beneficiary_sk = beneficiary.beneficiary_sk
left join {{ ref('gld_dim_pdm_sacco') }} sacco using (sacco_sk)
left join {{ ref('vlt_pdm_beneficiary_identity_signals') }} identity
  on beneficiary.beneficiary_token = identity.beneficiary_token
