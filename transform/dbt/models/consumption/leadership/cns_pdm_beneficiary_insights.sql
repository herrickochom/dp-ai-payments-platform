{{ config(materialized='iceberg_table', tags=['consumption', 'insights']) }}

with loans as (
    select
        beneficiary_sk,
        count(*) as loan_count,
        sum(amount_approved) as approved_amount,
        sum(amount_disbursed) as disbursed_amount,
        sum(amount_repaid) as repaid_amount,
        sum(outstanding_amount) as outstanding_amount
    from {{ ref('gld_fct_pdm_loans') }}
    group by 1
), cashouts as (
    select
        beneficiary_sk,
        count(*) as cashout_count,
        sum(cashout_amount) as cashout_amount,
        sum(case when days_from_loan_approval between 0 and 1 then 1 else 0 end) as rapid_cashout_count,
        sum(case when not is_reconciled then 1 else 0 end) as unreconciled_cashout_count
    from {{ ref('gld_fct_pdm_agent_cashouts') }}
    group by 1
)
select
    beneficiary.beneficiary_sk,
    beneficiary.geography_sk,
    beneficiary.special_group_sk,
    beneficiary.beneficiary_id,
    beneficiary.gender,
    beneficiary.nin_verified,
    beneficiary.phone_verified,
    coalesce(loans.loan_count, 0) as loan_count,
    coalesce(loans.approved_amount, 0) as approved_amount,
    coalesce(loans.disbursed_amount, 0) as disbursed_amount,
    coalesce(loans.repaid_amount, 0) as repaid_amount,
    coalesce(loans.outstanding_amount, 0) as outstanding_amount,
    coalesce(cashouts.cashout_count, 0) as cashout_count,
    coalesce(cashouts.cashout_amount, 0) as cashout_amount,
    coalesce(cashouts.rapid_cashout_count, 0) as rapid_cashout_count,
    coalesce(cashouts.unreconciled_cashout_count, 0) as unreconciled_cashout_count,
    coalesce(loans.loan_count, 0) > 1 as has_multiple_loans,
    case
        when coalesce(loans.loan_count, 0) > 1
          or coalesce(cashouts.unreconciled_cashout_count, 0) > 0
          or not coalesce(beneficiary.nin_verified, false) then 'REVIEW'
        else 'STANDARD'
    end as oversight_status
from {{ ref('gld_dim_pdm_beneficiary') }} beneficiary
left join loans using (beneficiary_sk)
left join cashouts using (beneficiary_sk)
where beneficiary.beneficiary_id is not null
