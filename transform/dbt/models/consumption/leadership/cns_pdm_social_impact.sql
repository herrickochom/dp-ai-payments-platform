{{ config(materialized='iceberg_table', tags=['consumption', 'social-impact']) }}

select
    special_group.special_group_sk,
    special_group.group_code,
    special_group.group_name,
    special_group.quota_percentage,
    count(distinct beneficiary.beneficiary_sk) as registered_beneficiary_count,
    count(distinct beneficiary.household_id) as household_count,
    count(distinct loan.beneficiary_sk) as funded_beneficiary_count,
    count(distinct loan.loan_id) as funded_loan_count,
    count(distinct loan.business_plan_id) as funded_business_plan_count,
    sum(loan.amount_approved) as approved_amount,
    sum(loan.amount_disbursed) as disbursed_amount,
    sum(loan.amount_repaid) as repaid_amount,
    sum(loan.outstanding_amount) as outstanding_amount,
    count(distinct loan.beneficiary_sk) / nullif(count(distinct beneficiary.beneficiary_sk), 0) as beneficiary_funding_rate,
    sum(loan.amount_disbursed) / nullif(sum(loan.amount_approved), 0) as intended_amount_delivery_rate,
    count(distinct loan.beneficiary_sk) filter (where loan.amount_repaid > 0)
        / nullif(count(distinct loan.beneficiary_sk), 0) as beneficiaries_entering_repayment_rate
from {{ ref('gld_dim_pdm_special_group') }} special_group
left join {{ ref('gld_dim_pdm_beneficiary') }} beneficiary using (special_group_sk)
left join {{ ref('gld_fct_pdm_loans') }} loan using (beneficiary_sk)
where special_group.group_code is not null
group by 1, 2, 3, 4
