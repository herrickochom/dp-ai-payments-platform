{{ config(materialized='iceberg_table', tags=['consumption', 'performance']) }}

select
    {{ gold_surrogate_key(['office.region', 'office.district', 'office.parish']) }} as parish_sk,
    office.region, office.district, office.parish,
    'SACCO_OFFICE_ATTRIBUTION' as geography_attribution,
    count(distinct loan.loan_id) as loan_count,
    count(distinct loan.beneficiary_sk) as beneficiary_count,
    sum(loan.amount_approved) as approved_amount,
    sum(loan.amount_disbursed) as disbursed_amount,
    sum(loan.amount_repaid) as repaid_amount,
    sum(loan.principal_repaid) as principal_repaid_amount,
    sum(loan.outstanding_amount) as outstanding_amount,
    sum(loan.amount_disbursed) / nullif(sum(loan.amount_approved), 0) as disbursement_rate,
    -- Principal-based repayment: principal repaid against principal disbursed
    -- (PDM POC v1 decisions 1-2). amount_repaid is the total contractual repaid
    -- and includes interest, so it must never be used as the numerator here.
    sum(loan.principal_repaid) / nullif(sum(loan.amount_disbursed), 0) as principal_repayment_rate
from {{ ref('gld_fct_pdm_loans') }} loan
left join {{ ref('gld_dim_pdm_sacco') }} sacco using (sacco_sk)
left join {{ ref('slv_pdm_saccos') }} office
  on sacco.sacco_id = office.sacco_id
where office.parish is not null
group by 1, 2, 3, 4, 5
