{{ config(materialized='iceberg_table') }}

select
    business_plan_id, loan_id, beneficiary_id, project_name, project_type,
    description, location, land_size, market, total_investment, expected_revenue,
    expected_costs, expected_profit, submission_date, approval_status,
    approval_date, approved_by, created_at, updated_at
from {{ ref('br_pdm_pdmis_business_plans') }}
qualify row_number() over (
    partition by business_plan_id order by updated_at desc nulls last, kafka_timestamp desc, kafka_offset desc
) = 1
