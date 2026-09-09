{{ config(materialized='iceberg_table') }}

with staging as (

    select
        event_id,
        business_plan_id,
        loan_id,
        beneficiary_id,
        project_name,
        project_type,
        description,
        location,
        land_size,
        market,
        total_investment,
        expected_revenue,
        expected_costs,
        expected_profit,
        submission_date,
        approval_status,
        approval_date,
        approved_by,
        created_at,
        updated_at,
        kafka_topic,
        kafka_partition,
        kafka_offset,
        kafka_timestamp,
        category,
        source_system,
        source_group,
        year,
        month,
        day,
        load_timestamp,
        record_source
    from {{ ref('stg_pdm_pdmis_business_plans') }}

)

select *
from staging
