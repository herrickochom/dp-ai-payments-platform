{{ config(materialized='iceberg_table') }}

with staging as (

    select
        event_id,
        household_id,
        head_of_household,
        head_phone,
        member_count,
        adults_count,
        children_count,
        economic_status,
        food_security_status,
        housing_type,
        land_ownership,
        village,
        parish,
        sub_county,
        county,
        district,
        region,
        registration_date,
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
    from {{ ref('stg_pdm_pdmis_households') }}

)

select *
from staging
