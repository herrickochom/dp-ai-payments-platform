{{ config(materialized='iceberg_table') }}

with staging as (

    select
        event_id,
        agent_id,
        agent_code,
        agent_name,
        phone,
        registration_number,
        registration_date,
        network_provider,
        commission_rate,
        location,
        country,
        village,
        parish,
        sub_county,
        county,
        district,
        region,
        verified,
        is_active,
        created_at,
        updated_at,
        kafka_topic,
        kafka_partition,
        kafka_offset,
        kafka_timestamp,
        year,
        month,
        day,
        load_timestamp,
        record_source
    from {{ ref('stg_pdm_agent_profiles') }}

)

select *
from staging
