{{ config(materialized='iceberg_table') }}

with staging as (

    select
        event_id,
        agent_id,
        location_type,
        address,
        latitude,
        longitude,
        is_active,
        kafka_topic,
        kafka_partition,
        kafka_offset,
        kafka_timestamp,
        year,
        month,
        day,
        load_timestamp,
        record_source
    from {{ ref('stg_pdm_agent_locations') }}

)

select *
from staging
