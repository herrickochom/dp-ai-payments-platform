{{ config(materialized='iceberg_table') }}

with staging as (

    select
        event_id,
        group_code,
        group_name,
        description,
        quota_percentage,
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
    from {{ ref('stg_pdm_pdmis_special_groups') }}

)

select *
from staging
