{{ config(
    materialized='iceberg_table',
    tags=['bronze', 'icmn', 'pmn', 'pain001']
) }}

-- Bronze persists atomic parsed columns + Kafka lineage only.
-- parsed_event_data is excluded so the raw JSON blob is never written to
-- Iceberg. (event_data / payload are no longer surfaced by staging at all,
-- so they cannot be carried here.)
select * exclude (
    parsed_event_data
)

from {{ ref('stg_pdm_icmn_pmn_pain001') }}