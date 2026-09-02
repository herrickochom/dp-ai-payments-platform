{{ config(materialized='iceberg_table') }}

select * exclude (
    event_data,
    parsed_event_data
)
from {{ ref('stg_pdm_pdmis_households') }}
