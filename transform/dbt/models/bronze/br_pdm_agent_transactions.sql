{{ config(materialized='iceberg_table') }}

select * from {{ ref('stg_pdm_agent_transactions') }}
