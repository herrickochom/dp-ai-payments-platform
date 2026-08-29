{{ config(materialized='iceberg_table') }}

select * from {{ ref('stg_pdm_mobile_airtel_pacs008') }}
