{{ config(materialized='iceberg_table') }}

select * from {{ ref('stg_pdm_pdmis_beneficiaries') }}
