{{ config(materialized='iceberg_table') }}

select *
from {{ ref('stg_pdm_cpo_plm_pain002') }}
