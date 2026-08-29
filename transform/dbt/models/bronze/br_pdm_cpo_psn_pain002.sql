{{ config(materialized='iceberg_table') }}

select *
from {{ ref('stg_pdm_cpo_psn_pain002') }}
