{{ config(materialized='iceberg_table') }}

select *
from {{ ref('br_pdm_cpo_plm_pain002') }}
