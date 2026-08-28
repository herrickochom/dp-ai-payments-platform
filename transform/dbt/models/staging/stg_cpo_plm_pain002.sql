{{ config(materialized='iceberg_table') }}

select *
from {{ ref('br_cpo_plm_pain002') }}
