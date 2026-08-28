{{ config(materialized='iceberg_table') }}

select *
from {{ ref('stg_cpo_plm_pain002') }}
