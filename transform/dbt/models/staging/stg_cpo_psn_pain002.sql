{{ config(materialized='iceberg_table') }}

select *
from {{ ref('br_cpo_psn_pain002') }}
