{{ config(materialized='iceberg_table') }}

select *
from {{ ref('br_pdm_icmn_pmn_pain001') }}
