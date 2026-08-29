{{ config(materialized='iceberg_table') }}

select *
from {{ ref('stg_pdm_icmn_pmn_pain001') }}
