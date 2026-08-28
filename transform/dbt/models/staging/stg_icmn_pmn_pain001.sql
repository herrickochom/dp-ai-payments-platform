{{ config(materialized='iceberg_table') }}

select *
from {{ ref('br_icmn_pmn_pain001') }}
