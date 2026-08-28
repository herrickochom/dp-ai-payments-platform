{{ config(materialized='iceberg_table') }}

select *
from {{ ref('br_icmn_vpm_pain001') }}
