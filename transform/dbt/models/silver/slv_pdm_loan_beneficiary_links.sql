{{ config(materialized='iceberg_table', tags=['silver', 'privacy-boundary']) }}

-- Pseudonymous loan -> beneficiary_token linkage (Tier 3 output only).
-- Converts the Silver loan fact's restricted Tier-2 internal linkage key into
-- the canonical analytical token so Gold/Consumption never need the clear
-- identifier. One row per loan (slv_pdm_loans grain is one row per loan).

select
    loan.loan_id,
    identity.beneficiary_token,
    identity.token_version
from {{ ref('slv_pdm_loans') }} loan
inner join {{ ref('vlt_pdm_beneficiary_identity') }} identity
    on loan.beneficiary_id = identity.beneficiary_id
