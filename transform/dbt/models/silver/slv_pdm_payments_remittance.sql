{{ config(materialized='iceberg_table') }}

with payments as (
    select * from {{ ref('br_pdm_icmn_vpm_pain001') }}
),
remittance_rows as (
    select
        payment.*,
        remittance.remittance_type,
        remittance.creditor_reference,
        remittance.invoice_reference
    from payments payment
    cross join lateral (
        values
            ('UNSTRUCTURED', payment.remittance_information, cast(null as varchar)),
            ('INVOICE', payment.invoice_reference_1, payment.invoice_reference_1),
            ('INVOICE', payment.invoice_reference_2, payment.invoice_reference_2),
            ('INVOICE', payment.invoice_reference_3, payment.invoice_reference_3),
            ('INVOICE', payment.invoice_reference_4, payment.invoice_reference_4),
            ('INVOICE', payment.invoice_reference_5, payment.invoice_reference_5)
    ) as remittance(remittance_type, creditor_reference, invoice_reference)
    where remittance.creditor_reference is not null
)
select distinct
    message_id,
    'VPM' as payment_source,
    payment_information_id,
    remittance_type,
    creditor_reference,
    invoice_reference
from remittance_rows
