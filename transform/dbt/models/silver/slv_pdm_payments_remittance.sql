{{ config(materialized='iceberg_table') }}

with payments as (
    select *
    from {{ source('bronze', 'br_pdm_icmn_vpm_pain001') }}
),

remittance_rows as (

    select
        message_id,
        payment_information_id,
        'UNSTRUCTURED' as remittance_type,
        remittance_information as creditor_reference,
        cast(null as varchar) as invoice_reference
    from payments
    where remittance_information is not null

    union all

    select
        message_id,
        payment_information_id,
        'INVOICE',
        invoice_reference_1,
        invoice_reference_1
    from payments
    where invoice_reference_1 is not null

    union all

    select
        message_id,
        payment_information_id,
        'INVOICE',
        invoice_reference_2,
        invoice_reference_2
    from payments
    where invoice_reference_2 is not null

    union all

    select
        message_id,
        payment_information_id,
        'INVOICE',
        invoice_reference_3,
        invoice_reference_3
    from payments
    where invoice_reference_3 is not null

    union all

    select
        message_id,
        payment_information_id,
        'INVOICE',
        invoice_reference_4,
        invoice_reference_4
    from payments
    where invoice_reference_4 is not null

    union all

    select
        message_id,
        payment_information_id,
        'INVOICE',
        invoice_reference_5,
        invoice_reference_5
    from payments
    where invoice_reference_5 is not null
)

select distinct
    message_id,
    'VPM' as payment_source,
    payment_information_id,
    remittance_type,
    creditor_reference,
    invoice_reference
from remittance_rows
