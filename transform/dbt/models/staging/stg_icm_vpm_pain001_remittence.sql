{{ config(
    materialized='iceberg_table',
    schema='staging',
    tags=['staging', 'icmn_vpm', 'pain001', 'remittence']
) }}

with vpm_payments as (
    select
        event_id,
        message_id,
        payment_information_id,
        creation_at,
        extract(year from creation_at) as created_year,
        extract(month from creation_at) as created_month,
        extract(day from creation_at) as created_day,
        extract(hour from creation_at) as created_hour,
        {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.RmtInf.Ustrd') }} as remittance_information,
        invoice_reference_1,
        invoice_reference_2,
        invoice_reference_3,
        invoice_reference_4,
        invoice_reference_5,
        kafka_topic,
        kafka_partition,
        kafka_offset,
        kafka_timestamp
    from {{ ref('br_icmn_vpm_pain001') }}
)

select
    payment.event_id,
    payment.message_id,
    payment.payment_information_id,
    remittance.remittance_type,
    remittance.creditor_reference,
    remittance.invoice_reference,
    payment.creation_at,
    payment.created_year,
    payment.created_month,
    payment.created_day,
    payment.created_hour,
    payment.kafka_topic,
    payment.kafka_partition,
    payment.kafka_offset,
    payment.kafka_timestamp
from vpm_payments payment
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
