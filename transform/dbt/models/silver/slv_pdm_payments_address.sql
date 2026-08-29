{{ config(materialized='iceberg_table') }}

select event_id, message_id, 'vpm' as payment_source, 'debtor' as party_role,
       debtor_name as party_name,
       {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.Dbtr.PstlAdr.StrtNm') }} as street_name,
       {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.Dbtr.PstlAdr.TwnNm') }} as town_name,
       {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.Dbtr.PstlAdr.PstCd') }} as postal_code,
       {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.Dbtr.PstlAdr.Ctry') }} as country_code
from {{ ref('br_pdm_icmn_vpm_pain001') }}

union all

select event_id, message_id, 'vpm' as payment_source, 'creditor' as party_role,
       creditor_name as party_name,
       {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.Cdtr.PstlAdr.StrtNm') }} as street_name,
       {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.Cdtr.PstlAdr.TwnNm') }} as town_name,
       {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.Cdtr.PstlAdr.PstCd') }} as postal_code,
       {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.Cdtr.PstlAdr.Ctry') }} as country_code
from {{ ref('br_pdm_icmn_vpm_pain001') }}
