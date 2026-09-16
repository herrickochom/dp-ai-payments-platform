# 05 - Bronze Lineage Audit

Exactly 25 Bronze tables were classified as Kafka-derived for the lineage
audit.

The separate AI Bronze prediction table is file-loaded ML output and is not
included in this Kafka-specific audit.

## Final audit result

All 25 Kafka-derived Bronze tables passed:

- row count greater than zero;
- zero NULL required lineage fields;
- unique `(topic, partition, offset)` count equals row count;
- unique event_id count equals row count.

Observed row counts:

- br_pdm_agent_locations: 38
- br_pdm_agent_profiles: 38
- br_pdm_agent_transactions: 151
- br_pdm_cpo_plm_pain002: 238
- br_pdm_cpo_psn_pain002: 238
- br_pdm_icmn_pmn_pain001: 238
- br_pdm_icmn_vpm_pain001: 238
- br_pdm_mobile_airtel_pacs002: 16
- br_pdm_mobile_airtel_pacs008: 16
- br_pdm_mobile_mtn_pacs002: 222
- br_pdm_mobile_mtn_pacs008: 222
- br_pdm_payments_plm_lifecycle_events: 238
- br_pdm_payments_pmn_lifecycle_events: 238
- br_pdm_pdmis_beneficiaries: 250
- br_pdm_pdmis_business_plans: 250
- br_pdm_pdmis_households: 250
- br_pdm_pdmis_loans: 250
- br_pdm_pdmis_saccos: 38
- br_pdm_pdmis_special_groups: 5
- br_pdm_wendi_camt052: 38
- br_pdm_wendi_camt053: 151
- br_pdm_wendi_camt054: 151
- br_pdm_wendi_pain001: 238
- br_pdm_wendi_pain002: 238
- br_pdm_wendi_transactions: 238

Two earlier audit harness attempts failed because of interactive
authentication constraints. Those were harness/authentication failures,
not Bronze data failures.

A Trino stage-count warning above a soft threshold was an audit-performance
observation rather than a data-quality failure.
