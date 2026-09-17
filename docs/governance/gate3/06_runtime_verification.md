# Gate 3.2 runtime verification

Date: 2026-09-16. Scope: read-only inspection of the currently connected local Compose deployment. No remediation or commit. This evidence establishes observations, not Gate 3 acceptance.

## Commands and safety

Conceptual commands: Git HEAD/status; filtered Compose service status; Kafka topic describe and config describe with explicit lifecycle-output allowlists; S3 GetBucketPolicy/GetBucketLifecycleConfiguration/GetBucketVersioning/GetObjectLockConfiguration; aggregate ListObjectsV2 for protected prefixes; Nessie GET reference/entries/content APIs; S3 GET of Iceberg metadata JSON for snapshot metadata only; source/AST inspection and synthetic serialization; filtered container mount/path inspection; model SHA256 and allowlisted metadata; in-memory read of installed sklearn version from the stopped feature container.

Docker access initially failed under the sandbox. Escalated read-only inspection was approved. Automatic review rejected an unfiltered broker config command because it could expose security/credential settings; a safer allowlisted-output inspection succeeded. No raw environment/config dumps, secrets, password hashes, event consumption, domain payload reads or audit/prediction rows were printed. No temporary scripts/test files were added. The synthetic check is not an end-to-end runtime unit test.

## 1. Git and services

VERIFIED: HEAD is `3284e2649e9a51cf25c4f6b89a0c2207f5333833`. Initial status: only untracked `docs/governance/`, containing Gate 3.1 documents. Implementation parent remains the frozen Gate 2 implementation reference.

| State | Services |
|---|---|
| RUNNING and HEALTHY | kafka, minio, nessie, payment-consumer-events, postgres, redis, schema-registry, trino |
| STOPPED, exit 0 | kafka-init, kafka-volume-init, minio-init-buckets, minio-init-databases, payment-producer, payment-xml-generator, pdm-ml-features, postgres-healthcheck, schema-registry-init |
| STOPPED, exit 1 | duckdb |
| UNHEALTHY | None among inventoried containers |
| RUNNING without a health result | None among inventoried containers |

Configured services without current project containers (including agent-api, scoring/training and profile-dependent BI services) are not proven STOPPED and are not NOT CONFIGURED: runtime deployment is absent/UNVERIFIED. A service lacking a health check would be NOT CONFIGURED for that check, not necessarily unhealthy. Successful stopped one-shot jobs do not establish ongoing readiness. DuckDB exit 1 is an operational finding; logs were not dumped and cause is UNVERIFIED. No restart was performed.

## 2. Kafka effective lifecycle settings

VERIFIED / MATCH: all 26 project-managed topics match `platform/kafka/topics.yaml` for partition count, replication factor, cleanup policy and retention.ms. All 28 inventoried topics, including internal topics, have effective retention.bytes=-1, segment.ms=604800000 and min.compaction.lag.ms=0. For delete-only topics compaction lag is not operationally relevant. These extra effective settings are not declared by the manifest and are recorded rather than treated as a manifest mismatch.

| Topic | Partitions | RF | cleanup.policy | retention.ms | Manifest comparison |
|---|---:|---:|---|---:|---|
| icmn.vpm.pain001 | 6 | 1 | delete | 604800000 | MATCH |
| icmn.pmn.pain001 | 6 | 1 | delete | 604800000 | MATCH |
| cpo.psn.pain002 | 6 | 1 | delete | 604800000 | MATCH |
| cpo.plm.pain002 | 6 | 1 | delete | 604800000 | MATCH |
| wendi.camt053 | 4 | 1 | delete | 2592000000 | MATCH |
| wendi.camt052 | 4 | 1 | delete | 86400000 | MATCH |
| wendi.camt054 | 8 | 1 | delete | 604800000 | MATCH |
| wendi.transactions | 8 | 1 | delete | 604800000 | MATCH |
| wendi.pain001 | 6 | 1 | delete | 604800000 | MATCH |
| wendi.pain002 | 6 | 1 | delete | 604800000 | MATCH |
| mobile.mtn.pacs008 | 6 | 1 | delete | 604800000 | MATCH |
| mobile.mtn.pacs002 | 4 | 1 | delete | 604800000 | MATCH |
| mobile.airtel.pacs008 | 6 | 1 | delete | 604800000 | MATCH |
| mobile.airtel.pacs002 | 4 | 1 | delete | 604800000 | MATCH |
| agent.transactions | 8 | 1 | delete | 2592000000 | MATCH |
| agent.profiles | 4 | 1 | delete | 604800000 | MATCH |
| agent.locations | 4 | 1 | delete | 604800000 | MATCH |
| pdmis.beneficiaries | 8 | 1 | delete | 604800000 | MATCH |
| pdmis.loans | 8 | 1 | delete | 604800000 | MATCH |
| pdmis.repayments | 8 | 1 | delete | 604800000 | MATCH |
| pdmis.saccos | 4 | 1 | delete | 604800000 | MATCH |
| pdmis.households | 4 | 1 | delete | 604800000 | MATCH |
| pdmis.business_plans | 4 | 1 | delete | 604800000 | MATCH |
| pdmis.special_groups | 4 | 1 | delete | 604800000 | MATCH |
| payment-events.retry | 8 | 1 | delete | 604800000 | MATCH |
| payment-events.dlq | 8 | 1 | delete | 2592000000 | MATCH |
| _schemas | 1 | 1 | compact | 604800000 | Outside project manifest |
| __consumer_offsets | 50 | 1 | compact | 604800000 | Outside project manifest |

VERIFIED: broker offsets.retention.minutes=10080 (7 days), offsets.retention.check.interval.ms=600000, log.retention.check.interval.ms=300000, log.message.timestamp.type=CreateTime. Offset expiry depends on group state; the value is not a blanket seven-day deletion of every active offset. Compact-only internal topics do not acquire a seven-day ordinary time-delete window merely because retention.ms reports that inherited value.

POC LIMITATION: RF=1. PRODUCTION FOLLOW-UP: resilient broker replication and monitored recovery windows.

Replay window can be lost: delete-topic retention continues while a consumer is stalled. A stall exceeding the relevant 1/7/30-day window can lose unarchived events. Segment rolling/cleanup timing means configured milliseconds are not an exact deletion deadline or guaranteed operational SLA. Durable Raw may support separately authorised reprocessing of already archived events, but does not prove recovery of events never archived. No domain records were consumed, offsets described/reset/committed or messages replayed; current lag and retained offset ranges remain UNVERIFIED.

## 3. MinIO security, lifecycle and backup presence

Inspected endpoint: active consumer's configured S3 endpoint, `http://minio:9000`, using existing credentials internally.

| Bucket | Anonymous access | Lifecycle | Versioning | Object lock/default retention | Finding |
|---|---|---|---|---|---|
| dp-ai-payment | CONFIRMED PRIVATE at bucket-policy layer: GetBucketPolicy returned NoSuchBucketPolicy | NoSuchLifecycleConfiguration: absent | Empty response: not enabled, no suspended status reported | ObjectLockConfigurationNotFoundError: not configured | VERIFIED |
| dp-ai-payment-gate2-backup | UNVERIFIED: NoSuchBucket | NoSuchBucket | NoSuchBucket | Lock query returned ObjectLockConfigurationNotFoundError; does not prove protection on an absent bucket | DRIFT / DEFECT: expected recovery bucket absent |

The active bucket is not configured for anonymous download through a bucket policy. The initializer command `mc policy set download local/dp-ai-payment` therefore does not demonstrate current public exposure. No anonymous object-download experiment was necessary/performed, no payload was read for security testing, and no external proxy/network exposure is claimed verified.

GetBucketPolicy for the expected backup bucket and both protected-prefix aggregate listings returned NoSuchBucket. Prefix counts/sizes cannot be recorded: missing bucket is not a verified empty-prefix result. Neither protected prefix is available at this endpoint. This contradicts frozen historical recovery availability evidence; it does not establish when or why the bucket became absent, or whether an independent copy exists elsewhere. Original recorded backup integrity remains historical and is not rewritten.

New acceptance blocker: recovery bucket/prefix availability cannot be confirmed. Do not reconstruct, delete, relocate or repair backups in this phase. There are no active bucket lifecycle rules proving either YAML retention declaration enforced. Storage versioning/object lock is not activated by documentation.

## 4. Nessie references

VERIFIED: complete reference response has hasMore=false and only one reference:

| Type | Name | Hash |
|---|---|---|
| BRANCH | main | ddd3b9b6bfd9db5b00ac6304d74124b249176ec0ddab1464adba8b8e69ee96ba |

DRIFT / DEFECT: `pre-gate2-20260915T155541Z` is absent from enumeration; direct GET returned HTTP 404. Its expected hash remains `f85893b23c441db5abd0d2400da134cbb5fc4d0cb52a1e3c24e3ae02d85b35df`; no current tag hash can be recorded. Other branches/tags: none returned. Availability of the historical recovery commit itself was not tested.

Current main MATCHES frozen baseline, so the conditional stop for main-hash mismatch was not triggered. Missing recovery tag still blocks recovery acceptance. No reference was created, moved or deleted and no rollback was attempted.

## 5. Iceberg snapshot inventory

VERIFIED: Nessie entries inventory has 111 entries, hasMore=false: 105 Iceberg tables and six namespaces. All 105 current table metadata files were readable; zero inspection errors. Observed 104 total snapshots: 104 tables have one snapshot; `consumption.cns_pdm_duplicate_fragmentation_alerts` has zero, current-snapshot-id=-1, and no oldest/newest timestamp. This metadata observation is not a defect or row-count claim.

| Namespace | Persistent tables |
|---|---:|
| bronze | 26 |
| silver | 26 |
| silver_vault | 4 |
| gold | 10 |
| consumption | 37 |
| staging | 2 |

The persistent `staging.uganda_district_geojson` and `staging.uganda_superset_district_iso` seed tables are an exception to the ephemeral parsing-view description; they must have their own lifecycle policy.

Inventory scope is current main metadata, not proof of complete historic file reachability or safe cleanup. Retained Nessie references, protected hashes and protected backups must be considered before future physical cleanup, even though the expected tag/bucket are currently missing. No file was classified orphaned. No maintenance eligibility or disposal conclusion is made.

| Table | Current snapshot identifier | Snapshot count | Oldest timestamp UTC | Newest timestamp UTC |
|---|---|---:|---|---|
| bronze.br_pdm_agent_locations | 5527434451725533714 | 1 | 2026-09-16T18:52:18.134000+00:00 | 2026-09-16T18:52:18.134000+00:00 |
| bronze.br_pdm_agent_profiles | 6607379265484573326 | 1 | 2026-09-16T18:52:23.729000+00:00 | 2026-09-16T18:52:23.729000+00:00 |
| bronze.br_pdm_agent_transactions | 9142814795715461318 | 1 | 2026-09-16T18:52:27.046000+00:00 | 2026-09-16T18:52:27.046000+00:00 |
| bronze.br_pdm_ai_default_risk | 4113621362992760987 | 1 | 2026-09-16T18:39:45.389000+00:00 | 2026-09-16T18:39:45.389000+00:00 |
| bronze.br_pdm_cpo_plm_pain002 | 1573943219732694572 | 1 | 2026-09-16T18:52:31.313000+00:00 | 2026-09-16T18:52:31.313000+00:00 |
| bronze.br_pdm_cpo_psn_pain002 | 8673228349419504393 | 1 | 2026-09-16T18:52:45.847000+00:00 | 2026-09-16T18:52:45.847000+00:00 |
| bronze.br_pdm_icmn_pmn_pain001 | 5690545691228323845 | 1 | 2026-09-16T18:52:49.631000+00:00 | 2026-09-16T18:52:49.631000+00:00 |
| bronze.br_pdm_icmn_vpm_pain001 | 2819557113566546322 | 1 | 2026-09-16T18:52:59.532000+00:00 | 2026-09-16T18:52:59.532000+00:00 |
| bronze.br_pdm_mobile_airtel_pacs002 | 5020461302754134342 | 1 | 2026-09-16T18:53:02.610000+00:00 | 2026-09-16T18:53:02.610000+00:00 |
| bronze.br_pdm_mobile_airtel_pacs008 | 1804999769560272702 | 1 | 2026-09-16T18:53:08.114000+00:00 | 2026-09-16T18:53:08.114000+00:00 |
| bronze.br_pdm_mobile_mtn_pacs002 | 2881593446889703534 | 1 | 2026-09-16T18:53:11.806000+00:00 | 2026-09-16T18:53:11.806000+00:00 |
| bronze.br_pdm_mobile_mtn_pacs008 | 2224434626960180405 | 1 | 2026-09-16T18:53:15.764000+00:00 | 2026-09-16T18:53:15.764000+00:00 |
| bronze.br_pdm_payments_plm_lifecycle_events | 3213423792978019835 | 1 | 2026-09-16T18:52:42.120000+00:00 | 2026-09-16T18:52:42.120000+00:00 |
| bronze.br_pdm_payments_pmn_lifecycle_events | 7891042205344416780 | 1 | 2026-09-16T18:52:55.910000+00:00 | 2026-09-16T18:52:55.910000+00:00 |
| bronze.br_pdm_pdmis_beneficiaries | 5801441511394525466 | 1 | 2026-09-16T18:53:22.280000+00:00 | 2026-09-16T18:53:22.280000+00:00 |
| bronze.br_pdm_pdmis_business_plans | 468526067172789633 | 1 | 2026-09-16T18:53:25.656000+00:00 | 2026-09-16T18:53:25.656000+00:00 |
| bronze.br_pdm_pdmis_households | 8482133399124658597 | 1 | 2026-09-16T18:53:29.302000+00:00 | 2026-09-16T18:53:29.302000+00:00 |
| bronze.br_pdm_pdmis_loans | 8764429033921332628 | 1 | 2026-09-16T18:53:36.133000+00:00 | 2026-09-16T18:53:36.133000+00:00 |
| bronze.br_pdm_pdmis_saccos | 8472777764151510131 | 1 | 2026-09-16T18:53:39.148000+00:00 | 2026-09-16T18:53:39.148000+00:00 |
| bronze.br_pdm_pdmis_special_groups | 6056373424360342957 | 1 | 2026-09-16T18:53:42.255000+00:00 | 2026-09-16T18:53:42.255000+00:00 |
| bronze.br_pdm_wendi_camt052 | 363582791555498089 | 1 | 2026-09-16T18:53:46.853000+00:00 | 2026-09-16T18:53:46.853000+00:00 |
| bronze.br_pdm_wendi_camt053 | 6321934324551271348 | 1 | 2026-09-16T18:53:51.018000+00:00 | 2026-09-16T18:53:51.018000+00:00 |
| bronze.br_pdm_wendi_camt054 | 516557050792265871 | 1 | 2026-09-16T18:53:54.320000+00:00 | 2026-09-16T18:53:54.320000+00:00 |
| bronze.br_pdm_wendi_pain001 | 6918477889205653483 | 1 | 2026-09-16T18:53:58.257000+00:00 | 2026-09-16T18:53:58.257000+00:00 |
| bronze.br_pdm_wendi_pain002 | 6430377913572020168 | 1 | 2026-09-16T18:54:04.800000+00:00 | 2026-09-16T18:54:04.800000+00:00 |
| bronze.br_pdm_wendi_transactions | 3686822485002833779 | 1 | 2026-09-16T18:54:08.387000+00:00 | 2026-09-16T18:54:08.387000+00:00 |
| consumption.cns_pdm_agent_risk_indicators | 5946658157571099189 | 1 | 2026-09-16T15:42:46.252000+00:00 | 2026-09-16T15:42:46.252000+00:00 |
| consumption.cns_pdm_ai_default_risk | 2661798445586597986 | 1 | 2026-09-16T18:54:45.341000+00:00 | 2026-09-16T18:54:45.341000+00:00 |
| consumption.cns_pdm_beneficiary_identity_alerts | 2591853703417448884 | 1 | 2026-09-16T18:55:01.868000+00:00 | 2026-09-16T18:55:01.868000+00:00 |
| consumption.cns_pdm_beneficiary_insights | 8041529851139869403 | 1 | 2026-09-16T15:42:46.986000+00:00 | 2026-09-16T15:42:46.986000+00:00 |
| consumption.cns_pdm_channel_agent_performance | 9070107807664260085 | 1 | 2026-09-16T15:42:47.238000+00:00 | 2026-09-16T15:42:47.238000+00:00 |
| consumption.cns_pdm_daily_operational_metrics | 2269667934807936382 | 1 | 2026-09-16T15:42:54.419000+00:00 | 2026-09-16T15:42:54.419000+00:00 |
| consumption.cns_pdm_district_ai_risk | 2487106934693382756 | 1 | 2026-09-16T18:54:49.619000+00:00 | 2026-09-16T18:54:49.619000+00:00 |
| consumption.cns_pdm_district_geographic_risk | 3575275267347203025 | 1 | 2026-09-16T15:42:56.852000+00:00 | 2026-09-16T15:42:56.852000+00:00 |
| consumption.cns_pdm_district_geojson_risk | 507422385506960156 | 1 | 2026-09-16T15:42:59.257000+00:00 | 2026-09-16T15:42:59.257000+00:00 |
| consumption.cns_pdm_duplicate_fragmentation_alerts | -1 | 0 | None | None |
| consumption.cns_pdm_end_to_end_traceability | 1579036126355147434 | 1 | 2026-09-16T15:43:01.163000+00:00 | 2026-09-16T15:43:01.163000+00:00 |
| consumption.cns_pdm_executive_ai_risk | 7441962913775305462 | 1 | 2026-09-16T18:54:49.899000+00:00 | 2026-09-16T18:54:49.899000+00:00 |
| consumption.cns_pdm_executive_geographic_drilldown | 6756555176259371735 | 1 | 2026-09-16T15:42:47.785000+00:00 | 2026-09-16T15:42:47.785000+00:00 |
| consumption.cns_pdm_executive_intervention_priorities | 5735942153946811972 | 1 | 2026-09-16T15:42:59.475000+00:00 | 2026-09-16T15:42:59.475000+00:00 |
| consumption.cns_pdm_executive_monthly_trend | 2637532138247242137 | 1 | 2026-09-16T15:42:48.131000+00:00 | 2026-09-16T15:42:48.131000+00:00 |
| consumption.cns_pdm_executive_overview | 3014662491332627844 | 1 | 2026-09-16T15:42:57.184000+00:00 | 2026-09-16T15:42:57.184000+00:00 |
| consumption.cns_pdm_financial_fund_flow | 3717638354216504162 | 1 | 2026-09-16T18:54:53.365000+00:00 | 2026-09-16T18:54:53.365000+00:00 |
| consumption.cns_pdm_fraud_risk_insights | 5504381035397857830 | 1 | 2026-09-16T15:42:59.736000+00:00 | 2026-09-16T15:42:59.736000+00:00 |
| consumption.cns_pdm_fund_flow_funnel | 2970971533709362240 | 1 | 2026-09-16T18:54:57.376000+00:00 | 2026-09-16T18:54:57.376000+00:00 |
| consumption.cns_pdm_geographic_alerts | 4481753384314095748 | 1 | 2026-09-16T18:54:58.131000+00:00 | 2026-09-16T18:54:58.131000+00:00 |
| consumption.cns_pdm_geographic_coverage | 5234930295478701300 | 1 | 2026-09-16T18:54:58.448000+00:00 | 2026-09-16T18:54:58.448000+00:00 |
| consumption.cns_pdm_geographic_risk_drivers | 5527636926254235933 | 1 | 2026-09-16T18:54:58.817000+00:00 | 2026-09-16T18:54:58.817000+00:00 |
| consumption.cns_pdm_geographic_risk_summary | 6063801347065856307 | 1 | 2026-09-16T15:43:01.409000+00:00 | 2026-09-16T15:43:01.409000+00:00 |
| consumption.cns_pdm_geographic_risk_trend | 4920234843587952979 | 1 | 2026-09-16T15:42:59.930000+00:00 | 2026-09-16T15:42:59.930000+00:00 |
| consumption.cns_pdm_lifecycle_exception_cases | 4741319834129679582 | 1 | 2026-09-16T18:54:57.816000+00:00 | 2026-09-16T18:54:57.816000+00:00 |
| consumption.cns_pdm_lifecycle_exceptions | 8084868662354723820 | 1 | 2026-09-16T18:54:53.767000+00:00 | 2026-09-16T18:54:53.767000+00:00 |
| consumption.cns_pdm_loan_intervention_dashboard | 7734324950018768927 | 1 | 2026-09-16T15:42:52.414000+00:00 | 2026-09-16T15:42:52.414000+00:00 |
| consumption.cns_pdm_local_government_performance | 6128813058006663339 | 1 | 2026-09-16T18:54:59.110000+00:00 | 2026-09-16T18:54:59.110000+00:00 |
| consumption.cns_pdm_parish_geographic_risk | 7136983565174001589 | 1 | 2026-09-16T18:54:54.237000+00:00 | 2026-09-16T18:54:54.237000+00:00 |
| consumption.cns_pdm_parish_performance | 3703862401694582103 | 1 | 2026-09-16T18:54:51.632000+00:00 | 2026-09-16T18:54:51.632000+00:00 |
| consumption.cns_pdm_payment_operations | 2633591514647708302 | 1 | 2026-09-16T15:42:49.098000+00:00 | 2026-09-16T15:42:49.098000+00:00 |
| consumption.cns_pdm_payment_reconciliation | 3799939440841475667 | 1 | 2026-09-16T15:42:52.631000+00:00 | 2026-09-16T15:42:52.631000+00:00 |
| consumption.cns_pdm_payments_daily_summary | 4929156338353392729 | 1 | 2026-09-16T15:42:52.842000+00:00 | 2026-09-16T15:42:52.842000+00:00 |
| consumption.cns_pdm_sacco_portfolio | 5485558406659458043 | 1 | 2026-09-16T15:42:53.139000+00:00 | 2026-09-16T15:42:53.139000+00:00 |
| consumption.cns_pdm_social_impact | 1940594935355821344 | 1 | 2026-09-16T15:42:49.339000+00:00 | 2026-09-16T15:42:49.339000+00:00 |
| consumption.cns_pdm_subcounty_geographic_risk | 8021990124762648371 | 1 | 2026-09-16T15:43:00.202000+00:00 | 2026-09-16T15:43:00.202000+00:00 |
| consumption.cns_pdm_village_geographic_risk | 5229766540195021181 | 1 | 2026-09-16T18:54:43.761000+00:00 | 2026-09-16T18:54:43.761000+00:00 |
| gold.gld_dim_pdm_agent | 2519275711550868074 | 1 | 2026-09-16T18:54:38.241000+00:00 | 2026-09-16T18:54:38.241000+00:00 |
| gold.gld_dim_pdm_beneficiary | 7276716310755232022 | 1 | 2026-09-16T18:54:44.225000+00:00 | 2026-09-16T18:54:44.225000+00:00 |
| gold.gld_dim_pdm_date | 690352082716737807 | 1 | 2026-09-16T18:54:44.646000+00:00 | 2026-09-16T18:54:44.646000+00:00 |
| gold.gld_dim_pdm_geography | 5102051482986039784 | 1 | 2026-09-16T18:54:44.990000+00:00 | 2026-09-16T18:54:44.990000+00:00 |
| gold.gld_dim_pdm_sacco | 6805552324228694744 | 1 | 2026-09-16T18:54:39.123000+00:00 | 2026-09-16T18:54:39.123000+00:00 |
| gold.gld_dim_pdm_special_group | 3927122973893610908 | 1 | 2026-09-16T18:54:39.456000+00:00 | 2026-09-16T18:54:39.456000+00:00 |
| gold.gld_fct_pdm_agent_cashouts | 7305638748464300829 | 1 | 2026-09-16T15:22:01.083000+00:00 | 2026-09-16T15:22:01.083000+00:00 |
| gold.gld_fct_pdm_loans | 49396013777375181 | 1 | 2026-09-16T18:54:45.736000+00:00 | 2026-09-16T18:54:45.736000+00:00 |
| gold.gld_fct_pdm_payment_lifecycle | 2991555491945136919 | 1 | 2026-09-16T18:54:47.575000+00:00 | 2026-09-16T18:54:47.575000+00:00 |
| gold.gld_fct_pdm_payments | 4554131763359662743 | 1 | 2026-09-16T19:10:27.442000+00:00 | 2026-09-16T19:10:27.442000+00:00 |
| silver.slv_pdm_agent_locations | 6294334225026011038 | 1 | 2026-09-16T18:54:26.643000+00:00 | 2026-09-16T18:54:26.643000+00:00 |
| silver.slv_pdm_agents | 2558593027855567526 | 1 | 2026-09-16T18:54:27.114000+00:00 | 2026-09-16T18:54:27.114000+00:00 |
| silver.slv_pdm_batch | 3115385992001244824 | 1 | 2026-09-16T18:54:29.161000+00:00 | 2026-09-16T18:54:29.161000+00:00 |
| silver.slv_pdm_beneficiaries | 6834053133371394330 | 1 | 2026-09-16T18:54:38.535000+00:00 | 2026-09-16T18:54:38.535000+00:00 |
| silver.slv_pdm_business_plans | 5098047388082457934 | 1 | 2026-09-16T18:54:32.376000+00:00 | 2026-09-16T18:54:32.376000+00:00 |
| silver.slv_pdm_dq_results | 647160258325464203 | 1 | 2026-09-16T18:54:53.041000+00:00 | 2026-09-16T18:54:53.041000+00:00 |
| silver.slv_pdm_households | 2119990857453356812 | 1 | 2026-09-16T18:54:32.693000+00:00 | 2026-09-16T18:54:32.693000+00:00 |
| silver.slv_pdm_loan_beneficiary_links | 1213030762110668789 | 1 | 2026-09-16T18:54:38.817000+00:00 | 2026-09-16T18:54:38.817000+00:00 |
| silver.slv_pdm_loans | 6321488753169088750 | 1 | 2026-09-16T18:54:33.051000+00:00 | 2026-09-16T18:54:33.051000+00:00 |
| silver.slv_pdm_payment_account_controls | 6697085344338821673 | 1 | 2026-09-16T18:54:46.790000+00:00 | 2026-09-16T18:54:46.790000+00:00 |
| silver.slv_pdm_payment_entity_matches | 1593060644104585857 | 1 | 2026-09-16T18:54:47.236000+00:00 | 2026-09-16T18:54:47.236000+00:00 |
| silver.slv_pdm_payment_event_correlation | 9119788643608512659 | 1 | 2026-09-16T18:54:39.873000+00:00 | 2026-09-16T18:54:39.873000+00:00 |
| silver.slv_pdm_payment_lifecycle | 375403359783897685 | 1 | 2026-09-16T18:54:40.484000+00:00 | 2026-09-16T18:54:40.484000+00:00 |
| silver.slv_pdm_payment_party_roles | 6843176813039862592 | 1 | 2026-09-16T18:54:29.672000+00:00 | 2026-09-16T18:54:29.672000+00:00 |
| silver.slv_pdm_payment_technical_events | 7355553849442913429 | 1 | 2026-09-16T18:54:35.512000+00:00 | 2026-09-16T18:54:35.512000+00:00 |
| silver.slv_pdm_payments_accounts | 5876663452883241724 | 1 | 2026-09-16T18:54:30.006000+00:00 | 2026-09-16T18:54:30.006000+00:00 |
| silver.slv_pdm_payments_messages | 4144326136021077697 | 1 | 2026-09-16T18:54:34.121000+00:00 | 2026-09-16T18:54:34.121000+00:00 |
| silver.slv_pdm_payments_party | 7659761684753107985 | 1 | 2026-09-16T18:54:30.412000+00:00 | 2026-09-16T18:54:30.412000+00:00 |
| silver.slv_pdm_payments_plm_lifecycle_events | 6246608283291832008 | 1 | 2026-09-16T18:54:27.464000+00:00 | 2026-09-16T18:54:27.464000+00:00 |
| silver.slv_pdm_payments_pmn_lifecycle_events | 1351183287875356231 | 1 | 2026-09-16T18:54:28.778000+00:00 | 2026-09-16T18:54:28.778000+00:00 |
| silver.slv_pdm_payments_reconciliation | 950291519067670888 | 1 | 2026-09-16T18:54:41.019000+00:00 | 2026-09-16T18:54:41.019000+00:00 |
| silver.slv_pdm_payments_remittance | 4729795066153550771 | 1 | 2026-09-16T18:54:30.821000+00:00 | 2026-09-16T18:54:30.821000+00:00 |
| silver.slv_pdm_payments_status_report | 3875089004620435326 | 1 | 2026-09-16T18:54:34.492000+00:00 | 2026-09-16T18:54:34.492000+00:00 |
| silver.slv_pdm_payments_transactions | 7409911256374856298 | 1 | 2026-09-16T18:54:34.925000+00:00 | 2026-09-16T18:54:34.925000+00:00 |
| silver.slv_pdm_saccos | 1414849726320626063 | 1 | 2026-09-16T18:54:33.372000+00:00 | 2026-09-16T18:54:33.372000+00:00 |
| silver.slv_pdm_special_groups | 9021822515787875145 | 1 | 2026-09-16T18:54:33.681000+00:00 | 2026-09-16T18:54:33.681000+00:00 |
| silver_vault.vlt_pdm_beneficiary_identity | 3206473111296297629 | 1 | 2026-09-16T18:54:32.031000+00:00 | 2026-09-16T18:54:32.031000+00:00 |
| silver_vault.vlt_pdm_beneficiary_identity_alerts | 3854132345932432513 | 1 | 2026-09-16T18:54:52.073000+00:00 | 2026-09-16T18:54:52.073000+00:00 |
| silver_vault.vlt_pdm_beneficiary_identity_signals | 162586032016346741 | 1 | 2026-09-16T18:54:57.039000+00:00 | 2026-09-16T18:54:57.039000+00:00 |
| silver_vault.vlt_pdm_beneficiary_token_link | 7388857501816539743 | 1 | 2026-09-16T15:19:27.286000+00:00 | 2026-09-16T15:19:27.286000+00:00 |
| staging.uganda_district_geojson | 3459270928034906645 | 1 | 2026-09-16T18:41:06.790000+00:00 | 2026-09-16T18:41:06.790000+00:00 |
| staging.uganda_superset_district_iso | 4608549076108656905 | 1 | 2026-09-16T18:41:07.187000+00:00 | 2026-09-16T18:41:07.187000+00:00 |

## 6. Failure-logging privacy investigation

DEFECT CONFIRMED (source path; actual historic log exposure UNVERIFIED). `services/kafka-consumer-events/kafka_consumer_events.py:485` builds failure_envelope with original_key_base64, original_payload_base64, business_key and unsanitized failure_reason. The acknowledged-DLQ branch at line 605 emits `**envelope` through logger.error. Encoding is reversible and original key/business key can be emitted directly.

A purely synthetic serialization check using invented beneficiary/account/NIN/name/phone/email values and a synthetic key proved that this emission expression retains a recoverable full payload/key. No sensitive values were generated/read and no service function contacting Kafka/MinIO was invoked. This is confirmation of the code defect, not proof a real failure occurred or installed container code matches every repository line.

| Field class | Source-path conclusion |
|---|---|
| Original payload | Emitted encoded in successful DLQ error path; no claim of direct clear full payload emission |
| Original key | Encoded copy plus decoded business_key in successful DLQ path |
| Base64 payload/key | Explicitly emitted |
| Beneficiary IDs, account identifiers, NIN, names, phone/email and other business fields | Recoverable if present in original payload; key/error text may also expose clear values |
| Failed DLQ publication | Fail-stop path at line 722 emits selected metadata/failure_reason rather than full encoded envelope; unsanitized error text still presents possible exposure |

DLQ publication intentionally retains full recoverable payload for restricted replay; do not remove replay payload merely to fix logging. `services/shared/data_protection.py:redact_for_log()` scrubs email/phone only; NIN regex is unused. `services/agent-api/observability.py:emit()` relies on callers. `tests/test_data_protection.py:test_logs_do_not_expose_protected_values` exercises the helper, not this consumer branch. Focused Kafka tests cover publication/commit semantics, not full-envelope log redaction.

Smallest remediation boundary, not implemented: replace successful-DLQ full-envelope logging with an explicit safe metadata allowlist and sanitized error category/text; review adjacent consumer error/fail-stop emissions and add synthetic log-capture tests preserving full restricted DLQ publication and durable-before-commit/fail-stop semantics. This is a Gate 3 blocker and potential Gate 2 privacy defect requiring controlled review.

## 7. Audit persistence

UNVERIFIED effective runtime durability: no agent-api project container is present, so no effective audit path/mount can be inspected and no replacement demonstration is possible without prohibited changes.

Repository design is EPHEMERAL at its default path: `services/agent-api/config.py` defaults to `/tmp/dp-agent/audit.jsonl`, `audit.py` flushes/fsyncs local JSONL, and Compose provides no audit persistence mount. Fsync does not make the writable container layer survive replacement. No audit content was read and no container was replaced. Durable retention design remains a blocker.

## 8. Accepted ML provenance

VERIFIED repository metadata: model_name=`pdm_default_risk_strict_early_warning`, model_version=`v1`, trained_at_utc=`2026-09-08T10:48:29.113055+00:00`. Artefacts under `services/pdm-ml/models/`:

| File | SHA256 |
|---|---|
| pdm_default_risk_strict_early_warning_v1.joblib | 3e999976325c877ff3696eed82790ccf683a23537017f15d0080ae95c1778ae2 |
| pdm_default_risk_strict_early_warning_v1.json | cfcf07879d2c459109823833f349669e6d3f84b19b149f7e00c1634450d0cd33 |
| pdm_default_risk_strict_early_warning_v1_metadata.json | 51190742deb5c394fe0de95f8b9c6417dfeb237db512d33bf5a7bf7bf216c135 |

Feature contract: metadata lists 20 input fields: loan_age_days, months_since_disbursement, project_type, special_group, amount_approved, amount_disbursed, interest_rate, interest_charged, loan_term_months, scheduled_monthly_repayment, amount_repaid_as_of_observation, outstanding_amount_as_of_observation, repayment_rate_as_of_observation, payment_failure_count, repayment_trend_3m, account_substituted, shared_identity_alert, nin_unverified, identity_alert_count, household_economic_score. Separate feature_contract_version is absent.

VERIFIED installed sklearn version in the stopped pdm-ml-features container: 1.9.1, read from its installed package source via an in-memory tar stream without starting it or extracting files. Live accepted scorer runtime Python/sklearn: UNVERIFIED because no scoring container is present. Exact Python version not executed; Dockerfile's Python 3.11 family is a declaration, not a verified runtime version.

Training sklearn version is absent from metadata. POC LIMITATION: preserve Gate 2's recorded earlier estimator 1.9.0 versus runtime 1.9.1 warning; training version cannot be proven here. Hashes do not establish complete input/environment reproducibility or release history. PRODUCTION FOLLOW-UP: compatible pinning, immutable release manifests and promotion/rollback. No artefacts/prediction rows were changed/read for scoring, and no model was loaded, retrained or rescored.

## 9. Newly confirmed blockers and interpretation

- DRIFT / DEFECT: protected recovery tag missing.
- DRIFT / DEFECT: expected backup bucket absent; both protected prefixes unavailable at inspected endpoint.
- DEFECT: full recoverable failure payload/key emitted by acknowledged-DLQ logging source path.
- UNVERIFIED: effective durable agent audit persistence; default design remains ephemeral.
- VERIFIED missing controls: active data bucket has no lifecycle, versioning or object lock.
- UNVERIFIED: accepted scorer runtime environment and complete historic ML training provenance.
- Operational follow-up: stopped DuckDB exit 1; cause not inspected.

Policy approval, dry-run planner/tests, reference-aware fixture tests, replay ledger/suppression and isolated restore requirements remain outstanding. No control is marked PASS solely because its configuration exists.

## 10. Frozen-state and safety conclusion

Git HEAD and Nessie main match the supplied frozen references. Full Gate 2 frozen recovery-state confirmation is NOT possible: expected tag and backups are missing. These discrepancies were observed, not caused/remediated by this exercise; cause/time remain unknown. Current table metadata was inventoried, not a fresh complete validation of Gate 2 accepted row contents.

No Kafka mutation, offset mutation, replay, MinIO mutation, Iceberg maintenance, Nessie mutation, dbt execution, token-link materialisation, ML retraining/scoring, service restart, Gate 2 evidence modification or protected-backup mutation was performed. Only Gate 3 documentation is written. Background services may operate normally; repository diff alone cannot establish absence of autonomous runtime activity.


## Gate 3.3 reconciliation update

Historical target lookup using explicit Nessie ref@hash returns commit-not-found; NESSIE TAG RECREATION BLOCKED. Backup location is BACKUP LOCATION INCONCLUSIVE: active bucket absent, current MinIO volume postdates both backup timestamps, no older MinIO candidate identified in local container/volume metadata; privileged filesystem inspection unavailable. Cause is not proven. Historical verification evidence remains valid as a record, but backup is not currently available for restore.

Consumer logging source remediation uses metadata-only allowlisting and omits exception text/tracebacks; all 48 selected tests passed, including 13 data-protection tests. DLQ/replay content and commit/retry semantics remain unchanged. Running consumer was not restarted; runtime activation remains outstanding. No tag or backup was created. See [Gate 3.3 report](07_recovery_reconciliation_and_logging_remediation.md). Earlier Gate 3.2 observations remain historical, not overwritten.
