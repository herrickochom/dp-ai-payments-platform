import importlib.util
import os
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


producer = load("payment_producer_separation", "services/payment-producer/kafka_producer.py")
consumer = load("payment_consumer_separation", "services/kafka-consumer-events/kafka_consumer_events.py")
GENERATOR_DIR = ROOT / "services/payment-xml-generator"
sys.path.insert(0, str(GENERATOR_DIR))
generator_common = load("generator_common", "services/payment-xml-generator/generator_common.py")


TECHNICAL_XML = """<?xml version="1.0" encoding="utf-8"?>
<TechnicalPaymentEvent>
  <EventId>ICMN-PMN-LOAN-000001</EventId>
  <MessageId>PMN-ROUTING-LOAN-000001</MessageId>
  <EventFamily>TECHNICAL_PAYMENT_EVENT</EventFamily><EventType>PMN</EventType>
  <CorrelationId>4c6bf4fa-9dd1-5a43-b760-a819eed49aa8</CorrelationId>
  <PaymentInstructionId>INSTR-LOAN-000001</PaymentInstructionId>
  <EndToEndId>LOAN-000001</EndToEndId>
  <TransactionId>TX-LOAN-000001</TransactionId>
  <UETR>20c66dab-f33b-4bad-8129-473af72511e2</UETR><BusinessReference>LOAN-000001</BusinessReference>
  <XTrace>6cc270bb-f8c0-55a5-a38a-a4ae496d2005</XTrace><XChannel>BANK</XChannel>
  <XSourceSystem>BOU_PAYMENT_GATEWAY</XSourceSystem>
  <XTargetSystem>UGANDA_POST</XTargetSystem><XService>payment-routing</XService>
  <XOperation>route</XOperation><XComponent>pmn-router</XComponent><XNode>bou-routing-01</XNode><XHost>pmn-01</XHost>
  <XPaymentRoute>PDMIS&gt;BOU_PAYMENT_GATEWAY&gt;UGANDA_POST</XPaymentRoute>
  <XOriginatingInstitution>Bank of Uganda</XOriginatingInstitution>
  <XIntermediaryInstitution>Uganda Post Office</XIntermediaryInstitution>
  <XRouteDecision>SELECTED</XRouteDecision><XValidationStatus>VALIDATED</XValidationStatus>
  <XSubmissionStatus>ACCEPTED</XSubmissionStatus>
  <TechnicalStage>payment-routing</TechnicalStage><TechnicalStatus>PENDING</TechnicalStatus>
  <EventTimestamp>2026-09-01T11:00:00.000+03:00</EventTimestamp>
  <ProcessingTimestamp>2026-09-01T11:00:05.000+03:00</ProcessingTimestamp>
  <XLatencyMs>5000</XLatencyMs><XRetryCount>0</XRetryCount><XTimeoutIndicator>false</XTimeoutIndicator>
</TechnicalPaymentEvent>
"""

BUSINESS_XML = """<?xml version="1.0" encoding="utf-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.001.001.09">
  <CstmrCdtTrfInitn><GrpHdr><MsgId>MSG-1</MsgId><CreDtTm>2026-09-01T10:00:00Z</CreDtTm></GrpHdr>
  <PmtInf><Dbtr><Nm>Debtor</Nm></Dbtr><CdtTrfTxInf><Amt><InstdAmt Ccy="UGX">1000</InstdAmt></Amt>
  <Cdtr><Nm>Creditor</Nm></Cdtr></CdtTrfTxInf></PmtInf></CstmrCdtTrfInitn>
</Document>
"""


class PaymentEventSeparationTests(unittest.TestCase):
    def write_fixture(self, relative_path, content):
        temporary = tempfile.TemporaryDirectory()
        path = Path(temporary.name) / relative_path
        path.parent.mkdir(parents=True)
        path.write_text(content)
        self.addCleanup(temporary.cleanup)
        return path

    def test_standalone_technical_event_needs_no_iso_namespace(self):
        path = self.write_fixture("icmn/pmn/pain001/pmn_0001.xml", TECHNICAL_XML)
        event = producer.parse_event(str(path))
        parsed = json.loads(event["parsed_event_data"])
        self.assertEqual("TECHNICAL_PAYMENT_EVENT", event["event_family"])
        self.assertEqual("PMN", event["event_type"])
        self.assertEqual("LOAN-000001", parsed["end_to_end_id"])
        self.assertEqual("INSTR-LOAN-000001", parsed["instruction_id"])
        self.assertEqual("BOU_PAYMENT_GATEWAY", parsed["x_source_system"])
        self.assertEqual(5000, parsed["x_latency_ms"])
        self.assertIsNone(event["x_attributes"])
        self.assertNotIn("urn:iso:std:iso:20022", event["event_data"])

    def test_technical_partition_key_uses_stable_payment_identifier(self):
        plm_xml = TECHNICAL_XML.replace("PMN", "PLM").replace(
            "</TechnicalPaymentEvent>",
            "<XProvider>MTN Mobile Money</XProvider><XNetwork>MTN</XNetwork>"
            "<XBeneficiarySa>BEN-000001</XBeneficiarySa><XWalletReference>256770000001</XWalletReference>"
            "<XProviderTransactionId>MTN-TX-LOAN-000001</XProviderTransactionId>"
            "<XCreditStatus>CREDITED</XCreditStatus><XAgentReference>AGENT-0001</XAgentReference>"
            "<XCashoutStatus>COMPLETED</XCashoutStatus></TechnicalPaymentEvent>",
        )
        path = self.write_fixture("cpo/plm/pain002/plm_0001.xml", plm_xml)
        event = producer.parse_event(str(path))
        self.assertEqual("LOAN-000001", producer.select_business_key("cpo.plm.pain002", event))
        self.assertNotEqual(event["event_id"], producer.select_business_key("cpo.plm.pain002", event))

    def test_iso_business_event_has_no_technical_projection(self):
        path = self.write_fixture("icmn/vpm/pain001/vpm_0001.xml", BUSINESS_XML)
        event = producer.parse_event(str(path))
        self.assertEqual("PAYMENT_BUSINESS_EVENT", event["event_family"])
        self.assertIsNone(event["x_attributes"])
        self.assertEqual("UGX", event["currency"])

    def test_raw_identity_is_unchanged_for_technical_topic(self):
        stamp = datetime(2026, 9, 12, tzinfo=timezone.utc)
        with patch.dict(os.environ, {"RAW_ROOT": "raw", "RAW_VERSION": "v2", "RAW_PREFIX": "raw/v2"}):
            self.assertEqual(
                "raw/v2/category=icmn/source_group=icmn/source_system=pmn/"
                "year=2026/month=09/day=12/topic=icmn.pmn.pain001/partition=2/offset=7/record.avro",
                consumer.deterministic_s3_key("icmn.pmn.pain001", 2, 7, stamp),
            )

    def test_dbt_keeps_technical_events_out_of_iso_message_models(self):
        messages = (ROOT / "transform/dbt/models/silver/slv_pdm_payments_messages.sql").read_text()
        statuses = (ROOT / "transform/dbt/models/silver/slv_pdm_payments_status_report.sql").read_text()
        correlation = (ROOT / "transform/dbt/models/silver/slv_pdm_payment_event_correlation.sql").read_text()
        self.assertNotIn("br_pdm_icmn_pmn_pain001", messages)
        self.assertNotIn("br_pdm_cpo_plm_pain002", messages)
        self.assertNotIn("br_pdm_cpo_plm_pain002", statuses)
        self.assertIn("'UNMATCHED'", correlation)
        self.assertNotIn("like", correlation.lower())

    def test_source_telemetry_is_preserved_without_random_defaults(self):
        path = self.write_fixture("icmn/pmn/pain001/pmn_0001.xml", TECHNICAL_XML)
        first = json.loads(producer.parse_event(str(path))["parsed_event_data"])
        second = json.loads(producer.parse_event(str(path))["parsed_event_data"])
        for field, expected in {
            "x_trace": "6cc270bb-f8c0-55a5-a38a-a4ae496d2005",
            "x_latency_ms": 5000, "x_retry_count": 0,
            "x_timeout_indicator": False, "x_error_code": None,
        }.items():
            self.assertEqual(expected, first[field])
            self.assertEqual(first[field], second[field])

    def test_lifecycle_lineage_uses_specific_bronze_models(self):
        for family in ("pmn", "plm"):
            bronze = (ROOT / f"transform/dbt/models/bronze/br_pdm_payments_{family}_lifecycle_events.sql").read_text()
            silver = (ROOT / f"transform/dbt/models/silver/slv_pdm_payments_{family}_lifecycle_events.sql").read_text()
            self.assertIn("event_family = 'TECHNICAL_PAYMENT_EVENT'", bronze)
            self.assertIn(f"ref('br_pdm_payments_{family}_lifecycle_events')", silver)

    def test_generator_technical_contract_is_deterministic_and_correlated(self):
        context = {
            "loan": {"loan_id": "LOAN-000014", "disbursement_date": "2026-09-01"},
            "beneficiary": {"beneficiary_id": "BEN-000014", "phone": "256770000014"},
            "intermediary": {"name": "Pearl Bank Uganda", "bic": "PRBLUGKA"},
            "lifecycle": {
                "vpm_transaction_id": "TX-LOAN-000014",
                "mtn_transaction_id": "MTN-TX-LOAN-000014",
                "airtel_transaction_id": "AIRTEL-TX-LOAN-000014",
            },
            "scenario": generator_common.SCENARIO_SUCCESS,
        }
        pmn = generator_common.technical_event_fields(context, "PMN")
        plm = generator_common.technical_event_fields(context, "PLM")
        self.assertEqual(pmn, generator_common.technical_event_fields(context, "PMN"))
        self.assertNotEqual(pmn["EventId"], plm["EventId"])
        for field in ("CorrelationId", "EndToEndId", "BusinessReference", "XTrace"):
            self.assertEqual(pmn[field], plm[field])
        self.assertEqual("BOU_PAYMENT_GATEWAY", pmn["XSourceSystem"])
        self.assertEqual("PEARL_BANK", plm["XSourceSystem"])
        self.assertEqual("MTN_MOMO", plm["XTargetSystem"])
        self.assertIn("XPaymentRoute", pmn)
        self.assertNotIn("XProvider", pmn)
        self.assertIn("XProvider", plm)
        self.assertIn("XWalletReference", plm)
        self.assertNotIn("XPaymentRoute", plm)

    def test_lifecycle_models_are_explicit_and_have_no_bronze_to_bronze_refs(self):
        models = [
            ROOT / "transform/dbt/models/bronze/br_pdm_payments_pmn_lifecycle_events.sql",
            ROOT / "transform/dbt/models/bronze/br_pdm_payments_plm_lifecycle_events.sql",
            ROOT / "transform/dbt/models/silver/slv_pdm_payments_pmn_lifecycle_events.sql",
            ROOT / "transform/dbt/models/silver/slv_pdm_payments_plm_lifecycle_events.sql",
            ROOT / "transform/dbt/models/silver/slv_pdm_payment_technical_events.sql",
        ]
        for model in models:
            self.assertNotIn("select *", model.read_text().lower())
        self.assertIn("ref('stg_pdm_icmn_pmn_pain001')", models[0].read_text())
        self.assertIn("ref('stg_pdm_cpo_plm_pain002')", models[1].read_text())


if __name__ == "__main__":
    unittest.main()
