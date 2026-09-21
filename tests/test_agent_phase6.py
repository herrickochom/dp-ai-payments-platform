"""Phase 6 architectural acceptance tests for payment technical events.

These tests guard boundaries and cross-model contracts. Detailed parser,
generator, Kafka delivery, and sink behavior remains covered by the focused
unit and integration suites.
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path
from xml.etree import ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
GENERATOR_DIR = ROOT / "services/payment-xml-generator"
sys.path.insert(0, str(GENERATOR_DIR))


def load_module(name: str, relative_path: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


producer = load_module("phase6_payment_producer", "services/payment-producer/kafka_producer.py")

from generator_common import (  # noqa: E402
    SCENARIO_AGENT,
    SCENARIO_DELAYED,
    SCENARIO_FAILED,
    SCENARIO_PENDING,
    SCENARIO_PROVIDER,
    SCENARIO_REJECTED,
    SCENARIO_SUCCESS,
    SCENARIO_VALIDATION,
    technical_event_fields,
)
from icmn_generator import generate_pmn  # noqa: E402


DBT = ROOT / "transform/dbt/models"
PHASE6_SQL = (
    DBT / "staging/stg_pdm_icmn_pmn_pain001.sql",
    DBT / "staging/stg_pdm_cpo_plm_pain002.sql",
    DBT / "bronze/br_pdm_payments_pmn_lifecycle_events.sql",
    DBT / "bronze/br_pdm_payments_plm_lifecycle_events.sql",
    DBT / "silver/slv_pdm_payments_pmn_lifecycle_events.sql",
    DBT / "silver/slv_pdm_payments_plm_lifecycle_events.sql",
    DBT / "silver/slv_pdm_payment_technical_events.sql",
    DBT / "silver/slv_pdm_payment_event_correlation.sql",
)

SHARED_FIELDS = {
    "event_id", "message_id", "event_family", "event_type", "correlation_id",
    "instruction_id", "end_to_end_id", "transaction_id", "uetr",
    "business_reference", "x_trace", "x_channel", "x_source_system",
    "x_target_system", "x_service", "x_operation", "x_component", "x_node",
    "x_host", "technical_stage", "technical_status", "event_timestamp",
    "processing_timestamp", "x_latency_ms", "x_retry_count",
    "x_timeout_indicator", "x_error_code", "x_error_category",
}
PMN_FIELDS = {
    "x_payment_route", "x_originating_institution", "x_intermediary_institution",
    "x_route_decision", "x_validation_status", "x_submission_status",
}
PLM_FIELDS = {
    "x_provider", "x_network", "x_beneficiary_sa", "x_wallet_reference",
    "x_provider_transaction_id", "x_credit_status", "x_agent_reference",
    "x_cashout_status",
}


def technical_context(scenario: str, serial: int = 1) -> dict:
    loan_id = f"LOAN-{serial:06d}"
    return {
        "loan": {
            "loan_id": loan_id,
            "loan_status": "DISBURSED",
            "disbursement_date": "2026-09-01",
        },
        "beneficiary": {"beneficiary_id": f"BEN-{serial:06d}", "phone": f"25677{serial:07d}"},
        "intermediary": {"name": "Uganda Post Office", "bic": "UGPOUGKA"},
        "lifecycle": {
            "vpm_transaction_id": f"TX-{loan_id}",
            "mtn_transaction_id": f"MTN-TX-{loan_id}",
            "airtel_transaction_id": f"AIRTEL-TX-{loan_id}",
        },
        "scenario": scenario,
    }


def projected_names(sql_path: Path) -> set[str]:
    """Return explicit snake-case identifiers present in a SQL contract."""
    return set(re.findall(r"\b[a-z][a-z0-9_]+\b", sql_path.read_text()))


def beneficiary_experience(
    business_status: str,
    credit_status: str | None,
    cashout_status: str | None,
    delay_excessive: bool = False,
    reached_provider: bool = True,
    validation_failed: bool = False,
) -> tuple[str, str | None]:
    """Executable Phase 6 outcome decision table."""
    if validation_failed:
        return "NOT_COMPLETED", "BENEFICIARY_DATA_VALIDATION"
    if not reached_provider:
        return "NOT_COMPLETED", "PAYMENT_SYSTEM_JOURNEY"
    if business_status == "SUCCESS" and credit_status == "SUCCESS" and cashout_status == "FAILED":
        return "NOT_COMPLETED", "FINAL_MILE_CASHOUT"
    if business_status == "SUCCESS" and delay_excessive:
        return "DEGRADED", "EXCESSIVE_JOURNEY_DELAY"
    return "COMPLETED", None


def test_business_and_technical_event_families_are_not_inferred_from_legacy_topics(tmp_path):
    technical = tmp_path / "icmn/pmn/pain001/event.xml"
    technical.parent.mkdir(parents=True)
    technical.write_bytes(generate_pmn(technical_context(SCENARIO_SUCCESS)))
    parsed = producer.parse_event(str(technical))
    assert parsed["event_family"] == "TECHNICAL_PAYMENT_EVENT"
    assert parsed["event_type"] == "PMN"
    assert json.loads(parsed["parsed_event_data"])["event_type"] == "PMN"

    business = next((ROOT / "data/icmn/vpm/pain001").glob("*.xml"))
    assert producer.parse_event(str(business))["event_family"] == "PAYMENT_BUSINESS_EVENT"


def test_technical_extensions_do_not_leak_into_iso_business_xml_contracts():
    technical_tags = {"EventFamily", "EventType", "XPaymentRoute", "XProvider", "TechnicalStage"}
    for directory in (ROOT / "data/icmn/vpm/pain001", ROOT / "data/cpo/psn/pain002"):
        sample = next(directory.glob("*.xml"))
        tags = {node.tag.rsplit("}", 1)[-1] for node in ET.parse(sample).iter()}
        assert technical_tags.isdisjoint(tags)


def test_pmn_and_plm_have_shared_but_distinct_explicit_contracts():
    pmn = projected_names(DBT / "bronze/br_pdm_payments_pmn_lifecycle_events.sql")
    plm = projected_names(DBT / "bronze/br_pdm_payments_plm_lifecycle_events.sql")
    assert SHARED_FIELDS <= pmn and SHARED_FIELDS <= plm
    assert PMN_FIELDS <= pmn and PMN_FIELDS.isdisjoint(plm)
    assert PLM_FIELDS <= plm and PLM_FIELDS.isdisjoint(pmn)


def test_generated_event_types_are_distinct_and_deterministically_correlated():
    context = technical_context(SCENARIO_SUCCESS)
    pmn = technical_event_fields(context, "PMN")
    plm = technical_event_fields(context, "PLM")
    assert pmn["EventType"] == "PMN" and plm["EventType"] == "PLM"
    assert pmn["EventId"] != plm["EventId"]
    for field in ("CorrelationId", "EndToEndId", "TransactionId", "UETR", "BusinessReference"):
        assert pmn[field] == plm[field]
    assert "XPaymentRoute" in pmn and "XProvider" not in pmn
    assert "XProvider" in plm and "XPaymentRoute" not in plm


def test_correlation_priority_is_exact_and_has_no_fuzzy_matching_or_fanout():
    sql = (DBT / "silver/slv_pdm_payment_event_correlation.sql").read_text().lower()
    positions = [sql.index(token) for token in ("'uetr'", "'transaction_id'", "'end_to_end_id'", "'instruction_id'")]
    assert positions == sorted(positions)
    assert "'uetr' as correlation_method, 1 as match_priority" in sql
    assert all(f"'{method}', {priority}" in sql for method, priority in (
        ("transaction_id", 2), ("end_to_end_id", 3), ("instruction_id", 4)
    ))
    assert " like " not in sql and "levenshtein" not in sql and "soundex" not in sql
    assert "partition by technical_event_id" in sql
    assert "r.match_rank = 1" in sql


def test_unmatched_technical_events_remain_observable():
    sql = (DBT / "silver/slv_pdm_payment_event_correlation.sql").read_text().lower()
    assert "from technical t" in sql
    assert "left join ranked r" in sql
    assert "then 'unmatched' else 'matched'" in sql


def test_all_required_payment_scenarios_have_explicit_technical_outcomes():
    scenarios = (
        SCENARIO_SUCCESS, SCENARIO_PENDING, SCENARIO_DELAYED, SCENARIO_REJECTED,
        SCENARIO_FAILED, SCENARIO_VALIDATION, SCENARIO_PROVIDER, SCENARIO_AGENT,
    )
    outcomes = {scenario: technical_event_fields(technical_context(scenario), "PLM") for scenario in scenarios}
    assert outcomes[SCENARIO_SUCCESS]["TechnicalStatus"] == "COMPLETED"
    assert outcomes[SCENARIO_PENDING]["TechnicalStatus"] == "PENDING"
    assert outcomes[SCENARIO_DELAYED]["TechnicalStatus"] == "DELAYED"
    assert outcomes[SCENARIO_REJECTED]["TechnicalStatus"] == "FAILED"
    assert outcomes[SCENARIO_FAILED]["TechnicalStatus"] == "FAILED"
    assert outcomes[SCENARIO_VALIDATION]["XErrorCategory"] == "VALIDATION"
    assert outcomes[SCENARIO_PROVIDER]["XErrorCategory"] == "DOWNSTREAM_PROVIDER"
    assert outcomes[SCENARIO_AGENT]["XErrorCategory"] == "FINAL_MILE"


def test_beneficiary_experience_decision_table():
    assert beneficiary_experience("SUCCESS", "SUCCESS", "FAILED") == (
        "NOT_COMPLETED", "FINAL_MILE_CASHOUT"
    )
    assert beneficiary_experience("SUCCESS", "SUCCESS", None, delay_excessive=True) == (
        "DEGRADED", "EXCESSIVE_JOURNEY_DELAY"
    )
    assert beneficiary_experience("FAILED", None, None, reached_provider=False) == (
        "NOT_COMPLETED", "PAYMENT_SYSTEM_JOURNEY"
    )
    assert beneficiary_experience("FAILED", None, None, validation_failed=True) == (
        "NOT_COMPLETED", "BENEFICIARY_DATA_VALIDATION"
    )


def test_raw_to_silver_lineage_is_type_specific():
    expected = {
        "pmn": ("icmn.pmn.pain001", "stg_pdm_icmn_pmn_pain001"),
        "plm": ("cpo.plm.pain002", "stg_pdm_cpo_plm_pain002"),
    }
    for family, (topic, staging) in expected.items():
        staging_sql = (DBT / f"staging/{staging}.sql").read_text()
        bronze_name = f"br_pdm_payments_{family}_lifecycle_events"
        bronze_sql = (DBT / f"bronze/{bronze_name}.sql").read_text()
        silver_sql = (DBT / f"silver/slv_pdm_payments_{family}_lifecycle_events.sql").read_text()
        assert f"topic={topic}" in staging_sql
        assert f"ref('{staging}')" in bronze_sql
        assert f"ref('{bronze_name}')" in silver_sql


def test_unified_models_consume_only_intended_phase6_upstreams():
    unified = (DBT / "silver/slv_pdm_payment_technical_events.sql").read_text()
    correlation = (DBT / "silver/slv_pdm_payment_event_correlation.sql").read_text()
    assert "ref('slv_pdm_payments_pmn_lifecycle_events')" in unified
    assert "ref('slv_pdm_payments_plm_lifecycle_events')" in unified
    assert "ref('slv_pdm_payment_technical_events')" in correlation
    assert "ref('slv_pdm_payments_transactions')" in correlation


def test_phase6_sql_contracts_have_explicit_columns_and_no_select_star():
    for path in PHASE6_SQL:
        sql = path.read_text().lower()
        assert not re.search(r"\bselect\s+(?:[a-z_][a-z0-9_]*\.)?\*", sql), path
    technical = projected_names(DBT / "silver/slv_pdm_payment_technical_events.sql")
    correlation = projected_names(DBT / "silver/slv_pdm_payment_event_correlation.sql")
    assert SHARED_FIELDS <= technical
    assert PMN_FIELDS <= technical
    assert (PLM_FIELDS - {"x_beneficiary_sa"}) <= technical
    assert "x_beneficiary_sa" not in technical
    assert {
        "technical_event_id", "correlation_method", "match_status",
        "business_source_system", "business_message_id", "business_transaction_id",
    } <= correlation
