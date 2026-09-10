#!/usr/bin/env python3
"""Build/update the privacy-limited PDM Investigators dashboard in Superset 6.1."""
from __future__ import annotations

from pdm_leadership_dashboard import (
    big_number,
    build_suite_dashboard,
    metric,
    pie,
    ranked,
    table,
    time_line,
)

DATASETS = {
    "identity": "cns_pdm_beneficiary_identity_alerts",
    "agents": "cns_pdm_agent_risk_indicators",
    "fraud": "cns_pdm_fraud_risk_insights",
    "duplicates": "cns_pdm_duplicate_fragmentation_alerts",
    "lifecycle": "cns_pdm_lifecycle_exceptions",
    "cases": "cns_pdm_lifecycle_exception_cases",
    "ai": "cns_pdm_ai_default_risk",
    "trace": "cns_pdm_end_to_end_traceability",
}


def filtered_count(column: str, label: str, subject: str, comparator):
    params = big_number(column, label, ",d", "COUNT_DISTINCT")
    params["adhoc_filters"] = [{"expressionType": "SIMPLE", "subject": subject,
        "operator": "IN", "operatorId": "IN", "comparator": comparator,
        "clause": "WHERE", "sqlExpression": None, "isExtra": False,
        "isNew": False, "filterOptionName": f"filter_{subject}"}]
    return params

CHARTS = [
    {"name": "IDENTITY INDICATORS", "dataset": "identity", "viz": "big_number_total", "params": big_number("identity_alert_count", "Identity indicators", ",d"), "required": ["identity_alert_count"]},
    {"name": "DETERMINISTIC HIGH RISK CASES", "dataset": "fraud", "viz": "big_number_total", "params": filtered_count("risk_case_sk", "High-risk cases", "risk_band", ["HIGH"]), "required": ["risk_case_sk", "risk_band"]},
    {"name": "LIFECYCLE EXCEPTION COUNT", "dataset": "cases", "viz": "big_number_total", "params": big_number("loan_id", "Exception cases", ",d", "COUNT"), "required": ["loan_id"]},
    {"name": "AI PRIORITY REVIEW CASES", "dataset": "ai", "viz": "big_number_total", "params": filtered_count("loan_id", "AI priority cases", "ai_risk_band", ["HIGH", "SEVERE"]), "required": ["loan_id", "requires_priority_review", "ai_risk_band"]},
    {"name": "DETERMINISTIC RISK DISTRIBUTION", "dataset": "fraud", "viz": "pie", "params": pie("risk_band", "risk_case_sk", "Cases", "COUNT_DISTINCT"), "required": ["risk_band", "risk_case_sk"]},
    {"name": "IDENTITY INVESTIGATION QUEUE", "dataset": "identity", "viz": "table", "params": table(["beneficiary_id", "region", "district", "has_unverified_nin", "has_shared_nin", "has_shared_phone", "has_shared_account", "has_account_substitution", "identity_alert_count", "identity_alert_reason", "identity_alert_severity", "requires_investigation", "interpretation"]), "required": ["beneficiary_id", "identity_alert_reason", "interpretation"]},
    {"name": "AGENT RISK INDICATORS", "dataset": "agents", "viz": "table", "params": table(["agent_id", "region", "district", "cashout_count", "total_cashout_amount", "reconciliation_exception_count", "unmatched_entity_cashout_count", "excess_amount_cashout_count", "rapid_cashout_count", "risk_score", "risk_band", "indicator_reason", "requires_review", "interpretation"]), "required": ["agent_id", "risk_score", "interpretation"]},
    {"name": "DUPLICATE AND FRAGMENTATION QUEUE", "dataset": "duplicates", "viz": "table", "params": table(["beneficiary_id", "loan_id", "sacco_id", "region", "district", "duplicate_indicator", "fragmentation_indicator", "shared_identity_indicator", "shared_account_indicator", "alert_count", "alert_reason", "alert_severity", "requires_review", "interpretation"]), "required": ["beneficiary_id", "loan_id", "alert_reason", "interpretation"]},
    {"name": "LIFECYCLE EXCEPTIONS BY STAGE", "dataset": "cases", "viz": "echarts_timeseries_bar", "params": ranked("lifecycle_stage", "loan_id", "Exceptions", ",d", "COUNT"), "required": ["lifecycle_stage", "loan_id"]},
    {"name": "LIFECYCLE EXCEPTION CASES", "dataset": "cases", "viz": "table", "params": table(["beneficiary_id", "loan_id", "sacco_id", "region", "district", "lifecycle_stage", "exception_type", "exception_reason", "exception_severity", "event_date", "current_status", "requires_review", "interpretation"]), "required": ["loan_id", "lifecycle_stage", "exception_type", "event_date"]},
    {"name": "AI DEFAULT RISK DISTRIBUTION", "dataset": "ai", "viz": "pie", "params": pie("ai_risk_band", "loan_id", "Scored loans", "COUNT_DISTINCT"), "required": ["ai_risk_band", "loan_id"]},
    {"name": "AI DEFAULT RISK BY OBSERVATION DATE", "dataset": "ai", "viz": "echarts_timeseries_line", "params": time_line("observation_date", [metric("probability_default_90d", "Average default probability", "AVG")], number_format=".2%"), "required": ["observation_date", "probability_default_90d"]},
    {"name": "AI DEFAULT-RISK CASE QUEUE", "dataset": "ai", "viz": "table", "params": table(["risk_rank", "beneficiary_id", "loan_id", "sacco_id", "region", "district", "project_type", "observation_date", "probability_default_90d", "ai_risk_band", "requires_priority_review", "model_name", "model_version", "interpretation"]), "required": ["risk_rank", "loan_id", "interpretation"]},
    {"name": "END-TO-END CASE TRACEABILITY", "dataset": "trace", "viz": "table", "params": table(["beneficiary_id", "household_id", "business_plan_id", "loan_id", "sacco_id", "region", "district", "project_type", "loan_status", "approved_amount", "disbursed_amount", "repaid_amount", "outstanding_amount", "payment_count", "reconciliation_exception_count", "identity_alert_count", "duplicate_alert_count", "fragmentation_alert_count", "lifecycle_exception_count", "risk_score", "risk_band", "probability_default_90d", "ai_risk_band", "risk_rank", "requires_priority_review", "deterministic_risk_interpretation", "ai_interpretation"]), "required": ["loan_id", "risk_band", "ai_risk_band", "deterministic_risk_interpretation", "ai_interpretation"]},
]

SECTIONS = [
    ("INVESTIGATION OVERVIEW", ["IDENTITY INDICATORS", "DETERMINISTIC HIGH RISK CASES", "LIFECYCLE EXCEPTION COUNT", "AI PRIORITY REVIEW CASES"]),
    ("IDENTITY & FRAUD INDICATORS", ["DETERMINISTIC RISK DISTRIBUTION", "IDENTITY INVESTIGATION QUEUE", "AGENT RISK INDICATORS"]),
    ("DUPLICATE / FRAGMENTATION ALERTS", ["DUPLICATE AND FRAGMENTATION QUEUE"]),
    ("LIFECYCLE EXCEPTIONS", ["LIFECYCLE EXCEPTIONS BY STAGE", "LIFECYCLE EXCEPTION CASES"]),
    ("AI DEFAULT-RISK CASES", ["AI DEFAULT RISK DISTRIBUTION", "AI DEFAULT RISK BY OBSERVATION DATE", "AI DEFAULT-RISK CASE QUEUE"]),
    ("END-TO-END TRACEABILITY", ["END-TO-END CASE TRACEABILITY"]),
]


def update_dashboard() -> None:
    build_suite_dashboard(title="PDM INVESTIGATION & ASSURANCE", slug="pdm-investigators", active="Investigators",
                          dataset_specs=DATASETS, temporal={"ai": "observation_date", "cases": "event_date"},
                          chart_specs=CHARTS, sections=SECTIONS,
                          filter_specs=[("Observation date", "ai", "observation_date"),
                                        ("Region", "trace", "region"), ("District", "trace", "district"),
                                        ("SACCO", "trace", "sacco_id"), ("Risk band", "trace", "risk_band"),
                                        ("AI risk band", "trace", "ai_risk_band"),
                                        ("Exception type", "cases", "exception_type"),
                                        ("Severity", "cases", "exception_severity")])


if __name__ == "__main__":
    update_dashboard()
