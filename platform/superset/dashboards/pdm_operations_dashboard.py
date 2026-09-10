#!/usr/bin/env python3
"""Build/update the PDM Operations dashboard in Superset 6.1."""
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
    "payments": "cns_pdm_payment_operations",
    "payment_daily": "cns_pdm_payments_daily_summary",
    "agents": "cns_pdm_channel_agent_performance",
    "sacco": "cns_pdm_sacco_portfolio",
    "loans": "cns_pdm_loan_intervention_dashboard",
    "reconciliation": "cns_pdm_payment_reconciliation",
    "daily": "cns_pdm_daily_operational_metrics",
    "fund_flow": "cns_pdm_financial_fund_flow",
    "funnel": "cns_pdm_fund_flow_funnel",
}


def latest_daily(column: str, label: str, fmt: str = ",d", aggregate: str = "SUM"):
    params = big_number(column, label, fmt, aggregate)
    params.update({"granularity_sqla": "reporting_date", "time_range": "Last day"})
    return params

CHARTS = [
    {"name": "OPERATIONS PAYMENT COUNT", "dataset": "daily", "viz": "big_number_total", "params": latest_daily("payment_count", "Payments", ",d"), "required": ["payment_count"]},
    {"name": "OPERATIONS PAYMENT SUCCESS RATE", "dataset": "payment_daily", "viz": "big_number_total", "params": latest_daily("payment_success_rate", "Success rate", ".2%", "AVG"), "required": ["payment_success_rate"]},
    {"name": "OPERATIONS RECONCILIATION RATE", "dataset": "payment_daily", "viz": "big_number_total", "params": latest_daily("reconciliation_rate", "Reconciliation rate", ".2%", "AVG"), "required": ["reconciliation_rate"]},
    {"name": "OPERATIONS EXCEPTIONS", "dataset": "daily", "viz": "big_number_total", "params": latest_daily("operational_exception_count", "Exceptions", ",d"), "required": ["operational_exception_count"]},
    {"name": "DAILY PAYMENT ACTIVITY", "dataset": "payment_daily", "viz": "echarts_timeseries_line", "params": time_line("reporting_date", [metric("payment_count", "Payments"), metric("successful_payment_count", "Successful"), metric("failed_payment_count", "Failed")], number_format=",d"), "required": ["reporting_date", "payment_count", "successful_payment_count", "failed_payment_count"]},
    {"name": "PAYMENT STATUS", "dataset": "payments", "viz": "pie", "params": pie("payment_status", "payment_count", "Payments"), "required": ["payment_status", "payment_count"]},
    {"name": "SOURCE SYSTEM PERFORMANCE", "dataset": "payments", "viz": "echarts_timeseries_bar", "params": ranked("source_system", "payment_amount", "Amount (UGX)"), "required": ["source_system", "payment_amount"]},
    {"name": "AGENT PERFORMANCE", "dataset": "agents", "viz": "table", "params": table(["agent_id", "agent_name", "network_provider", "payment_count", "payment_amount", "payment_success_rate", "reconciliation_rate", "exception_count", "channel_risk_band"]), "required": ["agent_id", "payment_count", "payment_amount"]},
    {"name": "SACCO OUTSTANDING RANKING", "dataset": "sacco", "viz": "echarts_timeseries_bar", "params": ranked("sacco_name", "outstanding_amount", "Outstanding (UGX)"), "required": ["sacco_name", "outstanding_amount"]},
    {"name": "SACCO PORTFOLIO", "dataset": "sacco", "viz": "table", "params": table(["sacco_id", "sacco_name", "region", "district", "beneficiary_count", "loan_count", "approved_amount", "disbursed_amount", "repaid_amount", "outstanding_amount", "principal_repayment_rate", "operational_exception_count", "ai_priority_review_count"]), "required": ["sacco_id", "sacco_name", "loan_count"]},
    {"name": "LOANS REQUIRING ACTION", "dataset": "loans", "viz": "table", "params": table(["loan_id", "beneficiary_id", "sacco_name", "region", "district", "project_type", "loan_status", "approved_amount", "disbursed_amount", "repaid_amount", "outstanding_amount", "payment_failure_count", "intervention_status", "probability_default_90d", "ai_risk_band", "risk_rank", "recommended_operational_action", "ai_interpretation"]), "required": ["loan_id", "intervention_status", "ai_interpretation"]},
    {"name": "RECONCILED VS UNRECONCILED", "dataset": "reconciliation", "viz": "echarts_timeseries_line", "params": time_line("reporting_date", [metric("reconciled_count", "Reconciled"), metric("unreconciled_count", "Unreconciled")], number_format=",d"), "required": ["reporting_date", "reconciled_count", "unreconciled_count"]},
    {"name": "RECONCILIATION EXCEPTIONS", "dataset": "reconciliation", "viz": "echarts_timeseries_line", "params": time_line("reporting_date", [metric("exception_count", "Exceptions")], number_format=",d"), "required": ["reporting_date", "exception_count"]},
    {"name": "FUND FLOW FUNNEL", "dataset": "funnel", "viz": "funnel", "params": {"groupby": ["stage"], "metric": metric("amount", "Amount (UGX)"), "adhoc_filters": [], "row_limit": 10, "show_labels": True, "number_format": ",.2f"}, "required": ["stage", "amount", "count"]},
    {"name": "DAILY FINANCIAL TRENDS", "dataset": "daily", "viz": "echarts_timeseries_line", "params": time_line("reporting_date", [metric("payment_amount", "Payments"), metric("disbursement_amount", "Disbursements"), metric("repayment_amount", "Repayments")]), "required": ["reporting_date", "payment_amount", "disbursement_amount", "repayment_amount"]},
]

SECTIONS = [
    ("OPERATIONS OVERVIEW", ["OPERATIONS PAYMENT COUNT", "OPERATIONS PAYMENT SUCCESS RATE", "OPERATIONS RECONCILIATION RATE", "OPERATIONS EXCEPTIONS"]),
    ("PAYMENT OPERATIONS", ["DAILY PAYMENT ACTIVITY", "PAYMENT STATUS", "SOURCE SYSTEM PERFORMANCE", "AGENT PERFORMANCE"]),
    ("SACCO PERFORMANCE", ["SACCO OUTSTANDING RANKING", "SACCO PORTFOLIO"]),
    ("LOAN INTERVENTION", ["LOANS REQUIRING ACTION"]),
    ("RECONCILIATION", ["RECONCILED VS UNRECONCILED", "RECONCILIATION EXCEPTIONS", "FUND FLOW FUNNEL"]),
    ("DAILY OPERATIONAL TRENDS", ["DAILY FINANCIAL TRENDS"]),
]


def update_dashboard() -> None:
    build_suite_dashboard(title="PDM OPERATIONS MANAGEMENT", slug="pdm-operations", active="Operations",
                          dataset_specs=DATASETS,
                          temporal={"payments": "calendar_date", "payment_daily": "reporting_date",
                                    "reconciliation": "reporting_date", "daily": "reporting_date"},
                          chart_specs=CHARTS, sections=SECTIONS,
                          filter_specs=[("Date", "payments", "calendar_date"), ("Region", "loans", "region"),
                                        ("District", "loans", "district"), ("SACCO", "loans", "sacco_name"),
                                        ("Source system", "payments", "source_system"),
                                        ("Loan status", "loans", "loan_status"),
                                        ("AI risk band", "loans", "ai_risk_band")])


if __name__ == "__main__":
    update_dashboard()
