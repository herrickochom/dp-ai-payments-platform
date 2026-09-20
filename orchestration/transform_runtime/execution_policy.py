from dataclasses import dataclass
from typing import FrozenSet


class ExecutionPolicyError(RuntimeError):
    pass


@dataclass(frozen=True)
class ExecutionUnit:
    name: str
    authority: str
    modes: FrozenSet[str]
    model_allowlist: FrozenSet[str]
    allows_runtime_graph_expansion: bool = False
    allows_plus_selector: bool = False
    allows_tag_selector: bool = False
    allows_arbitrary_dbt_args: bool = False


# Explicit production ownership identities.
#
# Model ownership is closed over the authoritative 125-model transform
# contract. Runtime graph expansion, plus selectors, tag selectors and
# arbitrary dbt arguments remain prohibited.
#
# These identities are not monolithic executable jobs. The model DAG crosses
# the ordinary/restricted authority boundary in both directions, so bounded
# topological batches must preserve these authorities without encoding unit-
# level prerequisites here.
#
# Staging and ordinary Bronze are intentionally one ownership unit because
# Staging consists of ephemeral DuckDB views required by dependent Bronze
# models in the same dbt runtime.
#
# ML-derived Bronze is isolated from Raw -> Bronze because its source is the
# governed prediction file rather than Kafka-ingested Raw.
#
# Silver Vault executes under separate restricted-identity authority.

EXECUTION_UNITS = {
    "raw_to_bronze": ExecutionUnit(
        name="raw_to_bronze",
        authority="ordinary_transform",
        modes=frozenset({
                "cdc_incremental",
                "incremental",
                "snapshot",
            }),
        model_allowlist=frozenset({
                "model.pdm_platform.br_pdm_agent_locations",
                "model.pdm_platform.br_pdm_agent_profiles",
                "model.pdm_platform.br_pdm_agent_transactions",
                "model.pdm_platform.br_pdm_cpo_plm_pain002",
                "model.pdm_platform.br_pdm_cpo_psn_pain002",
                "model.pdm_platform.br_pdm_icmn_pmn_pain001",
                "model.pdm_platform.br_pdm_icmn_vpm_pain001",
                "model.pdm_platform.br_pdm_mobile_airtel_pacs002",
                "model.pdm_platform.br_pdm_mobile_airtel_pacs008",
                "model.pdm_platform.br_pdm_mobile_mtn_pacs002",
                "model.pdm_platform.br_pdm_mobile_mtn_pacs008",
                "model.pdm_platform.br_pdm_payments_plm_lifecycle_events",
                "model.pdm_platform.br_pdm_payments_pmn_lifecycle_events",
                "model.pdm_platform.br_pdm_pdmis_beneficiaries",
                "model.pdm_platform.br_pdm_pdmis_business_plans",
                "model.pdm_platform.br_pdm_pdmis_households",
                "model.pdm_platform.br_pdm_pdmis_loans",
                "model.pdm_platform.br_pdm_pdmis_saccos",
                "model.pdm_platform.br_pdm_pdmis_special_groups",
                "model.pdm_platform.br_pdm_wendi_camt052",
                "model.pdm_platform.br_pdm_wendi_camt053",
                "model.pdm_platform.br_pdm_wendi_camt054",
                "model.pdm_platform.br_pdm_wendi_pain001",
                "model.pdm_platform.br_pdm_wendi_pain002",
                "model.pdm_platform.br_pdm_wendi_transactions",
                "model.pdm_platform.stg_pdm_agent_locations",
                "model.pdm_platform.stg_pdm_agent_profiles",
                "model.pdm_platform.stg_pdm_agent_transactions",
                "model.pdm_platform.stg_pdm_cpo_plm_pain002",
                "model.pdm_platform.stg_pdm_cpo_psn_pain002",
                "model.pdm_platform.stg_pdm_icmn_pmn_pain001",
                "model.pdm_platform.stg_pdm_icmn_vpm_pain001",
                "model.pdm_platform.stg_pdm_mobile_airtel_pacs002",
                "model.pdm_platform.stg_pdm_mobile_airtel_pacs008",
                "model.pdm_platform.stg_pdm_mobile_mtn_pacs002",
                "model.pdm_platform.stg_pdm_mobile_mtn_pacs008",
                "model.pdm_platform.stg_pdm_pdmis_beneficiaries",
                "model.pdm_platform.stg_pdm_pdmis_business_plans",
                "model.pdm_platform.stg_pdm_pdmis_households",
                "model.pdm_platform.stg_pdm_pdmis_loans",
                "model.pdm_platform.stg_pdm_pdmis_saccos",
                "model.pdm_platform.stg_pdm_pdmis_special_groups",
                "model.pdm_platform.stg_pdm_wendi_camt052",
                "model.pdm_platform.stg_pdm_wendi_camt053",
                "model.pdm_platform.stg_pdm_wendi_camt054",
                "model.pdm_platform.stg_pdm_wendi_pain001",
                "model.pdm_platform.stg_pdm_wendi_pain002",
                "model.pdm_platform.stg_pdm_wendi_transactions",
            }),
    ),
    "ordinary_analytics": ExecutionUnit(
        name="ordinary_analytics",
        authority="ordinary_transform",
        modes=frozenset({
                "cdc_incremental",
                "incremental",
                "snapshot",
            }),
        model_allowlist=frozenset({
                "model.pdm_platform.cns_pdm_agent_risk_indicators",
                "model.pdm_platform.cns_pdm_ai_default_risk",
                "model.pdm_platform.cns_pdm_beneficiary_identity_alerts",
                "model.pdm_platform.cns_pdm_beneficiary_insights",
                "model.pdm_platform.cns_pdm_channel_agent_performance",
                "model.pdm_platform.cns_pdm_daily_operational_metrics",
                "model.pdm_platform.cns_pdm_district_ai_risk",
                "model.pdm_platform.cns_pdm_district_geographic_risk",
                "model.pdm_platform.cns_pdm_district_geojson_risk",
                "model.pdm_platform.cns_pdm_duplicate_fragmentation_alerts",
                "model.pdm_platform.cns_pdm_end_to_end_traceability",
                "model.pdm_platform.cns_pdm_executive_ai_risk",
                "model.pdm_platform.cns_pdm_executive_geographic_drilldown",
                "model.pdm_platform.cns_pdm_executive_intervention_priorities",
                "model.pdm_platform.cns_pdm_executive_monthly_trend",
                "model.pdm_platform.cns_pdm_executive_overview",
                "model.pdm_platform.cns_pdm_financial_fund_flow",
                "model.pdm_platform.cns_pdm_fraud_risk_insights",
                "model.pdm_platform.cns_pdm_fund_flow_funnel",
                "model.pdm_platform.cns_pdm_geographic_alerts",
                "model.pdm_platform.cns_pdm_geographic_coverage",
                "model.pdm_platform.cns_pdm_geographic_risk_drivers",
                "model.pdm_platform.cns_pdm_geographic_risk_summary",
                "model.pdm_platform.cns_pdm_geographic_risk_trend",
                "model.pdm_platform.cns_pdm_lifecycle_exception_cases",
                "model.pdm_platform.cns_pdm_lifecycle_exceptions",
                "model.pdm_platform.cns_pdm_loan_intervention_dashboard",
                "model.pdm_platform.cns_pdm_local_government_performance",
                "model.pdm_platform.cns_pdm_parish_geographic_risk",
                "model.pdm_platform.cns_pdm_parish_performance",
                "model.pdm_platform.cns_pdm_payment_operations",
                "model.pdm_platform.cns_pdm_payment_reconciliation",
                "model.pdm_platform.cns_pdm_payments_daily_summary",
                "model.pdm_platform.cns_pdm_sacco_portfolio",
                "model.pdm_platform.cns_pdm_social_impact",
                "model.pdm_platform.cns_pdm_subcounty_geographic_risk",
                "model.pdm_platform.cns_pdm_village_geographic_risk",
                "model.pdm_platform.gld_dim_pdm_agent",
                "model.pdm_platform.gld_dim_pdm_beneficiary",
                "model.pdm_platform.gld_dim_pdm_date",
                "model.pdm_platform.gld_dim_pdm_geography",
                "model.pdm_platform.gld_dim_pdm_sacco",
                "model.pdm_platform.gld_dim_pdm_special_group",
                "model.pdm_platform.gld_fct_pdm_agent_cashouts",
                "model.pdm_platform.gld_fct_pdm_loans",
                "model.pdm_platform.gld_fct_pdm_payment_lifecycle",
                "model.pdm_platform.gld_fct_pdm_payments",
                "model.pdm_platform.slv_pdm_agent_locations",
                "model.pdm_platform.slv_pdm_agents",
                "model.pdm_platform.slv_pdm_batch",
                "model.pdm_platform.slv_pdm_beneficiaries",
                "model.pdm_platform.slv_pdm_business_plans",
                "model.pdm_platform.slv_pdm_dq_results",
                "model.pdm_platform.slv_pdm_households",
                "model.pdm_platform.slv_pdm_loan_beneficiary_links",
                "model.pdm_platform.slv_pdm_loans",
                "model.pdm_platform.slv_pdm_payment_account_controls",
                "model.pdm_platform.slv_pdm_payment_entity_matches",
                "model.pdm_platform.slv_pdm_payment_event_correlation",
                "model.pdm_platform.slv_pdm_payment_lifecycle",
                "model.pdm_platform.slv_pdm_payment_party_roles",
                "model.pdm_platform.slv_pdm_payment_technical_events",
                "model.pdm_platform.slv_pdm_payments_accounts",
                "model.pdm_platform.slv_pdm_payments_messages",
                "model.pdm_platform.slv_pdm_payments_party",
                "model.pdm_platform.slv_pdm_payments_plm_lifecycle_events",
                "model.pdm_platform.slv_pdm_payments_pmn_lifecycle_events",
                "model.pdm_platform.slv_pdm_payments_reconciliation",
                "model.pdm_platform.slv_pdm_payments_remittance",
                "model.pdm_platform.slv_pdm_payments_status_report",
                "model.pdm_platform.slv_pdm_payments_transactions",
                "model.pdm_platform.slv_pdm_saccos",
                "model.pdm_platform.slv_pdm_special_groups",
            }),
    ),
    "restricted_identity": ExecutionUnit(
        name="restricted_identity",
        authority="restricted_identity_transform",
        modes=frozenset({
                "cdc_incremental",
                "incremental",
                "snapshot",
            }),
        model_allowlist=frozenset({
                "model.pdm_platform.vlt_pdm_beneficiary_identity",
                "model.pdm_platform.vlt_pdm_beneficiary_identity_alerts",
                "model.pdm_platform.vlt_pdm_beneficiary_identity_signals",
            }),
    ),
    "ml_derived_bronze": ExecutionUnit(
        name="ml_derived_bronze",
        authority="ml_prediction_transform",
        modes=frozenset({
                "incremental",
                "snapshot",
            }),
        model_allowlist=frozenset({
                "model.pdm_platform.br_pdm_ai_default_risk",
            }),
    ),
}


def validate_execution_unit(
    name: str,
    mode: str,
) -> ExecutionUnit:

    unit = EXECUTION_UNITS.get(name)

    if unit is None:
        raise ExecutionPolicyError(
            "execution unit is not allowlisted"
        )

    if mode not in unit.modes:
        raise ExecutionPolicyError(
            "execution mode is not allowed for unit"
        )

    if not unit.model_allowlist:
        raise ExecutionPolicyError(
            "execution unit has no explicit model allowlist"
        )

    if unit.allows_runtime_graph_expansion:
        raise ExecutionPolicyError(
            "runtime graph expansion must remain disabled"
        )

    if unit.allows_plus_selector:
        raise ExecutionPolicyError(
            "plus selectors must remain disabled"
        )

    if unit.allows_tag_selector:
        raise ExecutionPolicyError(
            "tag selectors must remain disabled"
        )

    if unit.allows_arbitrary_dbt_args:
        raise ExecutionPolicyError(
            "arbitrary dbt arguments must remain disabled"
        )

    return unit
