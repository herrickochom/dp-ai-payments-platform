from dataclasses import dataclass
from types import MappingProxyType
from typing import FrozenSet, Mapping


TOKEN_LINK = (
    "source.pdm_platform.silver_vault."
    "vlt_pdm_beneficiary_token_link"
)
UGANDA_DISTRICT_GEOJSON = (
    "seed.pdm_platform.uganda_district_geojson"
)

APPROVED_AUTHORITIES = frozenset({
    "ml_prediction_transform",
    "ordinary_transform",
    "restricted_identity_transform",
})

PROTECTED_EXTERNAL_PREREQUISITES = frozenset({
    TOKEN_LINK,
    UGANDA_DISTRICT_GEOJSON,
})


class ExecutionPlanError(RuntimeError):
    pass


@dataclass(frozen=True)
class ExecutionBatch:
    batch_id: str
    authority: str
    ownership_unit: str
    contract_waves: FrozenSet[int]
    model_allowlist: FrozenSet[str]
    prerequisite_batches: FrozenSet[str]
    same_runtime_required: bool
    write_capable: bool
    post_write_tests_required: bool
    external_prerequisites: FrozenSet[str] = frozenset()
    allows_runtime_graph_expansion: bool = False
    allows_plus_selector: bool = False
    allows_tag_selector: bool = False
    allows_arbitrary_dbt_args: bool = False


EXECUTION_BATCHES: Mapping[str, ExecutionBatch] = MappingProxyType({
    "C4_ML_01": ExecutionBatch(
        batch_id="C4_ML_01",
        authority="ml_prediction_transform",
        ownership_unit="ml_derived_bronze",
        contract_waves=frozenset({1}),
        model_allowlist=frozenset({
            "model.pdm_platform.br_pdm_ai_default_risk",
        }),
        prerequisite_batches=frozenset(),
        same_runtime_required=False,
        write_capable=True,
        post_write_tests_required=True,
    ),
    "C4_RAW_02": ExecutionBatch(
        batch_id="C4_RAW_02",
        authority="ordinary_transform",
        ownership_unit="raw_to_bronze",
        contract_waves=frozenset({1, 2}),
        model_allowlist=frozenset({
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
        }),
        prerequisite_batches=frozenset(),
        same_runtime_required=True,
        write_capable=True,
        post_write_tests_required=True,
    ),
    "C4_RID_FOUNDATION_03": ExecutionBatch(
        batch_id="C4_RID_FOUNDATION_03",
        authority="restricted_identity_transform",
        ownership_unit="restricted_identity",
        contract_waves=frozenset({3}),
        model_allowlist=frozenset({
            "model.pdm_platform.vlt_pdm_beneficiary_identity",
        }),
        prerequisite_batches=frozenset({"C4_RAW_02"}),
        same_runtime_required=False,
        write_capable=True,
        post_write_tests_required=True,
        external_prerequisites=frozenset({TOKEN_LINK}),
    ),
    "C4_ORD_FOUNDATION_04": ExecutionBatch(
        batch_id="C4_ORD_FOUNDATION_04",
        authority="ordinary_transform",
        ownership_unit="ordinary_analytics",
        contract_waves=frozenset({3}),
        model_allowlist=frozenset({
            "model.pdm_platform.slv_pdm_agent_locations",
            "model.pdm_platform.slv_pdm_agents",
            "model.pdm_platform.slv_pdm_batch",
            "model.pdm_platform.slv_pdm_business_plans",
            "model.pdm_platform.slv_pdm_households",
            "model.pdm_platform.slv_pdm_loans",
            "model.pdm_platform.slv_pdm_payment_party_roles",
            "model.pdm_platform.slv_pdm_payments_accounts",
            "model.pdm_platform.slv_pdm_payments_messages",
            "model.pdm_platform.slv_pdm_payments_party",
            "model.pdm_platform.slv_pdm_payments_plm_lifecycle_events",
            "model.pdm_platform.slv_pdm_payments_pmn_lifecycle_events",
            "model.pdm_platform.slv_pdm_payments_remittance",
            "model.pdm_platform.slv_pdm_payments_status_report",
            "model.pdm_platform.slv_pdm_payments_transactions",
            "model.pdm_platform.slv_pdm_saccos",
            "model.pdm_platform.slv_pdm_special_groups",
        }),
        prerequisite_batches=frozenset({"C4_RAW_02"}),
        same_runtime_required=False,
        write_capable=True,
        post_write_tests_required=True,
    ),
    "C4_ORD_BRIDGE_05": ExecutionBatch(
        batch_id="C4_ORD_BRIDGE_05",
        authority="ordinary_transform",
        ownership_unit="ordinary_analytics",
        contract_waves=frozenset({4, 5}),
        model_allowlist=frozenset({
            "model.pdm_platform.gld_dim_pdm_agent",
            "model.pdm_platform.gld_dim_pdm_sacco",
            "model.pdm_platform.gld_dim_pdm_special_group",
            "model.pdm_platform.slv_pdm_beneficiaries",
            "model.pdm_platform.slv_pdm_loan_beneficiary_links",
            "model.pdm_platform.slv_pdm_payment_lifecycle",
            "model.pdm_platform.slv_pdm_payment_technical_events",
            "model.pdm_platform.slv_pdm_payments_reconciliation",
            "model.pdm_platform.cns_pdm_ai_default_risk",
            "model.pdm_platform.cns_pdm_village_geographic_risk",
            "model.pdm_platform.gld_dim_pdm_beneficiary",
            "model.pdm_platform.gld_dim_pdm_date",
            "model.pdm_platform.gld_dim_pdm_geography",
            "model.pdm_platform.gld_fct_pdm_loans",
            "model.pdm_platform.gld_fct_pdm_payment_lifecycle",
            "model.pdm_platform.slv_pdm_payment_account_controls",
            "model.pdm_platform.slv_pdm_payment_entity_matches",
            "model.pdm_platform.slv_pdm_payment_event_correlation",
        }),
        prerequisite_batches=frozenset({
            "C4_ML_01",
            "C4_RAW_02",
            "C4_RID_FOUNDATION_03",
            "C4_ORD_FOUNDATION_04",
        }),
        same_runtime_required=False,
        write_capable=True,
        post_write_tests_required=True,
    ),
    "C4_ORD_PRE_ALERT_06": ExecutionBatch(
        batch_id="C4_ORD_PRE_ALERT_06",
        authority="ordinary_transform",
        ownership_unit="ordinary_analytics",
        contract_waves=frozenset({6}),
        model_allowlist=frozenset({
            "model.pdm_platform.cns_pdm_district_ai_risk",
            "model.pdm_platform.cns_pdm_executive_ai_risk",
            "model.pdm_platform.cns_pdm_financial_fund_flow",
            "model.pdm_platform.cns_pdm_lifecycle_exceptions",
            "model.pdm_platform.cns_pdm_parish_performance",
            "model.pdm_platform.cns_pdm_social_impact",
            "model.pdm_platform.gld_fct_pdm_payments",
            "model.pdm_platform.slv_pdm_dq_results",
        }),
        prerequisite_batches=frozenset({
            "C4_ORD_FOUNDATION_04",
            "C4_ORD_BRIDGE_05",
        }),
        same_runtime_required=False,
        write_capable=True,
        post_write_tests_required=True,
    ),
    "C4_RID_ALERTS_07": ExecutionBatch(
        batch_id="C4_RID_ALERTS_07",
        authority="restricted_identity_transform",
        ownership_unit="restricted_identity",
        contract_waves=frozenset({6}),
        model_allowlist=frozenset({
            "model.pdm_platform.vlt_pdm_beneficiary_identity_alerts",
        }),
        prerequisite_batches=frozenset({
            "C4_RID_FOUNDATION_03",
            "C4_ORD_FOUNDATION_04",
            "C4_ORD_BRIDGE_05",
        }),
        same_runtime_required=False,
        write_capable=True,
        post_write_tests_required=True,
    ),
    "C4_ORD_PRE_SIGNAL_08": ExecutionBatch(
        batch_id="C4_ORD_PRE_SIGNAL_08",
        authority="ordinary_transform",
        ownership_unit="ordinary_analytics",
        contract_waves=frozenset({7}),
        model_allowlist=frozenset({
            "model.pdm_platform.cns_pdm_executive_monthly_trend",
            "model.pdm_platform.cns_pdm_fund_flow_funnel",
            "model.pdm_platform.cns_pdm_lifecycle_exception_cases",
            "model.pdm_platform.cns_pdm_parish_geographic_risk",
            "model.pdm_platform.cns_pdm_payment_operations",
            "model.pdm_platform.gld_fct_pdm_agent_cashouts",
        }),
        prerequisite_batches=frozenset({
            "C4_ORD_FOUNDATION_04",
            "C4_ORD_BRIDGE_05",
            "C4_ORD_PRE_ALERT_06",
        }),
        same_runtime_required=False,
        write_capable=True,
        post_write_tests_required=True,
        external_prerequisites=frozenset({
            UGANDA_DISTRICT_GEOJSON,
        }),
    ),
    "C4_RID_SIGNALS_09": ExecutionBatch(
        batch_id="C4_RID_SIGNALS_09",
        authority="restricted_identity_transform",
        ownership_unit="restricted_identity",
        contract_waves=frozenset({7}),
        model_allowlist=frozenset({
            "model.pdm_platform.vlt_pdm_beneficiary_identity_signals",
        }),
        prerequisite_batches=frozenset({"C4_RID_ALERTS_07"}),
        same_runtime_required=False,
        write_capable=True,
        post_write_tests_required=True,
    ),
    "C4_ORD_DOWNSTREAM_10": ExecutionBatch(
        batch_id="C4_ORD_DOWNSTREAM_10",
        authority="ordinary_transform",
        ownership_unit="ordinary_analytics",
        contract_waves=frozenset({8, 9, 10, 11}),
        model_allowlist=frozenset({
            "model.pdm_platform.cns_pdm_agent_risk_indicators",
            "model.pdm_platform.cns_pdm_beneficiary_identity_alerts",
            "model.pdm_platform.cns_pdm_beneficiary_insights",
            "model.pdm_platform.cns_pdm_channel_agent_performance",
            "model.pdm_platform.cns_pdm_duplicate_fragmentation_alerts",
            "model.pdm_platform.cns_pdm_executive_geographic_drilldown",
            "model.pdm_platform.cns_pdm_geographic_alerts",
            "model.pdm_platform.cns_pdm_geographic_coverage",
            "model.pdm_platform.cns_pdm_geographic_risk_drivers",
            "model.pdm_platform.cns_pdm_loan_intervention_dashboard",
            "model.pdm_platform.cns_pdm_local_government_performance",
            "model.pdm_platform.cns_pdm_payment_reconciliation",
            "model.pdm_platform.cns_pdm_payments_daily_summary",
            "model.pdm_platform.cns_pdm_sacco_portfolio",
            "model.pdm_platform.cns_pdm_daily_operational_metrics",
            "model.pdm_platform.cns_pdm_district_geographic_risk",
            "model.pdm_platform.cns_pdm_executive_overview",
            "model.pdm_platform.cns_pdm_district_geojson_risk",
            "model.pdm_platform.cns_pdm_executive_intervention_priorities",
            "model.pdm_platform.cns_pdm_fraud_risk_insights",
            "model.pdm_platform.cns_pdm_geographic_risk_trend",
            "model.pdm_platform.cns_pdm_subcounty_geographic_risk",
            "model.pdm_platform.cns_pdm_end_to_end_traceability",
            "model.pdm_platform.cns_pdm_geographic_risk_summary",
        }),
        prerequisite_batches=frozenset({
            "C4_ORD_FOUNDATION_04",
            "C4_ORD_BRIDGE_05",
            "C4_ORD_PRE_ALERT_06",
            "C4_ORD_PRE_SIGNAL_08",
            "C4_RID_SIGNALS_09",
        }),
        same_runtime_required=False,
        write_capable=True,
        post_write_tests_required=True,
        external_prerequisites=frozenset({
            UGANDA_DISTRICT_GEOJSON,
        }),
    ),
})


def validate_execution_batch(
    batch_id: str,
    *,
    authority: str,
    requested_models: FrozenSet[str],
    completed_batches: FrozenSet[str],
    runtime_graph_expansion: bool = False,
    plus_selector: bool = False,
    tag_selector: bool = False,
    arbitrary_dbt_args: bool = False,
) -> ExecutionBatch:
    batch = EXECUTION_BATCHES.get(batch_id)

    if batch is None:
        raise ExecutionPlanError("execution batch is not allowlisted")

    if authority != batch.authority:
        raise ExecutionPlanError("execution authority does not match batch")

    if requested_models != batch.model_allowlist:
        raise ExecutionPlanError(
            "requested models do not match the bounded batch allowlist"
        )

    unknown_completed = completed_batches.difference(EXECUTION_BATCHES)
    if unknown_completed:
        raise ExecutionPlanError("completed batch state is not allowlisted")

    missing = batch.prerequisite_batches.difference(completed_batches)
    if missing:
        raise ExecutionPlanError("execution batch prerequisites are incomplete")

    if runtime_graph_expansion or batch.allows_runtime_graph_expansion:
        raise ExecutionPlanError("runtime graph expansion is prohibited")

    if plus_selector or batch.allows_plus_selector:
        raise ExecutionPlanError("plus selectors are prohibited")

    if tag_selector or batch.allows_tag_selector:
        raise ExecutionPlanError("tag selectors are prohibited")

    if arbitrary_dbt_args or batch.allows_arbitrary_dbt_args:
        raise ExecutionPlanError("arbitrary dbt arguments are prohibited")

    return batch
