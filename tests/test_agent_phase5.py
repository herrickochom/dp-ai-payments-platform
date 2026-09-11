import pytest
from types import SimpleNamespace

from data_quality import DataQualityEngine
from quality_models import DQRule

from classification import (
    AmbiguousDatasetReference,
    TrustedClassificationResolver,
    UnknownDatasetClassification,
)
from governance import (
    GovernancePolicyEngine,
    mask_account,
    mask_email,
    mask_national_identifier,
    mask_phone,
    mask_record,
)
from governance_models import (
    ApprovalContext,
    DataClassification,
    FieldClassification,
    GovernanceRequest,
    IdentityContext,
    PolicyDecisionType,
    ResourceContext,
)
from phase5_agents import GovernanceAgent
from tools import (
    GovernanceApprovalRequired,
    GovernanceDenied,
    ToolRegistry,
)


# ---------------------------------------------------------------------------
# Governance request helpers
# ---------------------------------------------------------------------------


def public_request():
    return GovernanceRequest(
        identity=IdentityContext(
            subject_id="public-user",
            roles=["public_viewer"],
            purpose="public_reporting",
        ),
        action="read",
        resource=ResourceContext(
            dataset=(
                "iceberg.consumption."
                "cns_pdm_local_government_performance"
            ),
            classification=DataClassification.PUBLIC,
        ),
    )


# ---------------------------------------------------------------------------
# Phase 5 deterministic governance-policy tests
# ---------------------------------------------------------------------------


def test_public_data_allowed():
    engine = GovernancePolicyEngine()

    result = engine.evaluate(public_request())

    assert result.decision == PolicyDecisionType.ALLOW


def test_unknown_role_fails_closed():
    engine = GovernancePolicyEngine()

    request = GovernanceRequest(
        identity=IdentityContext(
            subject_id="x",
            roles=["unknown_role"],
        ),
        action="read",
        resource=ResourceContext(
            dataset="iceberg.silver.slv_pdm_loans",
            classification=DataClassification.INTERNAL,
        ),
    )

    result = engine.evaluate(request)

    assert result.decision == PolicyDecisionType.DENY


def test_programme_analyst_internal_allowed():
    engine = GovernancePolicyEngine()

    request = GovernanceRequest(
        identity=IdentityContext(
            subject_id="analyst-1",
            roles=["programme_analyst"],
            purpose="programme_monitoring",
        ),
        action="read",
        resource=ResourceContext(
            dataset="iceberg.silver.slv_pdm_loans",
            classification=DataClassification.INTERNAL,
        ),
    )

    result = engine.evaluate(request)

    assert result.decision == PolicyDecisionType.ALLOW


def test_programme_analyst_restricted_denied():
    engine = GovernancePolicyEngine()

    request = GovernanceRequest(
        identity=IdentityContext(
            subject_id="analyst-1",
            roles=["programme_analyst"],
            purpose="programme_monitoring",
        ),
        action="read",
        resource=ResourceContext(
            dataset="iceberg.silver.slv_pdm_beneficiaries",
            classification=DataClassification.RESTRICTED,
        ),
    )

    result = engine.evaluate(request)

    assert result.decision == PolicyDecisionType.DENY


def test_cross_district_access_denied():
    engine = GovernancePolicyEngine()

    request = GovernanceRequest(
        identity=IdentityContext(
            subject_id="district-user",
            roles=["district_officer"],
            district="Kabale",
            purpose="programme_monitoring",
        ),
        action="read",
        resource=ResourceContext(
            dataset="iceberg.consumption.pdm_district_data",
            classification=DataClassification.PUBLIC,
            district="Arua",
        ),
    )

    result = engine.evaluate(request)

    assert result.decision == PolicyDecisionType.DENY


def test_restricted_field_requires_approval():
    engine = GovernancePolicyEngine()

    request = GovernanceRequest(
        identity=IdentityContext(
            subject_id="investigator-1",
            roles=["investigator"],
            purpose="investigation",
        ),
        action="read",
        resource=ResourceContext(
            dataset="iceberg.silver.slv_pdm_beneficiaries",
            classification=DataClassification.RESTRICTED,
            fields=["beneficiary_name"],
            field_classifications=[
                FieldClassification(
                    dataset=(
                        "iceberg.silver."
                        "slv_pdm_beneficiaries"
                    ),
                    field="beneficiary_name",
                    classification=DataClassification.RESTRICTED,
                    semantic_type="beneficiary_name",
                )
            ],
        ),
    )

    result = engine.evaluate(request)

    assert (
        result.decision
        == PolicyDecisionType.REQUIRE_APPROVAL
    )


def test_approved_investigator_gets_masked_identity():
    engine = GovernancePolicyEngine()

    request = GovernanceRequest(
        identity=IdentityContext(
            subject_id="investigator-1",
            roles=["investigator"],
            purpose="investigation",
        ),
        action="read",
        approval=ApprovalContext(
            approval_id="approval-1",
            status="approved",
            approver="governance-admin",
        ),
        resource=ResourceContext(
            dataset="iceberg.silver.slv_pdm_beneficiaries",
            classification=DataClassification.RESTRICTED,
            fields=["beneficiary_name"],
            field_classifications=[
                FieldClassification(
                    dataset=(
                        "iceberg.silver."
                        "slv_pdm_beneficiaries"
                    ),
                    field="beneficiary_name",
                    classification=DataClassification.RESTRICTED,
                    semantic_type="beneficiary_name",
                )
            ],
        ),
    )

    result = engine.evaluate(request)

    assert result.decision == PolicyDecisionType.MASK
    assert "beneficiary_name" in result.masked_fields


def test_governance_admin_can_view_identity_after_approval():
    engine = GovernancePolicyEngine()

    request = GovernanceRequest(
        identity=IdentityContext(
            subject_id="admin",
            roles=["governance_administrator"],
            purpose="investigation",
        ),
        action="read",
        approval=ApprovalContext(
            approval_id="approval-2",
            status="approved",
            approver="governance-admin-2",
        ),
        resource=ResourceContext(
            dataset="iceberg.silver.slv_pdm_beneficiaries",
            classification=DataClassification.RESTRICTED,
            fields=["beneficiary_name"],
            field_classifications=[
                FieldClassification(
                    dataset=(
                        "iceberg.silver."
                        "slv_pdm_beneficiaries"
                    ),
                    field="beneficiary_name",
                    classification=DataClassification.RESTRICTED,
                    semantic_type="beneficiary_name",
                )
            ],
        ),
    )

    result = engine.evaluate(request)

    assert result.decision == PolicyDecisionType.ALLOW


# ---------------------------------------------------------------------------
# Phase 5 deterministic masking tests
# ---------------------------------------------------------------------------


def test_phone_masking():
    result = mask_phone("+256700123456")

    assert result.endswith("3456")
    assert "+256700123456" not in result


def test_account_masking():
    result = mask_account("123456789012")

    assert result.endswith("9012")
    assert result != "123456789012"


def test_national_identifier_masking():
    result = mask_national_identifier(
        "CM123456789"
    )

    assert result.endswith("789")
    assert result != "CM123456789"


def test_email_masking():
    assert (
        mask_email("john.smith@example.org")
        == "j***@example.org"
    )


def test_record_masking_does_not_return_original():
    record = {
        "beneficiary_name": "Jane Doe",
        "telephone": "+256700123456",
        "district": "Kabale",
    }

    classifications = [
        FieldClassification(
            dataset="example",
            field="beneficiary_name",
            classification=DataClassification.RESTRICTED,
            semantic_type="beneficiary_name",
        ),
        FieldClassification(
            dataset="example",
            field="telephone",
            classification=DataClassification.RESTRICTED,
            semantic_type="phone_number",
        ),
    ]

    result = mask_record(
        record,
        classifications,
    )

    assert result["beneficiary_name"] != "Jane Doe"
    assert result["telephone"] != "+256700123456"
    assert result["district"] == "Kabale"


def test_governance_agent_does_not_override_engine():
    agent = GovernanceAgent()

    result = agent.evaluate(public_request())

    assert (
        result["decision"].decision
        == PolicyDecisionType.ALLOW
    )


# ---------------------------------------------------------------------------
# Phase 5 query-boundary governance tests
# ---------------------------------------------------------------------------


class FakeSettings:
    max_tool_calls = 20
    max_rows = 100
    max_query_length = 10000


class FakeGateway:
    def __init__(self):
        self.executed = []

    def execute(self, sql):
        self.executed.append(sql)

        return (
            [
                "beneficiary_name",
                "phone_number",
                "district",
            ],
            [
                [
                    "Jane Doe",
                    "+256700123456",
                    "Kabale",
                ]
            ],
            "test_query_001",
        )

    def health(self):
        return {
            "status": "healthy",
            "query_id": "health_001",
        }


def test_governance_denial_prevents_trino_execution():
    gateway = FakeGateway()

    tools = ToolRegistry(
        gateway,
        FakeSettings(),
    )

    governance_request = GovernanceRequest(
        identity=IdentityContext(
            subject_id="analyst-1",
            roles=["programme_analyst"],
            purpose="programme_monitoring",
        ),
        action="read",
        resource=ResourceContext(
            dataset=(
                "iceberg.silver."
                "slv_pdm_beneficiaries"
            ),
            classification=(
                DataClassification.RESTRICTED
            ),
            fields=[
                "beneficiary_name",
            ],
        ),
    )

    with pytest.raises(
        GovernanceDenied
    ):
        tools.execute_query(
            (
                "SELECT beneficiary_name "
                "FROM iceberg.silver."
                "slv_pdm_beneficiaries"
            ),
            10,
            governance_request,
        )

    assert gateway.executed == []


def test_caller_cannot_spoof_restricted_dataset_as_public():
    gateway = FakeGateway()

    tools = ToolRegistry(
        gateway,
        FakeSettings(),
    )

    governance_request = GovernanceRequest(
        identity=IdentityContext(
            subject_id="analyst-1",
            roles=["programme_analyst"],
            purpose="programme_monitoring",
        ),
        action="read",
        resource=ResourceContext(
            dataset=(
                "iceberg.silver."
                "slv_pdm_beneficiaries"
            ),
            classification=DataClassification.PUBLIC,
            fields=[
                "beneficiary_name",
            ],
            field_classifications=[
                FieldClassification(
                    dataset=(
                        "iceberg.silver."
                        "slv_pdm_beneficiaries"
                    ),
                    field="beneficiary_name",
                    classification=DataClassification.PUBLIC,
                    semantic_type=None,
                )
            ],
        ),
    )

    assert (
        governance_request.resource.classification
        == DataClassification.PUBLIC
    )

    with pytest.raises(
        GovernanceDenied
    ):
        tools.execute_query(
            (
                "SELECT beneficiary_name "
                "FROM iceberg.silver."
                "slv_pdm_beneficiaries"
            ),
            10,
            governance_request,
        )

    assert gateway.executed == []


def test_governance_approval_requirement_prevents_execution():
    gateway = FakeGateway()

    tools = ToolRegistry(
        gateway,
        FakeSettings(),
    )

    governance_request = GovernanceRequest(
        identity=IdentityContext(
            subject_id="investigator-1",
            roles=["investigator"],
            purpose="investigation",
        ),
        action="read",
        resource=ResourceContext(
            dataset=(
                "iceberg.silver."
                "slv_pdm_beneficiaries"
            ),
            classification=(
                DataClassification.RESTRICTED
            ),
            fields=[
                "beneficiary_name",
            ],
            field_classifications=[
                FieldClassification(
                    dataset=(
                        "iceberg.silver."
                        "slv_pdm_beneficiaries"
                    ),
                    field="beneficiary_name",
                    classification=(
                        DataClassification.RESTRICTED
                    ),
                    semantic_type=(
                        "beneficiary_name"
                    ),
                )
            ],
        ),
    )

    with pytest.raises(
        GovernanceApprovalRequired
    ):
        tools.execute_query(
            (
                "SELECT beneficiary_name "
                "FROM iceberg.silver."
                "slv_pdm_beneficiaries"
            ),
            10,
            governance_request,
        )

    assert gateway.executed == []


def test_governance_masks_result_before_return():
    gateway = FakeGateway()

    tools = ToolRegistry(
        gateway,
        FakeSettings(),
    )

    governance_request = GovernanceRequest(
        identity=IdentityContext(
            subject_id="investigator-1",
            roles=["investigator"],
            purpose="investigation",
        ),
        action="read",
        approval=ApprovalContext(
            approval_id="approval-test",
            status="approved",
            approver="governance-admin",
        ),
        resource=ResourceContext(
            dataset=(
                "iceberg.silver."
                "slv_pdm_beneficiaries"
            ),
            classification=(
                DataClassification.RESTRICTED
            ),
            fields=[
                "beneficiary_name",
            ],
            field_classifications=[
                FieldClassification(
                    dataset=(
                        "iceberg.silver."
                        "slv_pdm_beneficiaries"
                    ),
                    field="beneficiary_name",
                    classification=(
                        DataClassification.RESTRICTED
                    ),
                    semantic_type=(
                        "beneficiary_name"
                    ),
                )
            ],
        ),
    )

    result = tools.execute_query(
        (
            "SELECT beneficiary_name, "
            "phone_number, district "
            "FROM iceberg.silver."
            "slv_pdm_beneficiaries"
        ),
        10,
        governance_request,
    )

    assert result["query_id"] == "test_query_001"

    assert (
        result["governance"]["decision"]
        == "MASK"
    )

    assert (
        "beneficiary_name"
        in result["governance"]["masked_fields"]
    )

    assert result["rows"][0][0] == "***"
    assert result["rows"][0][0] != "Jane Doe"

    assert result["rows"][0][2] == "Kabale"


def test_governance_allow_preserves_query_result():
    gateway = FakeGateway()

    tools = ToolRegistry(
        gateway,
        FakeSettings(),
    )

    governance_request = GovernanceRequest(
        identity=IdentityContext(
            subject_id="public-user",
            roles=["public_viewer"],
            purpose="public_reporting",
        ),
        action="read",
        resource=ResourceContext(
            dataset=(
                "iceberg.consumption."
                "cns_pdm_local_government_performance"
            ),
            classification=DataClassification.PUBLIC,
            fields=["district"],
        ),
    )

    result = tools.execute_query(
        (
            "SELECT district "
            "FROM iceberg.consumption."
            "cns_pdm_local_government_performance"
        ),
        10,
        governance_request,
    )

    assert result["query_id"] == "test_query_001"

    assert (
        result["governance"]["decision"]
        == "ALLOW"
    )

    assert result["rows"][0][2] == "Kabale"


def test_query_without_governance_remains_backward_compatible():
    gateway = FakeGateway()

    tools = ToolRegistry(
        gateway,
        FakeSettings(),
    )

    result = tools.execute_query(
        "SELECT district FROM example",
        10,
    )

    assert result["query_id"] == "test_query_001"
    assert "governance" not in result
    assert len(gateway.executed) == 1


# ---------------------------------------------------------------------------
# Phase 5 trusted-classification tests
# ---------------------------------------------------------------------------


def test_trusted_classifier_marks_beneficiaries_restricted():
    resolver = TrustedClassificationResolver()

    result = resolver.resolve_resource(
        "iceberg.silver.slv_pdm_beneficiaries",
        [
            "beneficiary_name",
            "district",
        ],
    )

    assert (
        result.classification
        == DataClassification.RESTRICTED
    )

    classifications = {
        item.field: item
        for item in result.field_classifications
    }

    assert (
        classifications[
            "beneficiary_name"
        ].classification
        == DataClassification.RESTRICTED
    )

    assert (
        classifications[
            "beneficiary_name"
        ].semantic_type
        == "beneficiary_name"
    )


def test_trusted_classifier_marks_public_aggregate_public():
    resolver = TrustedClassificationResolver()

    result = resolver.resolve_resource(
        (
            "iceberg.consumption."
            "cns_pdm_local_government_performance"
        ),
        [
            "district",
            "principal_repayment_rate",
        ],
    )

    assert (
        result.classification
        == DataClassification.PUBLIC
    )


def test_unknown_dataset_classification_fails_closed():
    resolver = TrustedClassificationResolver()

    with pytest.raises(
        UnknownDatasetClassification
    ):
        resolver.resolve_resource(
            "iceberg.silver.some_unknown_table",
            ["district"],
        )


def test_sensitive_phone_field_is_restricted():
    resolver = TrustedClassificationResolver()

    result = resolver.resolve_resource(
        "iceberg.silver.slv_pdm_beneficiaries",
        [
            "phone_number",
        ],
    )

    field = result.field_classifications[0]

    assert (
        field.classification
        == DataClassification.RESTRICTED
    )

    assert (
        field.semantic_type
        == "phone_number"
    )


def test_sql_resolver_extracts_physical_dataset():
    resolver = TrustedClassificationResolver()

    result = resolver.resolve_sql(
        (
            "SELECT district, "
            "AVG(principal_repayment_rate) "
            "AS repayment_performance "
            "FROM "
            "iceberg.consumption."
            "cns_pdm_local_government_performance "
            "GROUP BY district"
        )
    )

    assert (
        result.dataset
        == (
            "iceberg.consumption."
            "cns_pdm_local_government_performance"
        )
    )

    assert (
        result.classification
        == DataClassification.PUBLIC
    )

    assert "district" in result.fields

    assert (
        "principal_repayment_rate"
        in result.fields
    )


def test_sql_resolver_detects_beneficiary_identity():
    resolver = TrustedClassificationResolver()

    result = resolver.resolve_sql(
        (
            "SELECT beneficiary_name, district "
            "FROM iceberg.silver."
            "slv_pdm_beneficiaries"
        )
    )

    assert (
        result.classification
        == DataClassification.RESTRICTED
    )

    field_map = {
        item.field: item
        for item in result.field_classifications
    }

    assert (
        field_map[
            "beneficiary_name"
        ].semantic_type
        == "beneficiary_name"
    )


def test_sql_resolver_handles_cte_without_classifying_cte_alias():
    resolver = TrustedClassificationResolver()

    result = resolver.resolve_sql(
        (
            "WITH loans AS ("
            "SELECT district, repayment_rate "
            "FROM iceberg.silver.slv_pdm_loans"
            ") "
            "SELECT district, repayment_rate "
            "FROM loans"
        )
    )

    assert (
        result.dataset
        == "iceberg.silver.slv_pdm_loans"
    )

    assert (
        result.classification
        == DataClassification.INTERNAL
    )


def test_sql_resolver_rejects_multi_dataset_query():
    resolver = TrustedClassificationResolver()

    with pytest.raises(
        AmbiguousDatasetReference
    ):
        resolver.resolve_sql(
            (
                "SELECT a.district "
                "FROM iceberg.silver.slv_pdm_loans a "
                "JOIN iceberg.silver."
                "slv_pdm_beneficiaries b "
                "ON a.district = b.district"
            )
        )
def test_public_dimension_remains_public_on_restricted_dataset():
    resolver = TrustedClassificationResolver()

    result = resolver.resolve_resource(
        "iceberg.silver.slv_pdm_beneficiaries",
        [
            "beneficiary_name",
            "district",
        ],
    )

    field_map = {
        item.field: item
        for item in result.field_classifications
    }

    assert (
        field_map["beneficiary_name"].classification
        == DataClassification.RESTRICTED
    )

    assert (
        field_map["district"].classification
        == DataClassification.PUBLIC
    )


# ---------------------------------------------------------------------------
# Phase 5 DQ sample-leakage boundary tests
# ---------------------------------------------------------------------------


class FakeDQSettings:
    dq_max_sample_rows = 5


class FakeDQTools:
    def __init__(self):
        self.settings = FakeDQSettings()
        self.executed = []

    def describe_table(self, catalog, schema, table):
        return [
            {"column_name": "beneficiary_name"},
            {"column_name": "district"},
        ]

    def execute_query(self, sql, max_rows):
        self.executed.append((sql, max_rows))

        if "HAVING COUNT(*) > 1" in sql:
            return {
                "columns": [
                    "beneficiary_name",
                    "duplicate_count",
                ],
                "rows": [
                    ["Jane Doe", 2],
                    ["John Smith", 2],
                    ["Alice Example", 2],
                    ["Bob Example", 2],
                    ["Carol Example", 2],
                    ["Extra Person", 2],
                ],
                "query_id": "dq_sample_001",
            }

        return {
            "columns": [
                "checked_rows",
                "failed_rows",
            ],
            "rows": [
                [10, 2],
            ],
            "query_id": "dq_aggregate_001",
        }


def _restricted_beneficiary_unique_rule():
    return DQRule(
        rule_id=(
            "phase5.iceberg.silver."
            "slv_pdm_beneficiaries."
            "beneficiary_name.unique"
        ),
        dataset=(
            "iceberg.silver."
            "slv_pdm_beneficiaries"
        ),
        rule_type="unique",
        field="beneficiary_name",
        severity="high",
        parameters={},
        source="phase4_config",
        provenance="phase5-regression-test",
    )


def test_dq_samples_are_withheld_without_sample_permission():
    tools = FakeDQTools()
    engine = DataQualityEngine(tools)

    permissions = SimpleNamespace(
        can_run_data_quality_checks=True,
        can_execute_read_queries=True,
        can_view_data_quality_samples=False,
        max_rows=100,
    )

    finding = engine.run_rule(
        _restricted_beneficiary_unique_rule(),
        permissions,
    )

    assert finding.status == "failed"
    assert finding.sample == []

    assert len(tools.executed) == 1

    assert (
        "Raw failure samples were withheld "
        "by default permission policy"
        in finding.warnings
    )


def test_dq_sample_permission_returns_only_masked_bounded_values():
    tools = FakeDQTools()
    engine = DataQualityEngine(tools)

    permissions = SimpleNamespace(
        can_run_data_quality_checks=True,
        can_execute_read_queries=True,
        can_view_data_quality_samples=True,
        max_rows=100,
    )

    finding = engine.run_rule(
        _restricted_beneficiary_unique_rule(),
        permissions,
    )

    assert finding.status == "failed"

    assert len(tools.executed) == 2

    sample_sql, requested_limit = tools.executed[1]

    assert "HAVING COUNT(*) > 1" in sample_sql
    assert requested_limit == 5

    assert len(finding.sample) == 5

    returned_names = [
        row["beneficiary_name"]
        for row in finding.sample
    ]

    assert "Jane Doe" not in returned_names
    assert "John Smith" not in returned_names

    assert all(
        value.startswith("masked:")
        for value in returned_names
    )

    assert all(
        len(value) == len("masked:") + 12
        for value in returned_names
    )

    assert all(
        row["duplicate_count"] == 2
        for row in finding.sample
    )


def test_unknown_governed_dataset_fails_before_trino_execution():
    gateway = FakeGateway()

    tools = ToolRegistry(
        gateway,
        FakeSettings(),
    )

    governance_request = GovernanceRequest(
        identity=IdentityContext(
            subject_id="analyst-1",
            roles=["programme_analyst"],
            purpose="programme_monitoring",
        ),
        action="read",
        resource=ResourceContext(
            dataset="iceberg.silver.some_unknown_table",
            classification=DataClassification.PUBLIC,
            fields=["district"],
        ),
    )

    with pytest.raises(GovernanceDenied):
        tools.execute_query(
            (
                "SELECT district "
                "FROM iceberg.silver.some_unknown_table"
            ),
            10,
            governance_request,
        )

    assert gateway.executed == []



# ---------------------------------------------------------------------------
# Phase 5 BI publication governance tests
# ---------------------------------------------------------------------------

from bi_adapter import AdapterPermissionDenied, SupersetAdapter


class NetworkTrapSession:
    def __getattr__(self, name):
        raise AssertionError(
            f"Superset network call attempted before governance rejection: {name}"
        )


def _dashboard_for(dataset, fields):
    source = SimpleNamespace(
        id="source_1",
        dataset=dataset,
    )

    visual = SimpleNamespace(
        data_source=source,
        encoding=[
            SimpleNamespace(field=field)
            for field in fields
        ],
    )

    return SimpleNamespace(
        visualizations=[visual],
        filters=[],
    )


def test_restricted_dataset_publish_is_denied_before_superset_network():
    adapter = SupersetAdapter(
        settings=SimpleNamespace(
            superset_password="not-used",
        ),
        session=NetworkTrapSession(),
    )

    dashboard = _dashboard_for(
        "iceberg.silver.slv_pdm_beneficiaries",
        ["district"],
    )

    permissions = SimpleNamespace(
        can_publish_bi_assets=True,
    )

    with pytest.raises(
        AdapterPermissionDenied,
        match="RESTRICTED dataset",
    ):
        adapter.publish(
            dashboard,
            permissions,
        )


def test_public_aggregate_passes_bi_governance_validation():
    adapter = SupersetAdapter(
        settings=SimpleNamespace(
            superset_password="not-used",
        ),
        session=NetworkTrapSession(),
    )

    dashboard = _dashboard_for(
        (
            "iceberg.consumption."
            "cns_pdm_local_government_performance"
        ),
        [
            "district",
            "principal_repayment_rate",
        ],
    )

    adapter._validate_publication_governance(
        dashboard
    )

