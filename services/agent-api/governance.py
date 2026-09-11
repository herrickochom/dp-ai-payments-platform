from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from governance_models import (
    ApprovalContext,
    DataClassification,
    FieldClassification,
    GovernanceRequest,
    PolicyDecision,
    PolicyDecisionType,
)


ROLE_PERMISSIONS: dict[str, set[str]] = {
    "public_viewer": {
        "can_discover_metadata",
    },

    "parish_officer": {
        "can_discover_metadata",
        "can_run_read_queries",
        "can_generate_insights",
    },

    "district_officer": {
        "can_discover_metadata",
        "can_run_read_queries",
        "can_run_data_quality_checks",
        "can_generate_insights",
    },

    "programme_analyst": {
        "can_discover_metadata",
        "can_run_read_queries",
        "can_build_data_sources",
        "can_build_visualizations",
        "can_build_dashboards",
        "can_generate_insights",
        "can_run_data_quality_checks",
        "can_view_internal_data",
    },

    "auditor": {
        "can_discover_metadata",
        "can_run_read_queries",
        "can_run_data_quality_checks",
        "can_view_data_quality_samples",
        "can_generate_insights",
        "can_view_internal_data",
        "can_view_confidential_data",
        "can_view_governance_evidence",
    },

    "investigator": {
        "can_discover_metadata",
        "can_run_read_queries",
        "can_run_data_quality_checks",
        "can_view_data_quality_samples",
        "can_generate_insights",
        "can_view_internal_data",
        "can_view_confidential_data",
        "can_view_restricted_data",
        "can_view_beneficiary_data",
        "can_request_sensitive_access",
        "can_view_governance_evidence",
    },

    "platform_operator": {
        "can_discover_metadata",
        "can_view_internal_data",
        "can_view_governance_evidence",
    },

    "governance_administrator": {
        "can_discover_metadata",
        "can_view_internal_data",
        "can_view_confidential_data",
        "can_view_restricted_data",
        "can_view_beneficiary_data",
        "can_view_beneficiary_identity",
        "can_view_sensitive_payment_data",
        "can_manage_governance_policies",
        "can_view_governance_evidence",
        "can_approve_sensitive_access",
    },
}


RESTRICTED_SEMANTIC_TYPES = {
    "beneficiary_name",
    "national_identifier",
    "phone_number",
    "email",
    "account_identifier",
    "bank_account",
    "payment_account_identifier",
    "address",
}


SENSITIVE_PAYMENT_SEMANTIC_TYPES = {
    "account_identifier",
    "bank_account",
    "payment_account_identifier",
    "debtor_identity",
    "creditor_identity",
    "ultimate_debtor",
    "ultimate_creditor",
    "remittance_information",
}


APPROVED_PURPOSES = {
    "programme_monitoring",
    "financial_reconciliation",
    "audit",
    "investigation",
    "data_quality",
    "public_reporting",
    "system_operations",
}


@dataclass(frozen=True)
class GovernancePolicyEngine:
    fail_closed: bool = True

    def effective_permissions(
        self,
        request: GovernanceRequest,
    ) -> set[str]:
        permissions = set(request.identity.permissions)

        for role in request.identity.roles:
            permissions.update(
                ROLE_PERMISSIONS.get(role, set())
            )

        return permissions

    def evaluate(
        self,
        request: GovernanceRequest,
    ) -> PolicyDecision:
        decision_id = self._decision_id(request)

        permissions = self.effective_permissions(request)

        reasons: list[str] = []
        matched: list[str] = []
        required: list[str] = []
        masked_fields: list[str] = []

        classification = request.resource.classification

        #
        # Unknown roles fail closed.
        #
        unknown_roles = [
            role
            for role in request.identity.roles
            if role not in ROLE_PERMISSIONS
        ]

        if unknown_roles:
            return self._deny(
                request,
                decision_id,
                reasons=[
                    f"Unknown role(s): {', '.join(unknown_roles)}."
                ],
                policies=["fail_closed_unknown_role"],
            )

        #
        # Purpose validation.
        #
        if (
            request.identity.purpose is not None
            and request.identity.purpose not in APPROVED_PURPOSES
        ):
            return self._deny(
                request,
                decision_id,
                reasons=[
                    "Requested access purpose is not recognised."
                ],
                policies=["purpose_validation"],
            )

        #
        # Geographic ABAC.
        #
        geo_decision = self._evaluate_geography(request)
        if geo_decision is not None:
            return geo_decision

        #
        # Dataset-level classification.
        #
        if classification == DataClassification.PUBLIC:
            matched.append("public_resource_access")

        elif classification == DataClassification.INTERNAL:
            required.append("can_view_internal_data")

            if "can_view_internal_data" not in permissions:
                return self._deny(
                    request,
                    decision_id,
                    reasons=[
                        "Internal data requires "
                        "'can_view_internal_data'."
                    ],
                    policies=["internal_data_policy"],
                    required=required,
                )

            matched.append("internal_data_policy")

        elif classification == DataClassification.CONFIDENTIAL:
            required.append("can_view_confidential_data")

            if "can_view_confidential_data" not in permissions:
                return self._deny(
                    request,
                    decision_id,
                    reasons=[
                        "Confidential data requires "
                        "'can_view_confidential_data'."
                    ],
                    policies=["confidential_data_policy"],
                    required=required,
                )

            matched.append("confidential_data_policy")

        elif classification == DataClassification.RESTRICTED:
            required.append("can_view_restricted_data")

            if "can_view_restricted_data" not in permissions:
                return self._deny(
                    request,
                    decision_id,
                    reasons=[
                        "Restricted data requires explicit "
                        "restricted-data permission."
                    ],
                    policies=["restricted_data_policy"],
                    required=required,
                )

            matched.append("restricted_data_policy")

        else:
            return self._deny(
                request,
                decision_id,
                reasons=["Resource classification is unknown."],
                policies=["fail_closed_unknown_classification"],
            )

        #
        # Field-level governance.
        #
        for field in request.resource.field_classifications:
            field_result = self._evaluate_field(
                field=field,
                permissions=permissions,
            )

            if field_result == "DENY":
                return self._deny(
                    request,
                    decision_id,
                    reasons=[
                        f"Access to field '{field.field}' is not permitted."
                    ],
                    policies=["restricted_field_policy"],
                )

            if field_result == "MASK":
                masked_fields.append(field.field)

        #
        # Approval for sensitive beneficiary/payment access.
        #
        sensitive_access = self._contains_sensitive_fields(
            request.resource.field_classifications
        )

        if sensitive_access:
            approval = request.approval or ApprovalContext(
                status="required"
            )

            if approval.status != "approved":
                return PolicyDecision(
                    decision_id=decision_id,
                    subject=request.identity.subject_id,
                    action=request.action,
                    resource=request.resource.dataset,
                    decision=PolicyDecisionType.REQUIRE_APPROVAL,
                    reasons=[
                        "Sensitive beneficiary/payment access requires "
                        "approved access context."
                    ],
                    matched_policies=[
                        *matched,
                        "sensitive_access_approval",
                    ],
                    masked_fields=masked_fields,
                    required_permissions=required,
                    approval_required=True,
                    evidence=self._evidence(request),
                )

            matched.append("sensitive_access_approval")

        if masked_fields:
            return PolicyDecision(
                decision_id=decision_id,
                subject=request.identity.subject_id,
                action=request.action,
                resource=request.resource.dataset,
                decision=PolicyDecisionType.MASK,
                reasons=[
                    "Access is permitted with field-level masking."
                ],
                matched_policies=matched,
                masked_fields=sorted(set(masked_fields)),
                required_permissions=required,
                approval_required=False,
                evidence=self._evidence(request),
            )

        return PolicyDecision(
            decision_id=decision_id,
            subject=request.identity.subject_id,
            action=request.action,
            resource=request.resource.dataset,
            decision=PolicyDecisionType.ALLOW,
            reasons=[
                "Request satisfies configured governance policies."
            ],
            matched_policies=matched,
            masked_fields=[],
            required_permissions=required,
            approval_required=False,
            evidence=self._evidence(request),
        )

    def _evaluate_geography(
        self,
        request: GovernanceRequest,
    ) -> PolicyDecision | None:
        roles = set(request.identity.roles)

        subject_district = request.identity.district
        resource_district = request.resource.district

        if (
            "district_officer" in roles
            and subject_district
            and resource_district
            and subject_district != resource_district
        ):
            return self._deny(
                request,
                self._decision_id(request),
                reasons=[
                    "District officer access is restricted to "
                    "their assigned district."
                ],
                policies=["district_scope_policy"],
            )

        subject_parish = request.identity.parish
        resource_parish = request.resource.parish

        if (
            "parish_officer" in roles
            and subject_parish
            and resource_parish
            and subject_parish != resource_parish
        ):
            return self._deny(
                request,
                self._decision_id(request),
                reasons=[
                    "Parish officer access is restricted to "
                    "their assigned parish."
                ],
                policies=["parish_scope_policy"],
            )

        return None

    def _evaluate_field(
        self,
        field: FieldClassification,
        permissions: set[str],
    ) -> str:
        if field.classification != DataClassification.RESTRICTED:
            return "ALLOW"

        semantic_type = field.semantic_type or ""

        if semantic_type == "beneficiary_name":
            if "can_view_beneficiary_identity" in permissions:
                return "ALLOW"

            if "can_view_beneficiary_data" in permissions:
                return "MASK"

            return "DENY"

        if semantic_type in SENSITIVE_PAYMENT_SEMANTIC_TYPES:
            if "can_view_sensitive_payment_data" in permissions:
                return "ALLOW"

            if "can_view_restricted_data" in permissions:
                return "MASK"

            return "DENY"

        if semantic_type in RESTRICTED_SEMANTIC_TYPES:
            if "can_view_restricted_data" in permissions:
                return "MASK"

            return "DENY"

        return "MASK"

    @staticmethod
    def _contains_sensitive_fields(
        fields: list[FieldClassification],
    ) -> bool:
        return any(
            field.classification == DataClassification.RESTRICTED
            for field in fields
        )

    @staticmethod
    def _decision_id(
        request: GovernanceRequest,
    ) -> str:
        payload = (
            f"{request.identity.subject_id}:"
            f"{request.action}:"
            f"{request.resource.dataset}:"
            f"{','.join(sorted(request.resource.fields))}"
        )

        digest = hashlib.sha256(
            payload.encode("utf-8")
        ).hexdigest()[:16]

        return f"gov_{digest}"

    @staticmethod
    def _evidence(
        request: GovernanceRequest,
    ) -> dict:
        return {
            "roles": sorted(request.identity.roles),
            "purpose": request.identity.purpose,
            "dataset": request.resource.dataset,
            "classification": request.resource.classification.value,
            "fields": sorted(request.resource.fields),
            "district": request.resource.district,
            "parish": request.resource.parish,
        }

    @staticmethod
    def _deny(
        request: GovernanceRequest,
        decision_id: str,
        reasons: list[str],
        policies: list[str],
        required: list[str] | None = None,
    ) -> PolicyDecision:
        return PolicyDecision(
            decision_id=decision_id,
            subject=request.identity.subject_id,
            action=request.action,
            resource=request.resource.dataset,
            decision=PolicyDecisionType.DENY,
            reasons=reasons,
            matched_policies=policies,
            masked_fields=[],
            required_permissions=required or [],
            approval_required=False,
            evidence={
                "dataset": request.resource.dataset,
                "classification": (
                    request.resource.classification.value
                ),
            },
        )


#
# Deterministic masking helpers
#

def mask_phone(value: str | None) -> str | None:
    if value is None:
        return None

    value = str(value)

    if len(value) <= 4:
        return "*" * len(value)

    return ("*" * (len(value) - 4)) + value[-4:]


def mask_account(value: str | None) -> str | None:
    return mask_phone(value)


def mask_national_identifier(
    value: str | None,
) -> str | None:
    if value is None:
        return None

    value = str(value)

    if len(value) <= 3:
        return "*" * len(value)

    return ("*" * (len(value) - 3)) + value[-3:]


def mask_email(value: str | None) -> str | None:
    if value is None:
        return None

    value = str(value)

    if "@" not in value:
        return "***"

    local, domain = value.split("@", 1)

    if not local:
        return f"***@{domain}"

    return f"{local[0]}***@{domain}"


def mask_value(
    semantic_type: str | None,
    value,
):
    if value is None:
        return None

    if semantic_type == "phone_number":
        return mask_phone(str(value))

    if semantic_type in {
        "account_identifier",
        "bank_account",
        "payment_account_identifier",
    }:
        return mask_account(str(value))

    if semantic_type == "national_identifier":
        return mask_national_identifier(str(value))

    if semantic_type == "email":
        return mask_email(str(value))

    if semantic_type == "beneficiary_name":
        return "***"

    return "***"


def mask_record(
    record: dict,
    classifications: list[FieldClassification],
) -> dict:
    output = dict(record)

    by_field = {
        item.field: item
        for item in classifications
    }

    for field_name, classification in by_field.items():
        if field_name not in output:
            continue

        if classification.classification != DataClassification.RESTRICTED:
            continue

        output[field_name] = mask_value(
            classification.semantic_type,
            output[field_name],
        )

    return output
