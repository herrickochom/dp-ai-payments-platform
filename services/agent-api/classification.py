from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import sqlglot
from sqlglot import exp

from governance_models import (
    DataClassification,
    FieldClassification,
    ResourceContext,
)


class ClassificationError(RuntimeError):
    """
    Base error for trusted classification resolution.

    Classification failures are security-relevant. Callers must not silently
    downgrade or bypass these errors.
    """

    pass


class UnknownDatasetClassification(ClassificationError):
    """
    Raised when a dataset has no trusted platform classification.
    """

    pass


class AmbiguousDatasetReference(ClassificationError):
    """
    Raised when a query references more than one governed dataset and the
    current bounded governance model cannot safely reduce them to one
    ResourceContext.
    """

    pass


@dataclass(frozen=True)
class DatasetPolicy:
    """
    Platform-owned classification metadata for one dataset.

    This metadata is trusted configuration. It must never be populated from
    an API caller's GovernanceRequest.resource.classification.
    """

    dataset: str
    classification: DataClassification


#
# ---------------------------------------------------------------------------
# Trusted dataset registry
# ---------------------------------------------------------------------------
#
# This registry belongs to the platform.
#
# It is intentionally explicit. Unknown datasets do not inherit a caller-
# supplied classification and do not silently become PUBLIC.
#
# Add new datasets here, or later load the same model from governed metadata
# maintained in Git/catalogue configuration.
#

DATASET_POLICIES: dict[str, DatasetPolicy] = {
    #
    # Consumption layer
    #
    # Aggregated local-government performance is intended for broad reporting
    # and contains no beneficiary-level identity in the current platform
    # design.
    #
    "iceberg.consumption.cns_pdm_local_government_performance": (
        DatasetPolicy(
            dataset=(
                "iceberg.consumption."
                "cns_pdm_local_government_performance"
            ),
            classification=DataClassification.PUBLIC,
        )
    ),

    #
    # District-level aggregate data.
    #
    "iceberg.consumption.pdm_district_data": (
        DatasetPolicy(
            dataset=(
                "iceberg.consumption."
                "pdm_district_data"
            ),
            classification=DataClassification.PUBLIC,
        )
    ),

    #
    # Silver layer
    #
    # Loan records are operational/internal data.
    #
    "iceberg.silver.slv_pdm_loans": DatasetPolicy(
        dataset="iceberg.silver.slv_pdm_loans",
        classification=DataClassification.INTERNAL,
    ),

    #
    # Beneficiary-level records contain identity information and are
    # therefore restricted.
    #
    "iceberg.silver.slv_pdm_beneficiaries": DatasetPolicy(
        dataset="iceberg.silver.slv_pdm_beneficiaries",
        classification=DataClassification.RESTRICTED,
    ),

    #
    # DQ result metadata does not itself represent beneficiary identity.
    #
    "iceberg.silver.slv_pdm_dq_results": DatasetPolicy(
        dataset="iceberg.silver.slv_pdm_dq_results",
        classification=DataClassification.INTERNAL,
    ),
}


#
# ---------------------------------------------------------------------------
# Trusted field semantic registry
# ---------------------------------------------------------------------------
#
# Explicit definitions take precedence over deterministic name-based
# classification below.
#

FIELD_POLICIES: dict[
    tuple[str, str],
    tuple[DataClassification, str],
] = {
    (
        "iceberg.silver.slv_pdm_beneficiaries",
        "beneficiary_name",
    ): (
        DataClassification.RESTRICTED,
        "beneficiary_name",
    ),
}


#
# Exact field names and recognised aliases.
#
# These are deterministic platform rules, not LLM inference.
#

BENEFICIARY_NAME_FIELDS = {
    "beneficiary_name",
    "beneficiary_full_name",
    "beneficiary",
    "full_name",
}

NATIONAL_IDENTIFIER_FIELDS = {
    "nin",
    "national_id",
    "national_identifier",
    "national_identification_number",
    "national_id_number",
}

PHONE_FIELDS = {
    "phone",
    "phone_number",
    "telephone",
    "telephone_number",
    "mobile",
    "mobile_number",
    "msisdn",
}

EMAIL_FIELDS = {
    "email",
    "email_address",
}

ACCOUNT_FIELDS = {
    "account",
    "account_id",
    "account_number",
    "bank_account",
    "bank_account_number",
    "debtor_account",
    "creditor_account",
    "iban",
}

PAYMENT_IDENTIFIER_FIELDS = {
    "payment_id",
    "payment_identifier",
    "transaction_id",
    "transaction_identifier",
    "end_to_end_id",
    "endtoend_id",
    "instruction_id",
    "message_id",
}

ADDRESS_FIELDS = {
    "address",
    "postal_address",
    "residential_address",
    "beneficiary_address",
}

DEBTOR_IDENTITY_FIELDS = {
    "debtor_name",
    "debtor",
}

CREDITOR_IDENTITY_FIELDS = {
    "creditor_name",
    "creditor",
}


#
# Public/aggregate fields which are useful to explicitly recognise.
#
AGGREGATE_PUBLIC_FIELDS = {
    "district",
    "region",
    "parish",
    "status",
    "repayment_rate",
    "principal_repayment_rate",
    "repayment_performance",
    "total",
    "average",
    "count",
}


def normalize_identifier(value: str) -> str:
    """
    Normalise a SQL/catalogue identifier for deterministic comparison.
    """

    return (
        value
        .strip()
        .strip('"')
        .strip("`")
        .lower()
    )


def normalize_dataset(value: str) -> str:
    """
    Normalise a fully-qualified dataset reference.

    Expected canonical shape is:

        catalog.schema.table

    The function intentionally does not invent a missing catalog/schema.
    """

    parts = [
        normalize_identifier(part)
        for part in value.split(".")
        if part.strip()
    ]

    return ".".join(parts)


def _semantic_classification(
    field: str,
) -> tuple[DataClassification, str] | None:
    """
    Resolve recognised sensitive semantic fields deterministically.
    """

    name = normalize_identifier(field)

    if name in BENEFICIARY_NAME_FIELDS:
        return (
            DataClassification.RESTRICTED,
            "beneficiary_name",
        )

    if name in NATIONAL_IDENTIFIER_FIELDS:
        return (
            DataClassification.RESTRICTED,
            "national_identifier",
        )

    if name in PHONE_FIELDS:
        return (
            DataClassification.RESTRICTED,
            "phone_number",
        )

    if name in EMAIL_FIELDS:
        return (
            DataClassification.RESTRICTED,
            "email_address",
        )

    if name in ACCOUNT_FIELDS:
        return (
            DataClassification.RESTRICTED,
            "account_identifier",
        )

    if name in PAYMENT_IDENTIFIER_FIELDS:
        return (
            DataClassification.RESTRICTED,
            "payment_identifier",
        )

    if name in ADDRESS_FIELDS:
        return (
            DataClassification.RESTRICTED,
            "address",
        )

    if name in DEBTOR_IDENTITY_FIELDS:
        return (
            DataClassification.RESTRICTED,
            "debtor_identity",
        )

    if name in CREDITOR_IDENTITY_FIELDS:
        return (
            DataClassification.RESTRICTED,
            "creditor_identity",
        )

    return None


class TrustedClassificationResolver:
    """
    Deterministic, platform-owned data-classification resolver.

    Security boundary:

        caller identity/purpose
                 |
                 v
        GovernanceRequest
                 |
                 | resource classification is NOT trusted
                 v
        TrustedClassificationResolver
                 |
                 v
        authoritative ResourceContext
                 |
                 v
        GovernancePolicyEngine

    The resolver never uses caller-provided dataset or field classifications
    as the source of truth.
    """

    def __init__(
        self,
        dataset_policies: dict[
            str,
            DatasetPolicy,
        ] | None = None,
        field_policies: dict[
            tuple[str, str],
            tuple[DataClassification, str],
        ] | None = None,
    ):
        supplied_datasets = (
            dataset_policies
            if dataset_policies is not None
            else DATASET_POLICIES
        )

        supplied_fields = (
            field_policies
            if field_policies is not None
            else FIELD_POLICIES
        )

        self.dataset_policies = {
            normalize_dataset(name): DatasetPolicy(
                dataset=normalize_dataset(
                    policy.dataset
                ),
                classification=(
                    policy.classification
                ),
            )
            for name, policy
            in supplied_datasets.items()
        }

        self.field_policies = {
            (
                normalize_dataset(dataset),
                normalize_identifier(field),
            ): (
                classification,
                semantic_type,
            )
            for (
                dataset,
                field,
            ), (
                classification,
                semantic_type,
            )
            in supplied_fields.items()
        }

    # ------------------------------------------------------------------
    # Dataset classification
    # ------------------------------------------------------------------

    def classify_dataset(
        self,
        dataset: str,
    ) -> DataClassification:
        """
        Resolve a dataset using trusted platform configuration.

        Unknown datasets fail closed.
        """

        canonical = normalize_dataset(
            dataset
        )

        policy = self.dataset_policies.get(
            canonical
        )

        if policy is None:
            raise UnknownDatasetClassification(
                "No trusted classification exists for dataset "
                f"{canonical!r}"
            )

        return policy.classification

    # ------------------------------------------------------------------
    # Field classification
    # ------------------------------------------------------------------

    def classify_field(
        self,
        dataset: str,
        field: str,
    ) -> FieldClassification:
        """
        Produce authoritative classification metadata for one field.

        Precedence:

        1. exact platform field policy
        2. deterministic sensitive semantic-type mapping
        3. explicitly recognised safe reporting field
        4. inherit the trusted dataset classification
        """

        canonical_dataset = normalize_dataset(
            dataset
        )

        canonical_field = normalize_identifier(
            field
        )

        dataset_classification = (
            self.classify_dataset(
                canonical_dataset
            )
        )

        explicit = self.field_policies.get(
            (
                canonical_dataset,
                canonical_field,
            )
        )

        if explicit is not None:
            (
                classification,
                semantic_type,
            ) = explicit

            return FieldClassification(
                dataset=canonical_dataset,
                field=canonical_field,
                classification=classification,
                semantic_type=semantic_type,
            )

        semantic = _semantic_classification(
            canonical_field
        )

        if semantic is not None:
            (
                classification,
                semantic_type,
            ) = semantic

            return FieldClassification(
                dataset=canonical_dataset,
                field=canonical_field,
                classification=classification,
                semantic_type=semantic_type,
            )

        #
        # Explicitly recognised reporting dimensions and aggregate measures
        # remain public at field level even when selected from a restricted
        # row-level dataset.
        #
        # This allows harmless values such as district or repayment rate to
        # remain visible while sensitive identity fields from the same dataset
        # continue to be governed as RESTRICTED.
        #
        if canonical_field in AGGREGATE_PUBLIC_FIELDS:
            return FieldClassification(
                dataset=canonical_dataset,
                field=canonical_field,
                classification=DataClassification.PUBLIC,
                semantic_type=None,
            )

        #
        # Unknown fields inherit the trusted dataset classification.
        #
        # This preserves fail-closed behaviour for any field that has not been
        # explicitly recognised as sensitive or explicitly approved as a safe
        # reporting field.
        #
        return FieldClassification(
            dataset=canonical_dataset,
            field=canonical_field,
            classification=dataset_classification,
            semantic_type=None,
        )

    # ------------------------------------------------------------------
    # Resource construction
    # ------------------------------------------------------------------

    def resolve_resource(
        self,
        dataset: str,
        fields: Iterable[str] | None = None,
        *,
        district: str | None = None,
        parish: str | None = None,
    ) -> ResourceContext:
        """
        Construct an authoritative ResourceContext.

        Caller-supplied classification data is intentionally not accepted.
        """

        canonical_dataset = normalize_dataset(
            dataset
        )

        dataset_classification = (
            self.classify_dataset(
                canonical_dataset
            )
        )

        canonical_fields = []

        for field in fields or []:
            normalized = normalize_identifier(
                field
            )

            if (
                normalized
                and normalized
                not in canonical_fields
            ):
                canonical_fields.append(
                    normalized
                )

        field_classifications = [
            self.classify_field(
                canonical_dataset,
                field,
            )
            for field in canonical_fields
        ]

        return ResourceContext(
            dataset=canonical_dataset,
            classification=(
                dataset_classification
            ),
            fields=canonical_fields,
            field_classifications=(
                field_classifications
            ),
            district=district,
            parish=parish,
        )

    # ------------------------------------------------------------------
    # SQL extraction
    # ------------------------------------------------------------------

    @staticmethod
    def datasets_from_sql(
        sql: str,
    ) -> list[str]:
        """
        Extract physical dataset references from a read query.

        CTE aliases are excluded so that:

            WITH x AS (
                SELECT * FROM iceberg.silver.slv_pdm_loans
            )
            SELECT * FROM x

        resolves the physical source table rather than treating `x` as a
        governed dataset.
        """

        try:
            tree = sqlglot.parse_one(
                sql,
                read="trino",
            )

        except sqlglot.errors.ParseError as exc:
            raise ClassificationError(
                f"Unable to parse SQL for classification: {exc}"
            ) from exc

        cte_names = {
            normalize_identifier(
                cte.alias_or_name
            )
            for cte in tree.find_all(
                exp.CTE
            )
            if cte.alias_or_name
        }

        datasets: list[str] = []

        for table in tree.find_all(
            exp.Table
        ):
            table_name = normalize_identifier(
                table.name
            )

            catalog = normalize_identifier(
                table.catalog
            ) if table.catalog else ""

            schema = normalize_identifier(
                table.db
            ) if table.db else ""

            #
            # A simple CTE reference such as FROM x is not a physical
            # platform dataset.
            #
            if (
                not catalog
                and not schema
                and table_name in cte_names
            ):
                continue

            parts = [
                part
                for part in (
                    catalog,
                    schema,
                    table_name,
                )
                if part
            ]

            dataset = ".".join(
                parts
            )

            if (
                dataset
                and dataset not in datasets
            ):
                datasets.append(
                    dataset
                )

        return datasets

    @staticmethod
    def projected_fields_from_sql(
        sql: str,
    ) -> list[str]:
        """
        Extract directly projected column names where deterministically
        available.

        Expressions are handled conservatively:

            SELECT beneficiary_name       -> beneficiary_name
            SELECT district               -> district
            SELECT AVG(repayment_rate)     -> repayment_rate
            SELECT COUNT(*)                -> no physical field
            SELECT *                       -> no field-level assertion

        The trusted dataset classification still applies even where a
        projected field cannot be resolved.
        """

        try:
            tree = sqlglot.parse_one(
                sql,
                read="trino",
            )

        except sqlglot.errors.ParseError as exc:
            raise ClassificationError(
                f"Unable to parse SQL for field classification: {exc}"
            ) from exc

        fields: list[str] = []

        for select in tree.find_all(
            exp.Select
        ):
            for expression in select.expressions:
                if isinstance(
                    expression,
                    exp.Star,
                ):
                    continue

                for column in expression.find_all(
                    exp.Column
                ):
                    name = normalize_identifier(
                        column.name
                    )

                    if (
                        name
                        and name not in fields
                    ):
                        fields.append(
                            name
                        )

        return fields

    def resolve_sql(
        self,
        sql: str,
        *,
        district: str | None = None,
        parish: str | None = None,
    ) -> ResourceContext:
        """
        Resolve SQL into one authoritative governed ResourceContext.

        The current Phase 5 policy model evaluates one logical governed
        resource at a time. A query touching multiple physical datasets is
        therefore rejected rather than incorrectly classifying a join using
        only one table's policy.
        """

        datasets = self.datasets_from_sql(
            sql
        )

        if not datasets:
            raise UnknownDatasetClassification(
                "Query does not reference a trusted governed dataset"
            )

        if len(datasets) > 1:
            raise AmbiguousDatasetReference(
                "Query references multiple governed datasets; "
                "multi-resource policy evaluation is not yet supported: "
                + ", ".join(datasets)
            )

        dataset = datasets[0]

        fields = (
            self.projected_fields_from_sql(
                sql
            )
        )

        return self.resolve_resource(
            dataset,
            fields,
            district=district,
            parish=parish,
        )
