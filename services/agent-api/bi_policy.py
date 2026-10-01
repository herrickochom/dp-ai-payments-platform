"""Server-owned BI publication policy (F2/F3).

This module is the single authority for *which* platform datasets automated BI
publication may reference. It is server-owned configuration in code:

* it is never built from a request body, an HTTP header, or a token claim;
* a caller cannot add a dataset to it;
* absence of an entry means DENY, so unknown, restricted, identity-bearing and
  internal-but-not-BI-approved datasets are all refused.

Provenance of the allowlist
---------------------------
Derived from repository evidence only:

* ``platform/superset/imports/pdm_executive_datasources.yaml`` - the curated BI
  manifest, which is the platform's existing statement of what BI consumes;
* minus every dataset that GATE-2 evidence
  (``docs/architecture/gate2-data-protection-stop-report.md``) identifies as
  carrying row-level beneficiary identity.

No dataset was invented. Datasets absent from the curated manifest were never
eligible.
"""

from __future__ import annotations

from dataclasses import dataclass


BI_CATALOG = "iceberg"

BI_SCHEMA = "consumption"


#
# Identity-bearing consumption datasets identified by GATE-2 evidence.
#
# They remain valid platform data products and are NOT deleted, but they are
# not automatically BI-authorised. BI access is denied by default.
#
IDENTITY_BEARING_DATASETS: frozenset[str] = frozenset(
    {
        "cns_pdm_beneficiary_insights",
        "cns_pdm_loan_intervention_dashboard",
        "cns_pdm_payment_operations",
        "cns_pdm_end_to_end_traceability",
        "cns_pdm_ai_default_risk",
        "cns_pdm_beneficiary_identity_alerts",
    }
)


#
# The approved BI interface: the curated manifest minus the identity-bearing
# datasets. These are the only platform tables automated BI publication may
# register as a Superset dataset.
#
APPROVED_BI_TABLES: frozenset[str] = frozenset(
    {
        "cns_pdm_district_geographic_risk",
        "cns_pdm_executive_monthly_trend",
        "cns_pdm_executive_overview",
        "cns_pdm_financial_fund_flow",
        "cns_pdm_fraud_risk_insights",
        "cns_pdm_fund_flow_funnel",
        "cns_pdm_geographic_alerts",
        "cns_pdm_geographic_coverage",
        "cns_pdm_geographic_risk_drivers",
        "cns_pdm_geographic_risk_summary",
        "cns_pdm_geographic_risk_trend",
        "cns_pdm_lifecycle_exceptions",
        "cns_pdm_local_government_performance",
        "cns_pdm_parish_geographic_risk",
        "cns_pdm_parish_performance",
        "cns_pdm_payments_daily_summary",
        "cns_pdm_social_impact",
        "cns_pdm_subcounty_geographic_risk",
        "cns_pdm_village_geographic_risk",
    }
)


#
# Canonical Trino service identities for BI (D5). Prepared as configuration
# only: no password is provisioned here (F3_RUNTIME_TRINO_IDENTITY_PROVISIONING
# is outstanding).
#
SUPERSET_BI_TRINO_USER = "superset_bi"
METABASE_BI_TRINO_USER = "metabase_bi"

BI_TRINO_IDENTITIES: tuple[str, ...] = (
    SUPERSET_BI_TRINO_USER,
    METABASE_BI_TRINO_USER,
)


def normalize_dataset(dataset: str) -> str:
    return ".".join(
        part.strip().strip('"').lower()
        for part in (dataset or "").split(".")
        if part.strip()
    )


def is_bi_approved(dataset: str) -> bool:
    """True only for a curated, non-identity-bearing consumption dataset."""

    parts = normalize_dataset(dataset).split(".")

    if len(parts) != 3:
        return False

    catalog, schema, table = parts

    if catalog != BI_CATALOG or schema != BI_SCHEMA:
        return False

    if table in IDENTITY_BEARING_DATASETS:
        return False

    return table in APPROVED_BI_TABLES


@dataclass(frozen=True)
class BiPublicationRefusal:
    """A machine-readable refusal. Carries metadata only, never row data."""

    reason: str
    dataset: str


def check_bi_publication(dataset: str) -> BiPublicationRefusal | None:
    """Return a refusal when ``dataset`` may not be published.

    ``None`` means the dataset is approved for automated BI publication.
    """

    canonical = normalize_dataset(dataset)
    parts = canonical.split(".")

    if len(parts) != 3:
        return BiPublicationRefusal(
            "dataset reference must be catalog.schema.table",
            canonical,
        )

    _, _, table = parts

    if table in IDENTITY_BEARING_DATASETS:
        return BiPublicationRefusal(
            "dataset carries row-level beneficiary identity and is not "
            "approved for automated BI publication",
            canonical,
        )

    if table not in APPROVED_BI_TABLES:
        return BiPublicationRefusal(
            "dataset is not on the server-owned approved BI allowlist",
            canonical,
        )

    return None