#!/usr/bin/env python3
"""Build/update the PDM National Executive Intelligence dashboard for Superset 6.1."""
from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Optional

from superset.app import create_app

DASHBOARD_TITLE = "PDM NATIONAL EXECUTIVE INTELLIGENCE"
DASHBOARD_SLUG = "pdm-national-executive-intelligence"
SUPERSET_OWNER_USERNAME = "admin"
SCHEMA = "consumption"
EXECUTIVE_OVERVIEW = "cns_pdm_executive_overview"
EXECUTIVE_MONTHLY_TREND = "cns_pdm_executive_monthly_trend"
DISTRICT_RISK = "cns_pdm_district_geojson_risk"
EXECUTIVE_AI_RISK = "cns_pdm_executive_ai_risk"
DISTRICT_AI_RISK = "cns_pdm_district_ai_risk"
EXECUTIVE_INTERVENTION_MODEL = "cns_pdm_executive_intervention_priorities"
GEOJSON_MAP_DATASET = "vds_pdm_district_geojson_map"
EXECUTIVE_DRILLDOWN = "cns_pdm_executive_geographic_drilldown"

DISTRICT_SELECTOR_CHART = "DISTRICT DELIVERY DRILL"
COUNTY_DRILL_CHART = "COUNTY DELIVERY DRILL"
SUBCOUNTY_DRILL_CHART = "SUB COUNTY DELIVERY DRILL"
PARISH_DRILL_CHART = "PARISH DELIVERY DRILL"
VILLAGE_DRILL_CHART = "VILLAGE DELIVERY DRILL"
SACCO_DRILL_CHART = "PDM SACCO DELIVERY DRILL"
BENEFICIARY_DRILL_CHART = "BENEFICIARY DELIVERY DETAIL"

# Uganda national branding used by the executive header.
# Wikimedia Commons redirect is stable and avoids hard-coding a hashed upload path.
UGANDA_COAT_OF_ARMS_URL = (
    "/static/assets/images/Coat-of-arms-Uganda.webp"
)
EXECUTIVE_SUBTITLE = "National PDM Performance, Delivery and Risk Intelligence"
EXECUTIVE_TAGLINE = "Transforming Subsistence into Prosperity"

MAP_CHART = "UGANDA PDM RISK MAP"
FUNNEL_CHART = "PDM FUND DELIVERY"
MONTHLY_TREND_CHART = "MONTHLY FUNDING PERFORMANCE"
MONTHLY_VOLUME_CHART = "MONTHLY PROGRAMME REACH"
REPAYMENT_GAUGE = "Repayment Rate"
RECONCILIATION_GAUGE = "Payment Reconciliation Rate"
PAYMENT_SUCCESS_GAUGE = "Payment Success Rate"

EXECUTIVE_INTERVENTION_CHART = "EXECUTIVE INTERVENTION PRIORITIES"
AI_AVG_RISK_CHART = "AI DEFAULT RISK"
AI_PRIORITY_CASES_CHART = "AI PRIORITY CASES"
TOP_DISTRICTS_CHART = "DISTRICTS REQUIRING ATTENTION"
TOP_DISTRICTS_DATASET = "vds_pdm_top_districts_attention"
RISK_PROFILE_CHART = "NATIONAL RISK PROFILE"
RISK_PROFILE_DATASET = "vds_pdm_national_risk_profile"
DELIVERY_ASSURANCE_CHART = "DELIVERY & ASSURANCE"
DELIVERY_ASSURANCE_DATASET = "vds_pdm_delivery_assurance"

RISK_COLORS = {
    "NO DATA": "#90A4AE",
    "LOW": "#2E7D32",
    "MEDIUM": "#F9A825",
    "HIGH": "#EF6C00",
    "SEVERE": "#C62828",
}

UGANDA_VIEWPORT = {
    "longitude": 32.325,
    "latitude": 1.121,
    "zoom": 5.55,
    "bearing": 0,
    "pitch": 0,
}

HIERARCHY_LEVELS = [
    ("region", "Region"),
    ("district", "District"),
    ("county", "County"),
    ("sub_county", "Sub County"),
    ("parish", "Parish"),
    ("village", "Village"),
]

DRILL_LEVELS = [
    ("region", "Region"),
    ("district", "District"),
    ("county", "County"),
    ("sub_county", "Sub County"),
    ("parish", "Parish"),
    ("village", "Village"),
    ("sacco_name", "PDM SACCO"),
    ("beneficiary_id", "Beneficiary"),
]

DRILL_CHART_LEVELS = [
    (DISTRICT_SELECTOR_CHART, "district"),
    (COUNTY_DRILL_CHART, "county"),
    (SUBCOUNTY_DRILL_CHART, "sub_county"),
    (PARISH_DRILL_CHART, "parish"),
    (VILLAGE_DRILL_CHART, "village"),
    (SACCO_DRILL_CHART, "sacco_name"),
]

KPI_SPECS: List[Dict[str, Any]] = [
    {
        "title": "FUNDS APPROVED",
        "aliases": ["TOTAL APPROVED", "Total Approved"],
        "slug": "approved",
        "source": "monthly",
        "value": "approved_amount",
        "pct": "approved_amount_mom_pct",
        "currency": True,
        "increase_is_good": True,
    },
    {
        "title": "FUNDS DISBURSED",
        "aliases": ["TOTAL SETTLED", "Total Settled"],
        "slug": "disbursed",
        "source": "monthly",
        "value": "disbursed_amount",
        "pct": "disbursed_amount_mom_pct",
        "currency": True,
        "increase_is_good": True,
    },
    {
        "title": "FUNDS REPAID",
        "aliases": ["TOTAL CREDITED", "Total Credited"],
        "slug": "repaid",
        "source": "monthly",
        "value": "repaid_amount",
        "pct": "repaid_amount_mom_pct",
        "currency": True,
        "increase_is_good": True,
    },
    {
        "title": "BENEFICIARIES",
        "aliases": ["TOTAL DISBURSED", "Total Disbursed"],
        "slug": "beneficiaries",
        "source": "monthly",
        "value": "approved_beneficiary_count",
        "pct": "beneficiary_count_mom_pct",
        "currency": False,
        "increase_is_good": True,
    },
    {
        "title": "OUTSTANDING",
        "aliases": ["TOTAL OUTSTANDING", "Total Outstanding"],
        "slug": "outstanding",
        "source": "monthly",
        "value": "current_outstanding_amount",
        "pct": "outstanding_amount_mom_pct",
        "currency": True,
        "increase_is_good": False,
    },
    {
        "title": "HIGH RISK",
        "aliases": ["HIGH RISK CASES", "High Risk Cases"],
        "slug": "high_risk",
        "source": "overview",
        "current": "current_month_high_risk_case_count",
        "previous": "previous_month_high_risk_case_count",
        "pct": "high_risk_case_count_mom_pct",
        "currency": False,
        "increase_is_good": False,
    },
]


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower().strip()).strip("-")


def metric(column: str, label: str, aggregate: str = "SUM") -> Dict[str, Any]:
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column},
        "aggregate": aggregate,
        "sqlExpression": None,
        "hasCustomLabel": True,
        "label": label,
        "optionName": f"metric_{slugify(label)}",
    }


def column_names(dataset: Any) -> set[str]:
    return {c.column_name for c in dataset.columns}


def refresh_dataset(dataset: Any, session: Any) -> None:
    try:
        dataset.fetch_metadata()
        session.flush()
    except Exception as exc:
        print(f"WARNING: metadata refresh skipped for {dataset.table_name}: {exc}")


def mark_temporal(dataset: Any, name: str, session: Any) -> None:
    for col in dataset.columns:
        if col.column_name == name:
            if hasattr(col, "is_dttm"):
                col.is_dttm = True
            session.flush()
            return
    raise RuntimeError(f"{dataset.table_name} is missing temporal column {name!r}")


def trino_database(session: Any, Database: Any) -> Any:
    """Resolve the existing Superset Trino database connection without hard-coding an id."""
    databases = session.query(Database).order_by(Database.id.asc()).all()

    for database in databases:
        try:
            uri = database.sqlalchemy_uri or ""
        except Exception:
            uri = ""

        if "trino" in uri.lower():
            print(
                f"Using Trino database id={database.id}: "
                f"{database.database_name}"
            )
            return database

    for database in databases:
        if "trino" in (database.database_name or "").lower():
            print(
                f"Using Trino database id={database.id}: "
                f"{database.database_name}"
            )
            return database

    available = [
        f"id={database.id}:{database.database_name}"
        for database in databases
    ]
    raise RuntimeError(
        "No Superset Trino database connection was found. "
        f"Available databases: {available}"
    )




def resolve_owner_user() -> Any:
    """
    Resolve the Superset user who should own/edit the generated dashboard
    and charts.

    Superset 6.1 exposes chart/dashboard edit permissions through owners.
    Avoid newer subject/editor internals so this stays compatible with the
    installed 6.1 image.
    """
    import os

    from superset.extensions import appbuilder

    username = os.getenv(
        "SUPERSET_ADMIN_USERNAME",
        SUPERSET_OWNER_USERNAME,
    )

    user = appbuilder.sm.find_user(username=username)

    if user is None:
        raise RuntimeError(
            f"Superset owner user {username!r} was not found. "
            "Run superset-init first or set SUPERSET_ADMIN_USERNAME "
            "to an existing Superset username."
        )

    print(
        f"Using Superset owner/editor user "
        f"id={user.id}: {user.username}"
    )

    return user


def assign_owner(entity: Any, owner_user: Any) -> None:
    """
    Ensure the requested user owns the chart/dashboard.

    Keep any existing owners while adding the configured admin/editor.
    """
    if not hasattr(entity, "owners"):
        raise RuntimeError(
            f"{type(entity).__name__} does not expose an owners relationship "
            "in this Superset build."
        )

    current = list(entity.owners or [])

    if all(existing.id != owner_user.id for existing in current):
        current.append(owner_user)

    entity.owners = current



def executive_snapshot(database: Any) -> Dict[str, Any]:
    """Read the latest MoM comparison values used by the six executive KPIs."""
    from sqlalchemy import text as sql_text

    sql = f"""
    WITH latest_monthly AS (
        SELECT *
        FROM {SCHEMA}.{EXECUTIVE_MONTHLY_TREND}
        ORDER BY reporting_month DESC
        LIMIT 1
    )
    SELECT
        overview.kpi_reporting_month,
        overview.kpi_previous_month,
        monthly.approved_amount_mom_pct,
        monthly.disbursed_amount_mom_pct,
        monthly.repaid_amount_mom_pct,
        monthly.beneficiary_count_mom_pct,
        monthly.outstanding_amount_mom_pct,
        overview.high_risk_case_count_mom_pct
    FROM {SCHEMA}.{EXECUTIVE_OVERVIEW} overview
    CROSS JOIN latest_monthly monthly
    LIMIT 1
    """

    try:
        with database.get_sqla_engine(schema=SCHEMA) as engine:
            with engine.connect() as connection:
                row = connection.execute(sql_text(sql)).mappings().first()
    except TypeError:
        with database.get_sqla_engine() as engine:
            with engine.connect() as connection:
                row = connection.execute(sql_text(sql)).mappings().first()

    if row is None:
        raise RuntimeError("Executive KPI snapshot returned no row")

    snapshot = dict(row)
    previous_month = snapshot.get("kpi_previous_month")
    if previous_month is not None:
        try:
            snapshot["previous_month_label"] = previous_month.strftime("%b").upper()
        except AttributeError:
            snapshot["previous_month_label"] = str(previous_month)[5:7]
    else:
        snapshot["previous_month_label"] = "PRIOR"

    return snapshot

def comparison_text(
    pct: Any,
    previous_month_label: str,
    *,
    increase_is_good: bool,
) -> str:
    """
    Create a concise executive comparison label.

    Examples:
        ▼ 6.3% vs AUG
        ▲ 4.1% vs AUG
        — 0.0% vs AUG
        — vs AUG
    """
    if pct is None:
        return f"— vs {previous_month_label}"

    value = float(pct)

    if value > 0:
        arrow = "▲"
    elif value < 0:
        arrow = "▼"
    else:
        arrow = "—"

    return f"{arrow} {abs(value):.1f}% vs {previous_month_label}"



def physical_dataset(
    session: Any,
    SqlaTable: Any,
    *,
    database: Any,
    table_name: str,
) -> Any:
    """
    Resolve a registered physical dataset or register it automatically.

    The dbt model must already exist in Trino under the consumption schema.
    """
    ds = (
        session.query(SqlaTable)
        .filter(
            SqlaTable.database_id == database.id,
            SqlaTable.schema == SCHEMA,
            SqlaTable.table_name == table_name,
        )
        .order_by(SqlaTable.id.asc())
        .first()
    )

    if ds is None:
        ds = SqlaTable(
            table_name=table_name,
            schema=SCHEMA,
            database=database,
        )
        session.add(ds)
        session.flush()
        print(
            f"Registered physical dataset id={ds.id}: "
            f"{SCHEMA}.{table_name}"
        )
    else:
        print(
            f"Using physical dataset id={ds.id}: "
            f"{SCHEMA}.{table_name}"
        )

    refresh_dataset(ds, session)

    if not ds.columns:
        raise RuntimeError(
            f"Superset registered {SCHEMA}.{table_name} but discovered no columns. "
            "Confirm the table exists in Trino and the Superset Trino connection "
            "can read the consumption schema."
        )

    return ds


def virtual_dataset(
    session: Any,
    SqlaTable: Any,
    *,
    name: str,
    sql: str,
    base: Any,
    temporal: Optional[str] = None,
) -> Any:
    ds = (
        session.query(SqlaTable)
        .filter(SqlaTable.database_id == base.database_id, SqlaTable.table_name == name)
        .order_by(SqlaTable.id.asc())
        .first()
    )
    if ds is None:
        ds = SqlaTable(table_name=name, database=base.database, schema=SCHEMA, sql=sql)
        session.add(ds)
        session.flush()
        print(f"Created virtual dataset id={ds.id}: {name}")
    else:
        ds.sql = sql
        ds.schema = SCHEMA
        session.flush()
        print(f"Updated virtual dataset id={ds.id}: {name}")
    refresh_dataset(ds, session)
    if temporal:
        mark_temporal(ds, temporal, session)
    return ds


def dashboard_object(session: Any, Dashboard: Any, owner_user: Any) -> Any:
    """
    Resolve the dashboard by its stable slug so display-title changes do not
    create duplicate dashboards.
    """
    dbd = (
        session.query(Dashboard)
        .filter(Dashboard.slug == DASHBOARD_SLUG)
        .order_by(Dashboard.id.asc())
        .first()
    )

    if dbd is None:
        dbd = Dashboard(
            dashboard_title=DASHBOARD_TITLE,
            slug=DASHBOARD_SLUG,
            published=True,
        )
        session.add(dbd)
        session.flush()
        print(f"Created dashboard id={dbd.id}: {DASHBOARD_TITLE}")
    else:
        dbd.dashboard_title = DASHBOARD_TITLE
        dbd.slug = DASHBOARD_SLUG
        dbd.published = True
        session.flush()
        print(f"Updated dashboard id={dbd.id}: {DASHBOARD_TITLE}")

    assign_owner(dbd, owner_user)
    session.flush()

    return dbd


def upsert_chart(
    session: Any,
    Slice: Any,
    *,
    name: str,
    dataset: Any,
    viz_type: str,
    params: Dict[str, Any],
    description: str,
    owner_user: Any,
    aliases: Optional[Iterable[str]] = None,
) -> Any:
    """
    Create or update a chart idempotently.

    `aliases` lets the builder rename an existing chart (for example
    "Total Approved" -> "TOTAL APPROVED") instead of creating a duplicate.
    """
    candidate_names = [name]
    if aliases:
        candidate_names.extend(alias for alias in aliases if alias)

    chart = (
        session.query(Slice)
        .filter(Slice.slice_name.in_(candidate_names))
        .order_by(Slice.id.asc())
        .first()
    )

    payload = dict(params)
    payload["viz_type"] = viz_type
    payload["datasource"] = f"{dataset.id}__table"

    if chart is None:
        chart = Slice(
            slice_name=name,
            viz_type=viz_type,
            datasource_type="table",
            datasource_id=dataset.id,
            params=json.dumps(payload),
            description=description,
        )
        session.add(chart)
        session.flush()
        print(f"Created chart id={chart.id}: {name}")
    else:
        chart.slice_name = name
        chart.viz_type = viz_type
        chart.datasource_type = "table"
        chart.datasource_id = dataset.id
        chart.params = json.dumps(payload)
        chart.description = description
        session.flush()
        print(f"Updated chart id={chart.id}: {name}")

    assign_owner(chart, owner_user)
    session.flush()

    return chart


def kpi_sql(spec: Dict[str, Any]) -> str:
    if spec["source"] == "monthly":
        return f"""
SELECT reporting_month, CAST({spec['value']} AS DOUBLE) AS value
FROM {SCHEMA}.{EXECUTIVE_MONTHLY_TREND}
WHERE reporting_month IS NOT NULL
ORDER BY reporting_month
""".strip()

    return f"""
SELECT kpi_previous_month AS reporting_month, CAST({spec['previous']} AS DOUBLE) AS value
FROM {SCHEMA}.{EXECUTIVE_OVERVIEW}
UNION ALL
SELECT kpi_reporting_month AS reporting_month, CAST({spec['current']} AS DOUBLE) AS value
FROM {SCHEMA}.{EXECUTIVE_OVERVIEW}
ORDER BY reporting_month
""".strip()

def funnel_sql() -> str:
    return f"""
SELECT 1 stage_order, 'Approved' stage, CAST(current_month_total_approved_amount AS DOUBLE) amount FROM {SCHEMA}.{EXECUTIVE_OVERVIEW}
UNION ALL
SELECT 2, 'Settled', CAST(current_month_total_settled_amount AS DOUBLE) FROM {SCHEMA}.{EXECUTIVE_OVERVIEW}
UNION ALL
SELECT 3, 'Credited', CAST(current_month_total_credited_amount AS DOUBLE) FROM {SCHEMA}.{EXECUTIVE_OVERVIEW}
UNION ALL
SELECT 4, 'Disbursed', CAST(current_month_total_disbursed_amount AS DOUBLE) FROM {SCHEMA}.{EXECUTIVE_OVERVIEW}
ORDER BY stage_order
""".strip()


def available_hierarchy_columns(dataset: Any) -> List[str]:
    """Return geographic drill columns that actually exist in this dataset."""
    available = column_names(dataset)
    return [column for column, _label in HIERARCHY_LEVELS if column in available]


def hierarchy_label(column: str) -> str:
    return dict(DRILL_LEVELS).get(column, column.replace("_", " ").title())


def executive_intervention_sql(hierarchy_columns: Iterable[str]) -> str:
    """
    Build a ranked executive-exception dataset from observed district risk data.

    This intentionally uses only fields that already exist in the PDM
    geographic-risk model. It does not invent targets, accountable officers or
    beneficiary-level facts that are not present in the source.
    """
    hierarchy_columns = list(hierarchy_columns)
    hierarchy_select = ",\n        ".join(hierarchy_columns)

    hierarchy_prefix = ""
    if hierarchy_select:
        hierarchy_prefix = hierarchy_select + ",\n        "

    return f"""
WITH ranked AS (
    SELECT
        {hierarchy_prefix}
        geographic_risk_band,
        map_risk_score,
        beneficiary_count,
        loan_count,
        approved_amount,
        disbursed_amount,
        repaid_amount,
        outstanding_amount,
        avg_disbursement_rate,
        avg_principal_repayment_rate,
        high_identity_alert_count,
        account_substitution_amount,
        mapped_agent_count,

        CASE
            WHEN COALESCE(map_risk_score, 0) >= 4 THEN 1
            WHEN COALESCE(map_risk_score, 0) = 3 THEN 2
            WHEN COALESCE(avg_principal_repayment_rate, 1.0) < 0.50 THEN 3
            WHEN COALESCE(high_identity_alert_count, 0) > 0 THEN 4
            WHEN COALESCE(account_substitution_amount, 0.0) > 0 THEN 4
            WHEN COALESCE(outstanding_amount, 0) > 0 THEN 5
            ELSE 6
        END AS priority_rank,

        CASE
            WHEN COALESCE(map_risk_score, 0) >= 4 THEN 'CRITICAL'
            WHEN COALESCE(map_risk_score, 0) = 3 THEN 'HIGH'
            WHEN COALESCE(avg_principal_repayment_rate, 1.0) < 0.50 THEN 'HIGH'
            WHEN COALESCE(high_identity_alert_count, 0) > 0 THEN 'WATCH'
            WHEN COALESCE(account_substitution_amount, 0.0) > 0 THEN 'WATCH'
            WHEN COALESCE(outstanding_amount, 0) > 0 THEN 'MONITOR'
            ELSE 'NORMAL'
        END AS executive_priority,

        CASE
            WHEN COALESCE(map_risk_score, 0) >= 4
                THEN 'Severe geographic risk'
            WHEN COALESCE(map_risk_score, 0) = 3
                THEN 'High geographic risk'
            WHEN COALESCE(avg_principal_repayment_rate, 1.0) < 0.50
                THEN 'Repayment rate below 50%'
            WHEN COALESCE(high_identity_alert_count, 0) > 0
                THEN 'Identity alerts require verification'
            WHEN COALESCE(account_substitution_amount, 0.0) > 0
                THEN 'Account substitution exposure detected'
            WHEN COALESCE(outstanding_amount, 0) > 0
                THEN 'Outstanding balance requires monitoring'
            ELSE 'No immediate executive exception'
        END AS intervention_reason,

        CASE
            WHEN COALESCE(map_risk_score, 0) >= 4
                THEN 'Immediate district review and field verification'
            WHEN COALESCE(map_risk_score, 0) = 3
                THEN 'Escalate district performance review'
            WHEN COALESCE(avg_principal_repayment_rate, 1.0) < 0.50
                THEN 'Review repayment recovery plan'
            WHEN COALESCE(high_identity_alert_count, 0) > 0
                THEN 'Verify beneficiary identity exceptions'
            WHEN COALESCE(account_substitution_amount, 0.0) > 0
                THEN 'Investigate account substitution events'
            WHEN COALESCE(outstanding_amount, 0) > 0
                THEN 'Monitor outstanding portfolio'
            ELSE 'Continue routine monitoring'
        END AS recommended_action

    FROM {SCHEMA}.{DISTRICT_RISK}
)

SELECT *
FROM ranked
WHERE priority_rank < 6
ORDER BY
    priority_rank ASC,
    COALESCE(outstanding_amount, 0) DESC,
    COALESCE(map_risk_score, 0) DESC
""".strip()



def top_districts_sql() -> str:
    """Rank districts by combined geographic + predictive default-risk priority."""
    return f"""
SELECT
    district,
    region,
    geographic_risk_band,
    ai_intervention_band,
    CAST(COALESCE(avg_probability_default_90d, 0) * 100.0 AS DOUBLE)
        AS ai_default_risk_pct,
    CAST(COALESCE(ai_priority_review_count, 0) AS BIGINT)
        AS ai_priority_review_count,
    CAST(COALESCE(outstanding_amount, 0) AS DOUBLE)
        AS outstanding_amount,
    CAST(COALESCE(intervention_priority_score, 0) AS DOUBLE)
        AS intervention_priority_score,
    recommended_action
FROM {SCHEMA}.{EXECUTIVE_INTERVENTION_MODEL}
WHERE district IS NOT NULL
ORDER BY
    intervention_rank ASC,
    COALESCE(intervention_priority_score, 0) DESC
LIMIT 10
""".strip()


def risk_profile_sql() -> str:
    """National district composition by geographic risk band."""
    return f"""
SELECT
    CASE COALESCE(map_risk_score, 0)
        WHEN 1 THEN 'LOW'
        WHEN 2 THEN 'MEDIUM'
        WHEN 3 THEN 'HIGH'
        WHEN 4 THEN 'SEVERE'
        ELSE 'NO DATA'
    END AS risk_band,
    COALESCE(map_risk_score, 0) AS risk_order,
    COUNT(*) AS district_count
FROM {SCHEMA}.{DISTRICT_RISK}
GROUP BY 1, 2
ORDER BY risk_order
""".strip()


def delivery_assurance_sql() -> str:
    """Compact operational assurance measures as percentage-point values."""
    return f"""
SELECT 1 metric_order, 'Repayment' metric_name,
       CAST(COALESCE(repayment_rate, 0) * 100.0 AS DOUBLE) metric_value
FROM {SCHEMA}.{EXECUTIVE_OVERVIEW}
UNION ALL
SELECT 2, 'Reconciliation',
       CAST(COALESCE(reconciliation_rate, 0) * 100.0 AS DOUBLE)
FROM {SCHEMA}.{EXECUTIVE_OVERVIEW}
UNION ALL
SELECT 3, 'Payment Success',
       CAST(COALESCE(payment_success_rate, 0) * 100.0 AS DOUBLE)
FROM {SCHEMA}.{EXECUTIVE_OVERVIEW}
ORDER BY metric_order
""".strip()


def top_districts_params() -> Dict[str, Any]:
    return {
        "x_axis": "district",
        "metrics": [
            metric(
                "intervention_priority_score",
                "Priority Score",
                "MAX",
            )
        ],
        "groupby": ["recommended_action"],
        "adhoc_filters": [],
        "row_limit": 10,
        "order_desc": True,
        "orientation": "horizontal",
        "sort_series_type": "sum",
        "sort_series_ascending": True,
        "show_legend": True,
        "legendOrientation": "top",
        "show_value": True,
        "rich_tooltip": True,
        "y_axis_format": ".1f",
        "x_axis_title": "District",
        "y_axis_title": "Combined Priority Score",
        "truncateXAxis": False,
        "truncateYAxis": False,
    }


def risk_profile_params() -> Dict[str, Any]:
    return {
        "groupby": ["risk_band"],
        "metric": metric("district_count", "Districts"),
        "adhoc_filters": [],
        "row_limit": 10,
        "show_legend": True,
        "legendOrientation": "right",
        "show_labels": True,
        "label_type": "key_value_percent",
        "number_format": ",d",
        "donut": True,
        "innerRadius": 52,
    }


def delivery_assurance_params() -> Dict[str, Any]:
    """
    Horizontal bar/progress-style view. Using ECharts Bar keeps this within
    standard Superset 6.1 visualisations rather than depending on a custom
    bullet-chart plugin.
    """
    return {
        "x_axis": "metric_name",
        "metrics": [metric("metric_value", "Rate")],
        "groupby": [],
        "adhoc_filters": [],
        "row_limit": 10,
        "show_legend": False,
        "show_value": True,
        "rich_tooltip": True,
        "y_axis_format": ".1f",
        "x_axis_title": "",
        "y_axis_title": "Percent",
        "y_axis_bounds": [0, 100],
        "orientation": "horizontal",
    }


def intervention_table_params(hierarchy_columns: Iterable[str]) -> Dict[str, Any]:
    """Single executive table backed by the dbt Leadership intervention model."""
    hierarchy_columns = [
        column
        for column in hierarchy_columns
        if column in {"region", "district"}
    ]

    preferred_columns = [
        "intervention_rank",
        "recommended_action",
        *hierarchy_columns,
        "geographic_risk_band",
        "ai_intervention_band",
        "avg_probability_default_90d",
        "ai_priority_review_count",
        "ai_severe_risk_count",
        "outstanding_amount",
        "intervention_priority_score",
        "interpretation",
    ]

    return {
        "query_mode": "raw",
        "all_columns": preferred_columns,
        "adhoc_filters": [],
        "row_limit": 30,
        "page_length": 15,
        "include_search": True,
        "show_cell_bars": False,
        "allow_rearrange_columns": True,
        "order_desc": False,
        "emit_filter": True,
    }


def drill_table_params(level_column: str) -> Dict[str, Any]:
    """Clickable aggregate table for one level of the local delivery hierarchy."""
    return {
        "query_mode": "aggregate",
        "groupby": [level_column],
        "metrics": [
            metric("beneficiary_sk", "Beneficiaries", "COUNT_DISTINCT"),
            metric("loan_sk", "Loans", "COUNT_DISTINCT"),
            metric("approved_amount", "Approved UGX"),
            metric("disbursed_amount", "Disbursed UGX"),
            metric("repaid_amount", "Repaid UGX"),
            metric("outstanding_amount", "Outstanding UGX"),
            metric("repayment_rate", "Repayment Rate", "AVG"),
            metric("risk_indicator_count", "Risk Flags"),
        ],
        "adhoc_filters": [],
        "row_limit": 1000,
        "page_length": 15,
        "include_search": True,
        "show_cell_bars": False,
        "allow_rearrange_columns": True,
        "order_desc": True,
        "emit_filter": True,
    }


def beneficiary_drill_params() -> Dict[str, Any]:
    """Privacy-limited beneficiary detail; deliberately excludes direct PII."""
    return {
        "query_mode": "raw",
        "all_columns": [
            "beneficiary_id",
            "beneficiary_token",
            "region",
            "district",
            "county",
            "sub_county",
            "parish",
            "village",
            "sacco_name",
            "loan_id",
            "approved_amount",
            "disbursed_amount",
            "repaid_amount",
            "outstanding_amount",
            "repayment_rate",
            "risk_indicator_count",
            "risk_band",
        ],
        "adhoc_filters": [],
        "row_limit": 5000,
        "page_length": 20,
        "include_search": True,
        "show_cell_bars": False,
        "allow_rearrange_columns": True,
        "order_desc": False,
        "emit_filter": True,
    }


def geojson_map_sql(hierarchy_columns: Iterable[str]) -> str:
    """
    Return only renderable geometry and enrich each GeoJSON Feature with
    deterministic risk colours.

    Any geographic hierarchy columns present in the physical source are carried
    into the virtual map dataset automatically, allowing the dashboard's native
    drill filters to grow without another redesign.
    """
    hierarchy_columns = list(hierarchy_columns)
    optional = [
        col for col in hierarchy_columns
        if col not in {"region", "district"}
    ]

    cte_optional = "".join(f"        {col},\n" for col in optional)
    select_optional = "".join(f"    {col},\n" for col in optional)

    return f"""
WITH risk_source AS (
    SELECT
        region,
        district,
{cte_optional}        superset_district_iso,

        map_mapping_status,
        geometry_status,
        district_data_status,

        parish_count,
        assessed_parish_count,
        severe_parish_count,
        high_parish_count,
        medium_parish_count,
        low_parish_count,

        loan_count,
        beneficiary_count,

        approved_amount,
        disbursed_amount,
        repaid_amount,
        outstanding_amount,

        geographic_risk_score,
        map_risk_score,
        geographic_risk_band,
        geographic_risk_sort_order,
        geographic_risk_legend_label,

        avg_disbursement_rate,
        avg_principal_repayment_rate,
        avg_disbursement_peer_zscore,
        avg_repayment_peer_zscore,

        high_identity_alert_count,
        account_substitution_amount,
        mapped_agent_count,

        latitude,
        longitude,

        CASE COALESCE(map_risk_score, 0)
            WHEN 1 THEN '#2E7D32'
            WHEN 2 THEN '#F9A825'
            WHEN 3 THEN '#EF6C00'
            WHEN 4 THEN '#C62828'
            ELSE '#90A4AE'
        END AS risk_fill_color,

        '#FFFFFF' AS risk_stroke_color,

        geojson

    FROM {SCHEMA}.{DISTRICT_RISK}

    WHERE geojson IS NOT NULL
      AND trim(geojson) <> ''
      AND geometry_status = 'HAS GEOMETRY'
)

SELECT
    region,
    district,
{select_optional}    superset_district_iso,

    map_mapping_status,
    geometry_status,
    district_data_status,

    parish_count,
    assessed_parish_count,
    severe_parish_count,
    high_parish_count,
    medium_parish_count,
    low_parish_count,

    loan_count,
    beneficiary_count,

    approved_amount,
    disbursed_amount,
    repaid_amount,
    outstanding_amount,

    geographic_risk_score,
    map_risk_score,
    geographic_risk_band,
    geographic_risk_sort_order,
    geographic_risk_legend_label,

    avg_disbursement_rate,
    avg_principal_repayment_rate,
    avg_disbursement_peer_zscore,
    avg_repayment_peer_zscore,

    high_identity_alert_count,
    account_substitution_amount,
    mapped_agent_count,

    latitude,
    longitude,

    risk_fill_color,
    risk_stroke_color,

    replace(
        geojson,
        '"properties":{{',
        concat(
            '"properties":{{',
            '"fillColor":"', risk_fill_color, '",',
            '"strokeColor":"', risk_stroke_color, '",',
            '"district":"', replace(COALESCE(district, ''), '"', ''), '",',
            '"region":"', replace(COALESCE(region, ''), '"', ''), '",',
            '"geographic_risk_band":"', replace(COALESCE(geographic_risk_band, 'NO DATA'), '"', ''), '",',
            '"map_risk_score":', CAST(COALESCE(map_risk_score, 0) AS VARCHAR), ',',
            '"beneficiary_count":', CAST(COALESCE(beneficiary_count, 0) AS VARCHAR), ',',
            '"loan_count":', CAST(COALESCE(loan_count, 0) AS VARCHAR), ',',
            '"approved_amount":', CAST(COALESCE(approved_amount, 0) AS VARCHAR), ',',
            '"disbursed_amount":', CAST(COALESCE(disbursed_amount, 0) AS VARCHAR), ',',
            '"repaid_amount":', CAST(COALESCE(repaid_amount, 0) AS VARCHAR), ',',
            '"outstanding_amount":', CAST(COALESCE(outstanding_amount, 0) AS VARCHAR), ',',
            '"avg_disbursement_rate":', CAST(COALESCE(avg_disbursement_rate, 0) AS VARCHAR), ',',
            '"avg_principal_repayment_rate":', CAST(COALESCE(avg_principal_repayment_rate, 0) AS VARCHAR), ',',
            '"high_identity_alert_count":', CAST(COALESCE(high_identity_alert_count, 0) AS VARCHAR), ',',
            '"account_substitution_amount":', CAST(COALESCE(account_substitution_amount, 0) AS VARCHAR), ',',
            '"mapped_agent_count":', CAST(COALESCE(mapped_agent_count, 0) AS VARCHAR), ','
        )
    ) AS geojson

FROM risk_source
""".strip()



def big_number_params(
    title: str,
    currency: bool,
    increase_is_good: bool,
    comparison: str,
) -> Dict[str, Any]:
    """
    Configure a single-title executive KPI card.

    The Superset chart header supplies the visible KPI title. The internal
    metric name is hidden to avoid duplication. The comparison line is supplied
    explicitly so the dashboard can show ▲ / ▼ / — together with the actual
    previous reporting month.
    """
    p: Dict[str, Any] = {
        "granularity_sqla": "reporting_month",
        "time_grain_sqla": "P1M",
        "time_range": "No filter",
        "metric": metric("value", title),
        "adhoc_filters": [],

        # We supply our own executive comparison text below.
        "compare_lag": 0,
        "show_trend_line": False,
        "show_timestamp": False,

        # Prevent duplicate KPI titles.
        "show_metric_name": False,

        "metric_name_font_size": 0.14,
        "header_font_size": 0.48,
        "subheader_font_size": 0.19,

        # Explicit arrow comparison, e.g. "▼ 6.3% vs AUG".
        "subheader": comparison,

        "start_y_axis_at_zero": False,
        "time_format": "%b %Y",
        "pdm_increase_is_good": increase_is_good,
    }

    if currency:
        p["y_axis_format"] = ",.3s"
        p["currency_format"] = {
            "symbol": "UGX ",
            "symbolPosition": "prefix",
            "currencyDigits": 1,
        }
    else:
        p["y_axis_format"] = ",d"

    return p


def ai_big_number_params(
    column: str,
    label: str,
    *,
    percentage: bool = False,
) -> Dict[str, Any]:
    """Executive AI KPI sourced from persisted XGBoost scoring output."""
    return {
        "metric": metric(column, label, "MAX"),
        "adhoc_filters": [],
        "row_limit": 1,
        "show_trend_line": False,
        "show_timestamp": False,
        "show_metric_name": False,
        "header_font_size": 0.48,
        "subheader_font_size": 0.18,
        "subheader": (
            "XGBoost 90-day predicted default probability"
            if percentage
            else "HIGH + SEVERE predicted-default cases"
        ),
        "y_axis_format": ".1%" if percentage else ",d",
    }


def map_params() -> Dict[str, Any]:
    """Stable Uganda-only Deck.gl GeoJSON executive risk map."""
    return {
        "geojson": "geojson",
        "row_limit": 500,
        "adhoc_filters": [],
        "emit_filter": True,

        "tooltip_contents": [
            "district",
            "region",
            "geographic_risk_band",
            "map_risk_score",
            "beneficiary_count",
            "loan_count",
            "approved_amount",
            "disbursed_amount",
            "repaid_amount",
            "outstanding_amount",
            "avg_disbursement_rate",
            "avg_principal_repayment_rate",
            "high_identity_alert_count",
            "account_substitution_amount",
            "mapped_agent_count",
        ],

        # Keep Uganda stable when native filters change the returned features.
        "autozoom": False,
        "viewport": dict(UGANDA_VIEWPORT),

        # Preserve feature-specific risk colours injected into the GeoJSON.
        "fill_color_picker": {"r": 0, "g": 0, "b": 0, "a": 0},
        "stroke_color_picker": {"r": 255, "g": 255, "b": 255, "a": 0},

        "filled": True,
        "stroked": True,
        "extruded": False,
        "line_width": 1,
        "line_width_unit": "pixels",

        "enable_labels": True,
        "label_property_name": "district",
        "label_size": 10,
        "label_size_unit": "pixels",
        "enable_icons": False,

        "point_radius": 10,
        "point_radius_scale": 1,
        "point_radius_units": "pixels",
    }

def funnel_params() -> Dict[str, Any]:
    return {
        "groupby": ["stage"],
        "metric": metric("amount", "UGX Amount"),
        "adhoc_filters": [],
        "row_limit": 10,
        "show_legend": False,
        "show_labels": True,
        "label_type": "key_value",
        "number_format": ",.3s",
        "sort_by_metric": True,
    }


def monthly_trend_params() -> Dict[str, Any]:
    return {
        "granularity_sqla": "reporting_month",
        "time_grain_sqla": "P1M",
        "time_range": "No filter",
        "metrics": [
            metric("approved_amount", "Approved"),
            metric("disbursed_amount", "Disbursed"),
            metric("repaid_amount", "Repaid"),
        ],
        "adhoc_filters": [],
        "groupby": [],
        "row_limit": 10000,
        "show_legend": True,
        "legendOrientation": "top",
        "x_axis_time_format": "%b %Y",
        "y_axis_format": ",.3s",
        "rich_tooltip": True,
        "show_value": False,
        "x_axis_title": "Reporting month",
        "y_axis_title": "UGX",
    }


def monthly_volume_params() -> Dict[str, Any]:
    return {
        "granularity_sqla": "reporting_month",
        "time_grain_sqla": "P1M",
        "time_range": "No filter",
        "metrics": [
            metric("approved_beneficiary_count", "Beneficiaries"),
            metric("approved_loan_count", "Approved Loans"),
        ],
        "adhoc_filters": [],
        "groupby": [],
        "row_limit": 10000,
        "show_legend": True,
        "legendOrientation": "top",
        "x_axis_time_format": "%b %Y",
        "y_axis_format": ",d",
        "rich_tooltip": True,
        "show_value": True,
        "x_axis_title": "Reporting month",
        "y_axis_title": "Count",
    }


def gauge_params(column: str, label: str) -> Dict[str, Any]:
    return {
        "metric": metric(column, label, "MAX"),
        "adhoc_filters": [],
        "row_limit": 1,
        "min_val": 0,
        "max_val": 1,
        "start_angle": 225,
        "end_angle": -45,
        "show_pointer": True,
        "show_progress": True,
        "round_cap": True,
        "number_format": ".1%",
        "value_formatter": ".1%",
        "animation": True,
    }


def chart_node(
    chart: Any,
    row_id: str,
    width: int,
    height: int,
    *,
    parents: Optional[List[str]] = None,
) -> Dict[str, Any]:
    node_parents = parents or ["ROOT_ID", "GRID_ID", row_id]
    return {
        "id": f"CHART-{chart.id}",
        "type": "CHART",
        "meta": {
            "chartId": chart.id,
            "sliceName": chart.slice_name,
            "width": width,
            "height": height,
        },
        "parents": node_parents,
    }


def markdown_node(
    node_id: str,
    row_id: str,
    code: str,
    width: int,
    height: int,
    *,
    parents: Optional[List[str]] = None,
) -> Dict[str, Any]:
    node_parents = parents or ["ROOT_ID", "GRID_ID", row_id]
    return {
        "id": node_id,
        "type": "MARKDOWN",
        "meta": {
            "code": code,
            "width": width,
            "height": height,
        },
        "parents": node_parents,
    }


def executive_header_markdown() -> str:
    """Professional Uganda-branded executive dashboard banner."""
    return f"""
<div class="pdm-executive-header">
  <div class="pdm-header-left">
    <img
      class="pdm-coat-of-arms"
      src="{UGANDA_COAT_OF_ARMS_URL}"
      alt="Coat of Arms of Uganda"
    />
    <div class="pdm-header-copy">
      <div class="pdm-republic-label">REPUBLIC OF UGANDA</div>
      <div class="pdm-main-title">{DASHBOARD_TITLE}</div>
      <div class="pdm-main-subtitle">{EXECUTIVE_SUBTITLE}</div>
    </div>
  </div>

  <div class="pdm-header-right">
    <div class="pdm-brand-mark">PDM</div>
    <div class="pdm-brand-tagline">{EXECUTIVE_TAGLINE}</div>
  </div>
</div>
""".strip()


def executive_footer_markdown() -> str:
    return """
<div class="pdm-executive-footer">
  <div><strong>PDM National Executive Intelligence</strong></div>
  <div>National funding • delivery • geographic risk • AI default risk • payment performance</div>
  <div class="pdm-footer-tagline">Transforming Subsistence into Prosperity</div>
</div>
""".strip()


def leadership_nav_top_markdown() -> str:
    return """
<div class="pdm-left-nav-segment pdm-left-nav-top">
  <div class="pdm-left-nav-brand">LEADERSHIP</div>
  <div class="pdm-left-nav-section">
    <div class="pdm-left-nav-heading">EXECUTIVE</div>
    <div class="pdm-left-nav-item active">Overview</div>
    <div class="pdm-left-nav-item">Intervention</div>
    <div class="pdm-left-nav-item">Geography</div>
    <div class="pdm-left-nav-item">Performance</div>
  </div>
  <div class="pdm-left-nav-section">
    <div class="pdm-left-nav-heading">DRILL</div>
    <div class="pdm-left-nav-item">Region</div>
    <div class="pdm-left-nav-item">District</div>
    <div class="pdm-left-nav-item">County</div>
    <div class="pdm-left-nav-item">Sub County</div>
    <div class="pdm-left-nav-item">Parish</div>
    <div class="pdm-left-nav-item">Village</div>
    <div class="pdm-left-nav-item">PDM SACCO</div>
    <div class="pdm-left-nav-item">Beneficiary</div>
  </div>
</div>
""".strip()

def leadership_nav_legend_markdown() -> str:
    return """
<div class="pdm-left-nav-segment pdm-left-nav-middle pdm-left-nav-compact">
  <div class="pdm-left-nav-heading">RISK VIEW</div>
  <div class="pdm-left-nav-item active">National Risk</div>
</div>
""".strip()

def leadership_nav_trend_markdown() -> str:
    return """
<div class="pdm-left-nav-segment pdm-left-nav-middle">
  <div class="pdm-left-nav-section pdm-left-nav-section-first">
    <div class="pdm-left-nav-heading">PERFORMANCE</div>
    <div class="pdm-left-nav-item active">Funding Performance</div>
    <div class="pdm-left-nav-item">Fund Delivery</div>
  </div>
</div>
""".strip()

def leadership_nav_bottom_markdown() -> str:
    return """
<div class="pdm-left-nav-segment pdm-left-nav-bottom">
  <div class="pdm-left-nav-section pdm-left-nav-section-first">
    <div class="pdm-left-nav-heading">DELIVERY</div>
    <div class="pdm-left-nav-item active">Programme Reach</div>
    <div class="pdm-left-nav-item">Delivery &amp; Assurance</div>
  </div>
  <div class="pdm-left-nav-meta">
    <div><strong>Refresh</strong> Every 24 Hours</div>
    <div><strong>Access</strong> Role Based</div>
  </div>
</div>
""".strip()

def risk_legend_markdown() -> str:
    """Legend colours exactly match GeoJSON risk fill colours."""
    return """
<div class="pdm-risk-legend">
  <span class="pdm-risk-legend-title">GEOGRAPHIC RISK</span>
  <span class="pdm-risk-chip"><i class="risk-no-data"></i>0 NO DATA</span>
  <span class="pdm-risk-chip"><i class="risk-low"></i>1 LOW</span>
  <span class="pdm-risk-chip"><i class="risk-medium"></i>2 MEDIUM</span>
  <span class="pdm-risk-chip"><i class="risk-high"></i>3 HIGH</span>
  <span class="pdm-risk-chip"><i class="risk-severe"></i>4 SEVERE</span>
</div>
""".strip()


def active_drill_markdown(active_columns: Iterable[str]) -> str:
    labels = [hierarchy_label(column) for column in active_columns]
    active_path = " → ".join(labels) if labels else "No geographic hierarchy available"
    return f"""
<div class="pdm-drill-status">
  <strong>ACTIVE DRILL PATH</strong>
  <span>{active_path}</span>
  <small>Click a row at each level to constrain the level beneath it. The beneficiary view is privacy limited.</small>
</div>
""".strip()


def position_json(
    charts: Iterable[Any],
    active_hierarchy_columns: Iterable[str],
) -> str:
    """Build a focused, editor-safe national executive layout.

    The executive dashboard deliberately contains only one table:
    EXECUTIVE INTERVENTION PRIORITIES. Detailed District -> Beneficiary drill
    tables belong in a separate operational/drilldown dashboard.
    """
    by_name = {c.slice_name: c for c in charts}

    required = [s["title"] for s in KPI_SPECS] + [
        EXECUTIVE_INTERVENTION_CHART,
        AI_AVG_RISK_CHART,
        AI_PRIORITY_CASES_CHART,
        MAP_CHART,
        FUNNEL_CHART,
        TOP_DISTRICTS_CHART,
        RISK_PROFILE_CHART,
        MONTHLY_TREND_CHART,
        MONTHLY_VOLUME_CHART,
        DELIVERY_ASSURANCE_CHART,
    ]
    missing = [name for name in required if name not in by_name]
    if missing:
        raise RuntimeError(f"Missing charts for layout: {missing}")

    kpis = [f"CHART-{by_name[spec['title']].id}" for spec in KPI_SPECS]
    rows = [
        "ROW_EXEC_HEADER",
        "ROW_KPIS",
        "ROW_AI_KPIS",
        "ROW_MAP",
        "ROW_RISK_LEGEND",
        "ROW_DISTRICT_RISK_PROFILE",
        "ROW_INTERVENTION",
        "ROW_FUNDING",
        "ROW_REACH",
        "ROW_EXEC_FOOTER",
    ]

    layout: Dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {
            "id": "GRID_ID",
            "type": "GRID",
            "parents": ["ROOT_ID"],
            "children": rows,
        },
    }

    def row(row_id: str, children: List[str]) -> None:
        layout[row_id] = {
            "id": row_id,
            "type": "ROW",
            "parents": ["ROOT_ID", "GRID_ID"],
            "children": children,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }

    row("ROW_EXEC_HEADER", ["MD_EXEC_HEADER"])
    row("ROW_KPIS", kpis)
    row(
        "ROW_AI_KPIS",
        [
            f"CHART-{by_name[AI_AVG_RISK_CHART].id}",
            f"CHART-{by_name[AI_PRIORITY_CASES_CHART].id}",
        ],
    )
    row("ROW_MAP", ["MD_LEFT_NAV_GEO", f"CHART-{by_name[MAP_CHART].id}"])
    row("ROW_RISK_LEGEND", ["MD_LEFT_NAV_LEGEND", "MD_RISK_LEGEND"])
    row(
        "ROW_DISTRICT_RISK_PROFILE",
        [
            "MD_LEFT_NAV_ANALYSIS",
            f"CHART-{by_name[TOP_DISTRICTS_CHART].id}",
            f"CHART-{by_name[RISK_PROFILE_CHART].id}",
        ],
    )
    row(
        "ROW_INTERVENTION",
        ["MD_LEFT_NAV_TOP", f"CHART-{by_name[EXECUTIVE_INTERVENTION_CHART].id}"],
    )
    row(
        "ROW_FUNDING",
        [
            "MD_LEFT_NAV_TREND",
            f"CHART-{by_name[MONTHLY_TREND_CHART].id}",
            f"CHART-{by_name[FUNNEL_CHART].id}",
        ],
    )
    row(
        "ROW_REACH",
        [
            "MD_LEFT_NAV_BOTTOM",
            f"CHART-{by_name[MONTHLY_VOLUME_CHART].id}",
            f"CHART-{by_name[DELIVERY_ASSURANCE_CHART].id}",
        ],
    )
    row("ROW_EXEC_FOOTER", ["MD_EXEC_FOOTER"])

    layout["MD_EXEC_HEADER"] = markdown_node(
        "MD_EXEC_HEADER", "ROW_EXEC_HEADER", executive_header_markdown(), 12, 9
    )

    for spec in KPI_SPECS:
        chart = by_name[spec["title"]]
        layout[f"CHART-{chart.id}"] = chart_node(chart, "ROW_KPIS", 2, 20)

    ai_avg_chart = by_name[AI_AVG_RISK_CHART]
    ai_priority_chart = by_name[AI_PRIORITY_CASES_CHART]
    layout[f"CHART-{ai_avg_chart.id}"] = chart_node(
        ai_avg_chart, "ROW_AI_KPIS", 6, 18
    )
    layout[f"CHART-{ai_priority_chart.id}"] = chart_node(
        ai_priority_chart, "ROW_AI_KPIS", 6, 18
    )

    layout["MD_LEFT_NAV_GEO"] = markdown_node(
        "MD_LEFT_NAV_GEO",
        "ROW_MAP",
        """<div class="pdm-left-nav-segment pdm-left-nav-middle"><div class="pdm-left-nav-heading">GEOGRAPHY</div><div class="pdm-left-nav-item active">Uganda Risk Map</div><div class="pdm-left-nav-item">District Performance</div></div>""",
        2,
        68,
    )
    chart = by_name[MAP_CHART]
    layout[f"CHART-{chart.id}"] = chart_node(chart, "ROW_MAP", 10, 68)

    layout["MD_LEFT_NAV_LEGEND"] = markdown_node(
        "MD_LEFT_NAV_LEGEND",
        "ROW_RISK_LEGEND",
        leadership_nav_legend_markdown(),
        2,
        8,
    )
    layout["MD_RISK_LEGEND"] = markdown_node(
        "MD_RISK_LEGEND",
        "ROW_RISK_LEGEND",
        risk_legend_markdown(),
        10,
        8,
    )

    layout["MD_LEFT_NAV_ANALYSIS"] = markdown_node(
        "MD_LEFT_NAV_ANALYSIS",
        "ROW_DISTRICT_RISK_PROFILE",
        """<div class="pdm-left-nav-segment pdm-left-nav-middle"><div class="pdm-left-nav-heading">RISK</div><div class="pdm-left-nav-item active">District Attention</div><div class="pdm-left-nav-item">National Risk Profile</div></div>""",
        2,
        36,
    )
    chart = by_name[TOP_DISTRICTS_CHART]
    layout[f"CHART-{chart.id}"] = chart_node(chart, "ROW_DISTRICT_RISK_PROFILE", 7, 36)
    chart = by_name[RISK_PROFILE_CHART]
    layout[f"CHART-{chart.id}"] = chart_node(chart, "ROW_DISTRICT_RISK_PROFILE", 3, 36)

    layout["MD_LEFT_NAV_TOP"] = markdown_node(
        "MD_LEFT_NAV_TOP",
        "ROW_INTERVENTION",
        leadership_nav_top_markdown(),
        2,
        38,
    )
    chart = by_name[EXECUTIVE_INTERVENTION_CHART]
    layout[f"CHART-{chart.id}"] = chart_node(chart, "ROW_INTERVENTION", 10, 38)

    layout["MD_LEFT_NAV_TREND"] = markdown_node(
        "MD_LEFT_NAV_TREND",
        "ROW_FUNDING",
        leadership_nav_trend_markdown(),
        2,
        40,
    )
    chart = by_name[MONTHLY_TREND_CHART]
    layout[f"CHART-{chart.id}"] = chart_node(chart, "ROW_FUNDING", 7, 40)
    chart = by_name[FUNNEL_CHART]
    layout[f"CHART-{chart.id}"] = chart_node(chart, "ROW_FUNDING", 3, 40)

    layout["MD_LEFT_NAV_BOTTOM"] = markdown_node(
        "MD_LEFT_NAV_BOTTOM",
        "ROW_REACH",
        leadership_nav_bottom_markdown(),
        2,
        36,
    )
    chart = by_name[MONTHLY_VOLUME_CHART]
    layout[f"CHART-{chart.id}"] = chart_node(chart, "ROW_REACH", 7, 36)
    chart = by_name[DELIVERY_ASSURANCE_CHART]
    layout[f"CHART-{chart.id}"] = chart_node(chart, "ROW_REACH", 3, 36)

    layout["MD_EXEC_FOOTER"] = markdown_node(
        "MD_EXEC_FOOTER", "ROW_EXEC_FOOTER", executive_footer_markdown(), 12, 5
    )
    return json.dumps(layout, separators=(",", ":"))

def pdm_dashboard_css(map_chart_id: int) -> str:
    return f"""
/* ==========================================================================
   PDM National Executive Intelligence
   Uganda leadership dashboard
   ========================================================================== */

:root {{
    --pdm-black: #101418;
    --pdm-navy: #0B1F3A;
    --pdm-navy-2: #102A4C;
    --pdm-navy-3: #173A63;
    --pdm-yellow: #F9C400;
    --pdm-red: #D90000;
    --pdm-border: #D9DEE5;
    --pdm-muted: #667085;
    --pdm-page: #F4F6F9;

    --risk-no-data: #90A4AE;
    --risk-low: #2E7D32;
    --risk-medium: #F9A825;
    --risk-high: #EF6C00;
    --risk-severe: #C62828;
}}

.dashboard-content,
.dashboard-grid {{
    background: var(--pdm-page) !important;
}}

.dashboard-component-row {{
    margin-bottom: 8px !important;
}}

/* --------------------------------------------------------------------------
   Uganda executive header
   -------------------------------------------------------------------------- */

.pdm-executive-header {{
    width: 100%;
    min-height: 92px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 18px;
    padding: 10px 18px;
    box-sizing: border-box;
    border: 1px solid var(--pdm-border);
    border-radius: 12px;
    background: linear-gradient(90deg, #FFFFFF 0%, #FFFFFF 78%, #FFF7D6 100%);
    box-shadow: 0 2px 8px rgba(15, 23, 42, 0.08);
    position: relative;
    overflow: hidden;
}}

.pdm-executive-header::after {{
    content: "";
    position: absolute;
    right: -10px;
    top: 0;
    width: 220px;
    height: 9px;
    background: linear-gradient(
        90deg,
        var(--pdm-black) 0%,
        var(--pdm-black) 33.33%,
        var(--pdm-yellow) 33.33%,
        var(--pdm-yellow) 66.66%,
        var(--pdm-red) 66.66%,
        var(--pdm-red) 100%
    );
}}

.pdm-header-left {{
    display: flex;
    align-items: center;
    gap: 16px;
    min-width: 0;
}}

.pdm-coat-of-arms {{
    width: 74px;
    height: 74px;
    object-fit: contain;
    flex: 0 0 74px;
}}

.pdm-header-copy {{
    min-width: 0;
}}

.pdm-republic-label {{
    color: var(--pdm-black);
    font-size: 12px;
    line-height: 1.1;
    font-weight: 800;
    letter-spacing: 0.08em;
    text-transform: uppercase;
}}

.pdm-main-title {{
    color: var(--pdm-black);
    font-size: clamp(20px, 1.7vw, 28px);
    line-height: 1.08;
    font-weight: 800;
    letter-spacing: -0.015em;
    white-space: normal;
}}

.pdm-main-subtitle {{
    margin-top: 5px;
    color: var(--pdm-red);
    font-size: 14px;
    line-height: 1.1;
    font-weight: 800;
}}

.pdm-header-right {{
    flex: 0 0 auto;
    text-align: right;
    padding-right: 12px;
}}

.pdm-brand-mark {{
    color: var(--pdm-black);
    font-size: 32px;
    line-height: 0.95;
    font-weight: 900;
    letter-spacing: -0.04em;
}}

.pdm-brand-tagline {{
    max-width: 170px;
    margin-top: 5px;
    color: #344054;
    font-size: 10px;
    line-height: 1.15;
    font-weight: 700;
}}

/* --------------------------------------------------------------------------
   Cards
   -------------------------------------------------------------------------- */

.dashboard-component-chart-holder {{
    border-radius: 11px !important;
    border: 1px solid var(--pdm-border) !important;
    background: #FFFFFF !important;
    box-shadow: 0 2px 7px rgba(15, 23, 42, 0.06) !important;
    overflow: visible !important;
    position: relative !important;
}}

.dashboard-component-chart-holder > div,
.dashboard-component-chart-holder .chart-container,
.dashboard-component-chart-holder .slice_container,
.dashboard-component-chart-holder .chart-slice,
.dashboard-component-chart-holder .ant-spin-nested-loading,
.dashboard-component-chart-holder .ant-spin-container {{
    overflow: hidden !important;
    max-height: 100% !important;
}}

/* Medium, bold chart titles. KPI chart names themselves are stored uppercase. */
.dashboard-component-chart-holder .chart-header,
.dashboard-component-chart-holder [class*="ChartHeader"] {{
    width: 100% !important;
}}

.dashboard-component-chart-holder .chart-header .header-title,
.dashboard-component-chart-holder .chart-header a,
.dashboard-component-chart-holder [class*="ChartHeader"] a {{
    width: 100% !important;
    display: block !important;
    text-align: center !important;
    color: var(--pdm-black) !important;
    font-size: 16px !important;
    line-height: 1.15 !important;
    font-weight: 800 !important;
    letter-spacing: 0.025em !important;
    margin-left: auto !important;
    margin-right: auto !important;
}}

/* Superset 6.1 Big Number */
.dashboard-component-chart-holder .superset-legacy-chart-big-number {{
    width: 100% !important;
    height: 100% !important;
    min-height: 0 !important;
    display: flex !important;
    flex-direction: column !important;
    justify-content: center !important;
    align-items: center !important;
    text-align: center !important;
    overflow: hidden !important;
    box-sizing: border-box !important;
}}

.dashboard-component-chart-holder
.superset-legacy-chart-big-number
.text-container {{
    width: 100% !important;
    display: flex !important;
    flex-direction: column !important;
    justify-content: center !important;
    align-items: center !important;
    text-align: center !important;
    padding: 4px 5px 0 5px !important;
    box-sizing: border-box !important;
}}

.dashboard-component-chart-holder
.superset-legacy-chart-big-number
.header-line {{
    width: 100% !important;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
    text-align: center !important;
    align-self: center !important;
    color: var(--pdm-black) !important;
    font-weight: 900 !important;
    font-size: clamp(22px, 2vw, 36px) !important;
    line-height: 1 !important;
    letter-spacing: -0.02em !important;
    margin: 4px auto 5px auto !important;
    white-space: nowrap !important;
}}

.dashboard-component-chart-holder
.superset-legacy-chart-big-number
.subheader-line {{
    width: 100% !important;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
    text-align: center !important;
    align-self: center !important;
    font-weight: 800 !important;
    font-size: clamp(11px, 0.92vw, 15px) !important;
    line-height: 1.1 !important;
    margin: 2px auto 4px auto !important;
    white-space: nowrap !important;
}}

.dashboard-component-chart-holder
.superset-legacy-chart-big-number
.echarts-for-react,
.dashboard-component-chart-holder
.superset-legacy-chart-big-number
canvas,
.dashboard-component-chart-holder
.superset-legacy-chart-big-number
svg {{
    margin-left: auto !important;
    margin-right: auto !important;
}}

/* --------------------------------------------------------------------------
   Left Leadership navigation
   Flat-row implementation: visually continuous, structurally editor-safe.
   -------------------------------------------------------------------------- */

.pdm-left-nav-segment {{
    width: 100%;
    height: 100%;
    min-height: 100%;
    box-sizing: border-box;
    padding: 14px 11px;
    background: linear-gradient(180deg, var(--pdm-navy-2) 0%, var(--pdm-navy) 100%);
    color: #FFFFFF;
    box-shadow: 0 2px 8px rgba(11, 31, 58, 0.16);
}}

.pdm-left-nav-top {{
    border-radius: 12px 12px 4px 4px;
}}

.pdm-left-nav-middle {{
    border-radius: 4px;
}}

.pdm-left-nav-bottom {{
    border-radius: 4px 4px 12px 12px;
}}

.pdm-left-nav-compact {{
    padding-top: 8px;
    padding-bottom: 8px;
}}

.pdm-left-nav-brand {{
    padding: 4px 4px 12px 4px;
    color: #FFFFFF;
    font-size: 17px;
    line-height: 1;
    font-weight: 900;
    letter-spacing: 0.07em;
    border-bottom: 3px solid var(--pdm-yellow);
}}

.pdm-left-nav-section {{
    margin-top: 15px;
}}

.pdm-left-nav-section-first {{
    margin-top: 2px;
}}

.pdm-left-nav-heading {{
    margin-bottom: 6px;
    color: #9CC5F5;
    font-size: 11.5px;
    line-height: 1.15;
    font-weight: 900;
    letter-spacing: 0.09em;
}}

.pdm-left-nav-item {{
    margin: 2px 0;
    padding: 6px 7px;
    border-radius: 7px;
    color: #E7EEF7;
    font-size: 12.5px;
    line-height: 1.2;
    font-weight: 700;
}}

.pdm-left-nav-item.active {{
    background: rgba(252, 209, 22, 0.16);
    color: #FFFFFF;
    font-weight: 850;
    box-shadow: inset 4px 0 0 var(--pdm-yellow);
}}

.pdm-left-nav-meta {{
    margin-top: 16px;
    padding-top: 11px;
    border-top: 1px solid rgba(255, 255, 255, 0.22);
    color: #C5D4E5;
    font-size: 9px;
    line-height: 1.6;
}}

/*
Do NOT use fixed/sticky/absolute positioning for the navigation.
It must remain a normal dashboard component so it never overlays chart
selection handles in Superset Edit mode.
*/

/* --------------------------------------------------------------------------
   Coloured geographic-risk legend
   -------------------------------------------------------------------------- */

.pdm-risk-legend {{
    width: 100%;
    min-height: 40px;
    display: flex;
    align-items: center;
    justify-content: center;
    flex-wrap: wrap;
    gap: 10px 16px;
    padding: 7px 10px;
    box-sizing: border-box;
    border: 1px solid var(--pdm-border);
    border-radius: 9px;
    background: #FFFFFF;
    box-shadow: 0 1px 4px rgba(15, 23, 42, 0.05);
}}

.pdm-risk-legend-title {{
    color: var(--pdm-navy);
    font-size: 10px;
    font-weight: 900;
    letter-spacing: 0.06em;
}}

.pdm-risk-chip {{
    display: inline-flex;
    align-items: center;
    gap: 5px;
    color: #344054;
    font-size: 10px;
    line-height: 1;
    font-weight: 800;
    white-space: nowrap;
}}

.pdm-risk-chip i {{
    display: inline-block;
    width: 13px;
    height: 13px;
    border-radius: 3px;
    border: 1px solid rgba(0, 0, 0, 0.16);
}}

.risk-no-data {{ background: var(--risk-no-data); }}
.risk-low {{ background: var(--risk-low); }}
.risk-medium {{ background: var(--risk-medium); }}
.risk-high {{ background: var(--risk-high); }}
.risk-severe {{ background: var(--risk-severe); }}

.dashboard-component-chart-holder:has(.pdm-risk-legend) {{
    border: none !important;
    background: transparent !important;
    box-shadow: none !important;
    padding: 0 !important;
}}

/* --------------------------------------------------------------------------
   Executive drill status
   -------------------------------------------------------------------------- */

.pdm-drill-status {{
    width: 100%;
    min-height: 40px;
    display: flex;
    flex-direction: column;
    justify-content: center;
    gap: 3px;
    padding: 7px 10px;
    box-sizing: border-box;
    border-radius: 9px;
    background: #EAF2FB;
    border: 1px solid #B8CCE3;
    color: var(--pdm-navy);
}}

.pdm-drill-status strong {{
    font-size: 9px;
    letter-spacing: 0.07em;
}}

.pdm-drill-status span {{
    font-size: 12px;
    font-weight: 850;
}}

.pdm-drill-status small {{
    color: #52677F;
    font-size: 8.5px;
    line-height: 1.15;
}}

.dashboard-component-chart-holder:has(.pdm-drill-status) {{
    border: none !important;
    background: transparent !important;
    box-shadow: none !important;
    padding: 0 !important;
}}

/* --------------------------------------------------------------------------
   Footer
   -------------------------------------------------------------------------- */

.pdm-executive-footer {{
    width: 100%;
    min-height: 48px;
    display: grid;
    grid-template-columns: 1fr 1.6fr 1fr;
    align-items: center;
    gap: 12px;
    padding: 8px 16px;
    box-sizing: border-box;
    border-radius: 10px;
    background: var(--pdm-navy);
    color: #FFFFFF;
    font-size: 10px;
    line-height: 1.2;
}}

.pdm-executive-footer > div:nth-child(2) {{
    text-align: center;
    color: #D0DCE9;
}}

.pdm-footer-tagline {{
    text-align: right;
    color: var(--pdm-yellow);
    font-weight: 800;
}}

/* Remove internal scrollbars. */
.dashboard-component-chart-holder * {{
    scrollbar-width: none !important;
}}

.dashboard-component-chart-holder *::-webkit-scrollbar {{
    width: 0 !important;
    height: 0 !important;
    display: none !important;
}}

@media (max-width: 1100px) {{
    .pdm-main-title {{
        font-size: 20px;
    }}

    .pdm-coat-of-arms {{
        width: 58px;
        height: 58px;
        flex-basis: 58px;
    }}

    .pdm-left-nav-item {{
        font-size: 12px;
        padding: 6px 6px;
    }}
}}
"""


def native_filter_configuration(
    *,
    filter_dataset: Any,
    active_hierarchy_columns: Iterable[str],
    charts: Iterable[Any],
    geography_chart_names: Iterable[str],
) -> List[Dict[str, Any]]:
    """
    Build cascading Superset native geographic filters.

    Filters are scoped only to geographic charts/datasets so they cannot break
    national KPI charts whose datasets do not expose geographic columns.
    """
    active_hierarchy_columns = list(active_hierarchy_columns)
    charts = list(charts)
    geography_chart_names = set(geography_chart_names)

    included_chart_ids = {
        chart.id for chart in charts
        if chart.slice_name in geography_chart_names
    }
    excluded_chart_ids = [
        chart.id for chart in charts
        if chart.id not in included_chart_ids
    ]

    filters: List[Dict[str, Any]] = []
    parent_ids: List[str] = []

    for column in active_hierarchy_columns:
        filter_id = f"NATIVE_FILTER-pdm-{column.replace('_', '-')}"
        config = {
            "id": filter_id,
            "controlValues": {
                "enableEmptyFilter": False,
                "defaultToFirstItem": False,
                "creatable": False,
                "multiSelect": True,
                "searchAllOptions": True,
                "inverseSelection": False,
            },
            "name": hierarchy_label(column),
            "filterType": "filter_select",
            "targets": [
                {
                    "datasetId": filter_dataset.id,
                    "column": {"name": column},
                }
            ],
            "defaultDataMask": {
                "extraFormData": {},
                "filterState": {},
                "ownState": {},
            },
            "cascadeParentIds": list(parent_ids),
            "scope": {
                "rootPath": ["ROOT_ID"],
                "excluded": excluded_chart_ids,
            },
            "type": "NATIVE_FILTER",
            "description": (
                f"PDM executive geographic drill filter: "
                f"{hierarchy_label(column)}"
            ),
        }
        filters.append(config)
        parent_ids.append(filter_id)

    return filters


def chart_cross_filter_configuration(
    charts: Iterable[Any],
    *,
    enabled_chart_names: Iterable[str],
) -> Dict[str, Any]:
    """Enable click-driven cross-filter metadata only for supported intelligence charts."""
    enabled_chart_names = set(enabled_chart_names)
    configuration: Dict[str, Any] = {}

    for chart in charts:
        if chart.slice_name not in enabled_chart_names:
            continue

        configuration[str(chart.id)] = {
            "id": chart.id,
            "crossFilters": {
                "scope": {
                    "rootPath": ["ROOT_ID"],
                    "excluded": [chart.id],
                },
                "chartsInScope": [],
            },
        }

    return configuration


def update_dashboard() -> None:
    app = create_app()
    with app.app_context():
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        session = db.session
        owner_user = resolve_owner_user()
        dashboard = dashboard_object(
            session,
            Dashboard,
            owner_user,
        )

        database = trino_database(session, Database)
        snapshot = executive_snapshot(database)
        previous_month_label = snapshot["previous_month_label"]

        overview = physical_dataset(
            session,
            SqlaTable,
            database=database,
            table_name=EXECUTIVE_OVERVIEW,
        )
        monthly = physical_dataset(
            session,
            SqlaTable,
            database=database,
            table_name=EXECUTIVE_MONTHLY_TREND,
        )
        district = physical_dataset(
            session,
            SqlaTable,
            database=database,
            table_name=DISTRICT_RISK,
        )
        executive_ai = physical_dataset(
            session,
            SqlaTable,
            database=database,
            table_name=EXECUTIVE_AI_RISK,
        )
        intervention_ds = physical_dataset(
            session,
            SqlaTable,
            database=database,
            table_name=EXECUTIVE_INTERVENTION_MODEL,
        )

        required_overview = {
            "kpi_reporting_month", "kpi_previous_month",
            "current_month_total_approved_amount", "previous_month_total_approved_amount",
            "current_month_total_settled_amount", "previous_month_total_settled_amount",
            "current_month_total_credited_amount", "previous_month_total_credited_amount",
            "current_month_total_disbursed_amount", "previous_month_total_disbursed_amount",
            "current_month_total_outstanding_amount", "previous_month_total_outstanding_amount",
            "current_month_high_risk_case_count", "previous_month_high_risk_case_count",
            "repayment_rate", "reconciliation_rate", "payment_success_rate",
            "high_risk_case_count_mom_pct",
        }
        missing = required_overview - column_names(overview)
        if missing:
            raise RuntimeError(f"{EXECUTIVE_OVERVIEW} missing: {sorted(missing)}")

        required_monthly = {
            "reporting_month", "approved_amount", "previous_approved_amount", "approved_amount_mom_pct",
            "disbursed_amount", "previous_disbursed_amount", "disbursed_amount_mom_pct",
            "repaid_amount", "previous_repaid_amount", "repaid_amount_mom_pct",
            "current_outstanding_amount", "previous_outstanding_amount", "outstanding_amount_mom_pct",
            "approved_beneficiary_count", "previous_beneficiary_count", "beneficiary_count_mom_pct",
            "approved_loan_count",
        }
        missing = required_monthly - column_names(monthly)
        if missing:
            raise RuntimeError(f"{EXECUTIVE_MONTHLY_TREND} missing: {sorted(missing)}")
        mark_temporal(monthly, "reporting_month", session)

        if "kpi_reporting_month" in column_names(overview):
            mark_temporal(overview, "kpi_reporting_month", session)
        if "kpi_previous_month" in column_names(overview):
            mark_temporal(overview, "kpi_previous_month", session)

        required_district = {"district", "superset_district_iso", "map_risk_score", "geographic_risk_band", "geometry_status", "geojson"}
        missing = required_district - column_names(district)
        if missing:
            raise RuntimeError(f"{DISTRICT_RISK} missing: {sorted(missing)}")

        required_executive_ai = {
            "observation_date",
            "ai_scored_loan_count",
            "avg_probability_default_90d",
            "ai_high_risk_count",
            "ai_severe_risk_count",
            "ai_priority_review_count",
            "ai_priority_review_rate",
            "model_name",
            "model_version",
        }
        missing = required_executive_ai - column_names(executive_ai)
        if missing:
            raise RuntimeError(
                f"{EXECUTIVE_AI_RISK} missing: {sorted(missing)}"
            )

        required_intervention = {
            "region",
            "district",
            "geographic_risk_band",
            "ai_intervention_band",
            "avg_probability_default_90d",
            "ai_priority_review_count",
            "ai_severe_risk_count",
            "outstanding_amount",
            "intervention_priority_score",
            "recommended_action",
            "intervention_rank",
            "interpretation",
        }
        missing = required_intervention - column_names(intervention_ds)
        if missing:
            raise RuntimeError(
                f"{EXECUTIVE_INTERVENTION_MODEL} missing: {sorted(missing)}"
            )

        map_hierarchy_columns = available_hierarchy_columns(district)

        # Executive dashboard filters stay deliberately high level. Detailed
        # County -> Beneficiary drill is handled by a separate dashboard.
        active_hierarchy_columns = [
            column for column in ("region", "district")
            if column in column_names(district)
        ]

        kpi_datasets: Dict[str, Any] = {}
        for spec in KPI_SPECS:
            kpi_datasets[spec["title"]] = virtual_dataset(
                session, SqlaTable,
                name=f"vds_pdm_kpi_{spec['slug']}_mom",
                sql=kpi_sql(spec),
                base=(monthly if spec["source"] == "monthly" else overview),
                temporal="reporting_month",
            )
        funnel_ds = virtual_dataset(
            session, SqlaTable,
            name="vds_pdm_fund_flow_funnel",
            sql=funnel_sql(),
            base=overview,
        )

        map_ds = virtual_dataset(
            session,
            SqlaTable,
            name=GEOJSON_MAP_DATASET,
            sql=geojson_map_sql(map_hierarchy_columns),
            base=district,
        )

        top_districts_ds = virtual_dataset(
            session,
            SqlaTable,
            name=TOP_DISTRICTS_DATASET,
            sql=top_districts_sql(),
            base=intervention_ds,
        )

        risk_profile_ds = virtual_dataset(
            session,
            SqlaTable,
            name=RISK_PROFILE_DATASET,
            sql=risk_profile_sql(),
            base=district,
        )

        delivery_assurance_ds = virtual_dataset(
            session,
            SqlaTable,
            name=DELIVERY_ASSURANCE_DATASET,
            sql=delivery_assurance_sql(),
            base=overview,
        )

        map_required = {
            "district",
            "region",
            "geographic_risk_band",
            "map_risk_score",
            "geometry_status",
            "risk_fill_color",
            "risk_stroke_color",
            "geojson",
        }
        missing = map_required - column_names(map_ds)
        if missing:
            raise RuntimeError(
                f"{GEOJSON_MAP_DATASET} missing: {sorted(missing)}"
            )

        charts: List[Any] = []
        for spec in KPI_SPECS:
            charts.append(upsert_chart(
                session, Slice,
                name=spec["title"],
                dataset=kpi_datasets[spec["title"]],
                viz_type="big_number",
                params=big_number_params(
                    spec["title"],
                    spec["currency"],
                    spec["increase_is_good"],
                    comparison_text(
                        snapshot.get(spec["pct"]),
                        previous_month_label,
                        increase_is_good=spec["increase_is_good"],
                    ),
                ),
                owner_user=owner_user,
                aliases=spec.get("aliases"),
                description=f"{spec['title']} with one-month comparison.",
            ))

        charts.append(
            upsert_chart(
                session,
                Slice,
                name=AI_AVG_RISK_CHART,
                dataset=executive_ai,
                viz_type="big_number",
                params=ai_big_number_params(
                    "avg_probability_default_90d",
                    AI_AVG_RISK_CHART,
                    percentage=True,
                ),
                owner_user=owner_user,
                aliases=["AVERAGE AI DEFAULT RISK"],
                description=(
                    "Average XGBoost-predicted probability of default within "
                    "90 days. Predictive default risk is not a fraud determination."
                ),
            )
        )

        charts.append(
            upsert_chart(
                session,
                Slice,
                name=AI_PRIORITY_CASES_CHART,
                dataset=executive_ai,
                viz_type="big_number",
                params=ai_big_number_params(
                    "ai_priority_review_count",
                    AI_PRIORITY_CASES_CHART,
                ),
                owner_user=owner_user,
                aliases=["AI HIGH / SEVERE CASES"],
                description=(
                    "Loans classified HIGH or SEVERE by the persisted XGBoost "
                    "90-day default-risk model."
                ),
            )
        )

        charts.append(
            upsert_chart(
                session,
                Slice,
                name=EXECUTIVE_INTERVENTION_CHART,
                dataset=intervention_ds,
                viz_type="table",
                params=intervention_table_params(map_hierarchy_columns),
                owner_user=owner_user,
                aliases=["Executive Intervention Priorities"],
                description=(
                    "Combined geographic and XGBoost predictive-default risk ranking "
                    "for executive intervention and prioritisation."
                ),
            )
        )

        charts.append(
            upsert_chart(
                session,
                Slice,
                name=TOP_DISTRICTS_CHART,
                dataset=top_districts_ds,
                viz_type="echarts_timeseries_bar",
                params=top_districts_params(),
                owner_user=owner_user,
                aliases=["Top Districts Requiring Attention"],
                description=(
                    "Top districts ranked by the combined intervention priority score "
                    "from geographic risk and AI predicted default risk."
                ),
            )
        )

        charts.append(
            upsert_chart(
                session,
                Slice,
                name=RISK_PROFILE_CHART,
                dataset=risk_profile_ds,
                viz_type="pie",
                params=risk_profile_params(),
                owner_user=owner_user,
                aliases=["National Risk Profile"],
                description="National composition of districts by PDM geographic risk band.",
            )
        )

        charts.append(
            upsert_chart(
                session,
                Slice,
                name=DELIVERY_ASSURANCE_CHART,
                dataset=delivery_assurance_ds,
                viz_type="echarts_timeseries_bar",
                params=delivery_assurance_params(),
                owner_user=owner_user,
                aliases=["Delivery & Assurance"],
                description=(
                    "Compact delivery and assurance rates for repayment, "
                    "reconciliation and payment success."
                ),
            )
        )

        charts.append(
            upsert_chart(
                session,
                Slice,
                name=MAP_CHART,
                dataset=map_ds,
                viz_type="deck_geojson",
                params=map_params(),
                owner_user=owner_user,
                aliases=["PDM National Geographic Risk Map"],
                description=(
                    "Uganda district-level PDM geographic risk map rendered "
                    "with Deck.gl GeoJSON using only districts with matched geometry."
                ),
            )
        )
        charts.append(upsert_chart(session, Slice, name=FUNNEL_CHART, dataset=funnel_ds, viz_type="funnel", params=funnel_params(), owner_user=owner_user, aliases=["PDM Fund Flow"], description="Approved → Settled → Credited → Disbursed."))
        charts.append(upsert_chart(session, Slice, name=MONTHLY_TREND_CHART, dataset=monthly, viz_type="echarts_timeseries_line", params=monthly_trend_params(), owner_user=owner_user, aliases=["PDM Monthly Funding Trend"], description="Monthly approved, disbursed and repaid funding movement."))
        charts.append(upsert_chart(session, Slice, name=MONTHLY_VOLUME_CHART, dataset=monthly, viz_type="echarts_timeseries_bar", params=monthly_volume_params(), owner_user=owner_user, aliases=["PDM Monthly Beneficiary & Loan Volume"], description="Monthly programme reach comparing approved beneficiaries and approved loans."))

        dashboard.position_json = position_json(
            charts,
            active_hierarchy_columns,
        )
        dashboard.slices = charts

        map_chart_id = next(
            chart.id for chart in charts
            if chart.slice_name == MAP_CHART
        )
        dashboard.css = pdm_dashboard_css(map_chart_id)

        geography_chart_names = {
            MAP_CHART,
            EXECUTIVE_INTERVENTION_CHART,
            TOP_DISTRICTS_CHART,
        }

        native_filters = native_filter_configuration(
            filter_dataset=district,
            active_hierarchy_columns=active_hierarchy_columns,
            charts=charts,
            geography_chart_names=geography_chart_names,
        )

        chart_configuration = chart_cross_filter_configuration(
            charts,
            enabled_chart_names={
                MAP_CHART,
                EXECUTIVE_INTERVENTION_CHART,
                TOP_DISTRICTS_CHART,
            },
        )

        dashboard.json_metadata = json.dumps({
            "timed_refresh_immune_slices": [],
            "expanded_slices": {},
            "refresh_frequency": 0,
            "default_filters": "{}",
            "label_colors": RISK_COLORS,
            "chart_configuration": chart_configuration,
            "native_filter_configuration": native_filters,
            "cross_filters_enabled": True,
            "filter_bar_orientation": "HORIZONTAL",
        })
        session.commit()

        print("=" * 80)
        print(f"Updated dashboard {dashboard.id}: {dashboard.dashboard_title}")
        print(f"Charts managed: {len(charts)}")
        print(f"6 operational KPI cards + 2 AI KPI cards; operational comparison vs {previous_month_label}")
        print(f"AI executive model: {EXECUTIVE_AI_RISK}")
        print(f"Combined intervention model: {EXECUTIVE_INTERVENTION_MODEL}")
        print(f"Executive intervention panel: {EXECUTIVE_INTERVENTION_CHART}")
        print(f"Ranked district chart: {TOP_DISTRICTS_CHART} (geographic + AI priority)")
        print(f"Risk composition chart: {RISK_PROFILE_CHART}")
        print(f"Delivery assurance chart: {DELIVERY_ASSURANCE_CHART}")
        print(
            "Executive geographic filters: "
            + (" -> ".join(hierarchy_label(c) for c in active_hierarchy_columns) or "none")
        )
        print("Executive table count: 1 (EXECUTIVE INTERVENTION PRIORITIES)")
        print("Detailed District -> Beneficiary drill tables are intentionally excluded")
        print("Modern charts: Funnel, ECharts Line, ECharts Bar, Risk Profile")
        print("Executive design: focused national overview + one action table")
        print(f"Map: Deck.gl GeoJSON using {GEOJSON_MAP_DATASET}.geojson")
        print("Risk colours: 0 GREY | 1 GREEN | 2 YELLOW | 3 ORANGE | 4 RED")
        print("Map safety: NULL/blank GeoJSON rows excluded before Deck.gl query")
        print("=" * 80)


if __name__ == "__main__":
    update_dashboard()
