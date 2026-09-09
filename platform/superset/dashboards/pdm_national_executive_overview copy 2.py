#!/usr/bin/env python3
"""Build/update the PDM National Executive Intelligence dashboard for Superset 6.1."""
from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Optional

from superset.app import create_app

DASHBOARD_TITLE = "PDM National Executive Intelligence"
DASHBOARD_SLUG = "pdm-national-executive-intelligence"
SUPERSET_OWNER_USERNAME = "admin"
SCHEMA = "consumption"
EXECUTIVE_OVERVIEW = "cns_pdm_executive_overview"
EXECUTIVE_MONTHLY_TREND = "cns_pdm_executive_monthly_trend"
DISTRICT_RISK = "cns_pdm_district_geojson_risk"
GEOJSON_MAP_DATASET = "vds_pdm_district_geojson_map"

# Uganda national branding used by the executive header.
# Wikimedia Commons redirect is stable and avoids hard-coding a hashed upload path.
UGANDA_COAT_OF_ARMS_URL = (
    "/static/assets/images/Coat-of-arms-Uganda.webp"
)
EXECUTIVE_SUBTITLE = "National PDM Performance, Delivery and Risk Intelligence"
EXECUTIVE_TAGLINE = "Transforming Subsistence into Prosperity"

MAP_CHART = "PDM National Geographic Risk Map"
FUNNEL_CHART = "PDM Fund Flow"
MONTHLY_TREND_CHART = "PDM Monthly Funding Trend"
MONTHLY_VOLUME_CHART = "PDM Monthly Beneficiary & Loan Volume"
REPAYMENT_GAUGE = "Repayment Rate"
RECONCILIATION_GAUGE = "Payment Reconciliation Rate"
PAYMENT_SUCCESS_GAUGE = "Payment Success Rate"

KPI_SPECS: List[Dict[str, Any]] = [
    {
        "title": "TOTAL APPROVED",
        "legacy_title": "Total Approved",
        "slug": "approved",
        "current": "current_month_total_approved_amount",
        "previous": "previous_month_total_approved_amount",
        "currency": True,
        "increase_is_good": True,
        "pct": "total_approved_amount_mom_pct",
    },
    {
        "title": "TOTAL SETTLED",
        "legacy_title": "Total Settled",
        "slug": "settled",
        "current": "current_month_total_settled_amount",
        "previous": "previous_month_total_settled_amount",
        "currency": True,
        "increase_is_good": True,
        "pct": "total_settled_amount_mom_pct",
    },
    {
        "title": "TOTAL CREDITED",
        "legacy_title": "Total Credited",
        "slug": "credited",
        "current": "current_month_total_credited_amount",
        "previous": "previous_month_total_credited_amount",
        "currency": True,
        "increase_is_good": True,
        "pct": "total_credited_amount_mom_pct",
    },
    {
        "title": "TOTAL DISBURSED",
        "legacy_title": "Total Disbursed",
        "slug": "disbursed",
        "current": "current_month_total_disbursed_amount",
        "previous": "previous_month_total_disbursed_amount",
        "currency": True,
        "increase_is_good": True,
        "pct": "total_disbursed_amount_mom_pct",
    },
    {
        "title": "TOTAL OUTSTANDING",
        "legacy_title": "Total Outstanding",
        "slug": "outstanding",
        "current": "current_month_total_outstanding_amount",
        "previous": "previous_month_total_outstanding_amount",
        "currency": True,
        "increase_is_good": False,
        "pct": "total_outstanding_amount_mom_pct",
    },
    {
        "title": "HIGH RISK CASES",
        "legacy_title": "High Risk Cases",
        "slug": "high_risk_cases",
        "current": "current_month_high_risk_case_count",
        "previous": "previous_month_high_risk_case_count",
        "currency": False,
        "increase_is_good": False,
        "pct": "high_risk_case_count_mom_pct",
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
    """
    Read the single-row executive overview directly through Superset's Trino
    connection so KPI subtitles can show exact arrows and the actual prior
    reporting month (for example: ▼ 6.3% vs AUG).
    """
    from sqlalchemy import text as sql_text

    sql = f"""
    SELECT
        kpi_reporting_month,
        kpi_previous_month,

        total_approved_amount_mom_pct,
        total_settled_amount_mom_pct,
        total_credited_amount_mom_pct,
        total_disbursed_amount_mom_pct,
        total_outstanding_amount_mom_pct,
        high_risk_case_count_mom_pct

    FROM {SCHEMA}.{EXECUTIVE_OVERVIEW}
    LIMIT 1
    """

    try:
        with database.get_sqla_engine(schema=SCHEMA) as engine:
            with engine.connect() as connection:
                row = connection.execute(sql_text(sql)).mappings().first()
    except TypeError:
        # Compatibility fallback for Superset builds whose get_sqla_engine()
        # does not accept schema as a keyword argument.
        with database.get_sqla_engine() as engine:
            with engine.connect() as connection:
                row = connection.execute(sql_text(sql)).mappings().first()

    if row is None:
        raise RuntimeError(
            f"{SCHEMA}.{EXECUTIVE_OVERVIEW} returned no executive snapshot row"
        )

    snapshot = dict(row)

    previous_month = snapshot.get("kpi_previous_month")
    if previous_month is not None:
        try:
            snapshot["previous_month_label"] = previous_month.strftime("%b").upper()
        except AttributeError:
            # Trino/driver may return an ISO date string.
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


def geojson_map_sql() -> str:
    """
    Return only renderable district geometry and enrich each GeoJSON Feature
    with deterministic risk colours.

    Superset 6.1's Deck.gl GeoJSON layer recognises `fillColor` / `strokeColor`
    feature properties. The chart's global colour pickers are configured with
    alpha=0 so these feature colours are not overridden.
    """
    return f"""
WITH risk_source AS (
    SELECT
        region,
        district,
        superset_district_iso,

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
    superset_district_iso,

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
            '"strokeColor":"', risk_stroke_color, '",'
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
        "show_trend_line": True,
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


def map_params() -> Dict[str, Any]:
    """
    Superset 6.1 Deck.gl GeoJSON configuration.

    Setting the global fill/stroke picker alpha to zero is intentional:
    Superset then preserves each GeoJSON Feature's own fillColor/strokeColor.
    """
    return {
        "geojson": "geojson",
        "row_limit": 500,
        "adhoc_filters": [],

        "tooltip_contents": [
            "district",
            "region",
            "geographic_risk_band",
            "map_risk_score",
            "beneficiary_count",
            "loan_count",
            "approved_amount",
            "disbursed_amount",
            "outstanding_amount",
        ],

        "autozoom": True,

        # Do not override feature-specific risk colours.
        "fill_color_picker": {"r": 0, "g": 0, "b": 0, "a": 0},
        "stroke_color_picker": {"r": 255, "g": 255, "b": 255, "a": 0},

        "filled": True,
        "stroked": True,
        "extruded": False,
        "line_width": 1,
        "line_width_unit": "pixels",

        "enable_labels": True,
        "label_property_name": "district",
        "label_size": 11,
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
            metric("current_outstanding_amount", "Outstanding"),
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
  <div>National funding • delivery • geographic risk • payment performance</div>
  <div class="pdm-footer-tagline">Transforming Subsistence into Prosperity</div>
</div>
""".strip()


def leadership_navigation_markdown() -> str:
    """Single continuous left-hand executive navigation rail."""
    return """
<div class="pdm-left-nav">
  <div class="pdm-left-nav-brand">LEADERSHIP</div>

  <div class="pdm-left-nav-section">
    <div class="pdm-left-nav-heading">EXECUTIVE</div>
    <div class="pdm-left-nav-item active">Executive Overview</div>
    <div class="pdm-left-nav-item">National Performance</div>
    <div class="pdm-left-nav-item">Financial Position</div>
    <div class="pdm-left-nav-item">Delivery &amp; Repayment</div>
  </div>

  <div class="pdm-left-nav-section">
    <div class="pdm-left-nav-heading">GEOGRAPHY</div>
    <div class="pdm-left-nav-item">Region</div>
    <div class="pdm-left-nav-item">District</div>
    <div class="pdm-left-nav-item">County</div>
    <div class="pdm-left-nav-item">Sub County</div>
    <div class="pdm-left-nav-item">Parish</div>
    <div class="pdm-left-nav-item">Village</div>
  </div>

  <div class="pdm-left-nav-section">
    <div class="pdm-left-nav-heading">PEOPLE</div>
    <div class="pdm-left-nav-item">Households</div>
    <div class="pdm-left-nav-item">Beneficiaries</div>
  </div>

  <div class="pdm-left-nav-section">
    <div class="pdm-left-nav-heading">RISK &amp; ASSURANCE</div>
    <div class="pdm-left-nav-item">Geographic Risk</div>
    <div class="pdm-left-nav-item">Payment Risk</div>
    <div class="pdm-left-nav-item">Reconciliation</div>
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


def position_json(charts: Iterable[Any]) -> str:
    by_name = {c.slice_name: c for c in charts}

    required = [s["title"] for s in KPI_SPECS] + [
        MAP_CHART,
        FUNNEL_CHART,
        MONTHLY_TREND_CHART,
        MONTHLY_VOLUME_CHART,
        REPAYMENT_GAUGE,
        RECONCILIATION_GAUGE,
        PAYMENT_SUCCESS_GAUGE,
    ]

    missing = [name for name in required if name not in by_name]
    if missing:
        raise RuntimeError(f"Missing charts for layout: {missing}")

    kpis = [f"CHART-{by_name[s['title']].id}" for s in KPI_SPECS]

    body_column_parents = [
        "ROOT_ID",
        "GRID_ID",
        "ROW_BODY",
        "COLUMN_ANALYTICS",
    ]

    layout: Dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",

        "ROOT_ID": {
            "id": "ROOT_ID",
            "type": "ROOT",
            "children": ["GRID_ID"],
        },

        "GRID_ID": {
            "id": "GRID_ID",
            "type": "GRID",
            "parents": ["ROOT_ID"],
            "children": [
                "ROW_EXEC_HEADER",
                "ROW_KPIS",
                "ROW_BODY",
                "ROW_EXEC_FOOTER",
            ],
        },

        "ROW_EXEC_HEADER": {
            "id": "ROW_EXEC_HEADER",
            "type": "ROW",
            "parents": ["ROOT_ID", "GRID_ID"],
            "children": ["MD_EXEC_HEADER"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },

        "ROW_KPIS": {
            "id": "ROW_KPIS",
            "type": "ROW",
            "parents": ["ROOT_ID", "GRID_ID"],
            "children": kpis,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },

        "ROW_BODY": {
            "id": "ROW_BODY",
            "type": "ROW",
            "parents": ["ROOT_ID", "GRID_ID"],
            "children": ["MD_LEFT_NAV", "COLUMN_ANALYTICS"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },

        "COLUMN_ANALYTICS": {
            "id": "COLUMN_ANALYTICS",
            "type": "COLUMN",
            "parents": ["ROOT_ID", "GRID_ID", "ROW_BODY"],
            "children": [
                "ROW_MAP_FUNNEL",
                "ROW_RISK_LEGEND",
                "ROW_TREND_GAUGE",
                "ROW_OPERATIONS",
            ],
            "meta": {
                "width": 10,
                "background": "BACKGROUND_TRANSPARENT",
            },
        },

        "ROW_MAP_FUNNEL": {
            "id": "ROW_MAP_FUNNEL",
            "type": "ROW",
            "parents": body_column_parents,
            "children": [
                f"CHART-{by_name[MAP_CHART].id}",
                f"CHART-{by_name[FUNNEL_CHART].id}",
            ],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },

        "ROW_RISK_LEGEND": {
            "id": "ROW_RISK_LEGEND",
            "type": "ROW",
            "parents": body_column_parents,
            "children": ["MD_RISK_LEGEND"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },

        "ROW_TREND_GAUGE": {
            "id": "ROW_TREND_GAUGE",
            "type": "ROW",
            "parents": body_column_parents,
            "children": [
                f"CHART-{by_name[MONTHLY_TREND_CHART].id}",
                f"CHART-{by_name[REPAYMENT_GAUGE].id}",
            ],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },

        "ROW_OPERATIONS": {
            "id": "ROW_OPERATIONS",
            "type": "ROW",
            "parents": body_column_parents,
            "children": [
                f"CHART-{by_name[MONTHLY_VOLUME_CHART].id}",
                f"CHART-{by_name[RECONCILIATION_GAUGE].id}",
                f"CHART-{by_name[PAYMENT_SUCCESS_GAUGE].id}",
            ],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },

        "ROW_EXEC_FOOTER": {
            "id": "ROW_EXEC_FOOTER",
            "type": "ROW",
            "parents": ["ROOT_ID", "GRID_ID"],
            "children": ["MD_EXEC_FOOTER"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
    }

    layout["MD_EXEC_HEADER"] = markdown_node(
        "MD_EXEC_HEADER",
        "ROW_EXEC_HEADER",
        executive_header_markdown(),
        12,
        9,
    )

    # All six KPI cards in one row: 6 x width 2 = full 12-column grid.
    for spec in KPI_SPECS:
        chart = by_name[spec["title"]]
        layout[f"CHART-{chart.id}"] = chart_node(
            chart,
            "ROW_KPIS",
            2,
            20,
        )

    layout["MD_LEFT_NAV"] = markdown_node(
        "MD_LEFT_NAV",
        "ROW_BODY",
        leadership_navigation_markdown(),
        2,
        140,
    )

    nested_row_prefix = [
        "ROOT_ID",
        "GRID_ID",
        "ROW_BODY",
        "COLUMN_ANALYTICS",
    ]

    chart = by_name[MAP_CHART]
    layout[f"CHART-{chart.id}"] = chart_node(
        chart,
        "ROW_MAP_FUNNEL",
        8,
        56,
        parents=nested_row_prefix + ["ROW_MAP_FUNNEL"],
    )

    chart = by_name[FUNNEL_CHART]
    layout[f"CHART-{chart.id}"] = chart_node(
        chart,
        "ROW_MAP_FUNNEL",
        4,
        56,
        parents=nested_row_prefix + ["ROW_MAP_FUNNEL"],
    )

    layout["MD_RISK_LEGEND"] = markdown_node(
        "MD_RISK_LEGEND",
        "ROW_RISK_LEGEND",
        risk_legend_markdown(),
        12,
        6,
        parents=nested_row_prefix + ["ROW_RISK_LEGEND"],
    )

    chart = by_name[MONTHLY_TREND_CHART]
    layout[f"CHART-{chart.id}"] = chart_node(
        chart,
        "ROW_TREND_GAUGE",
        8,
        38,
        parents=nested_row_prefix + ["ROW_TREND_GAUGE"],
    )

    chart = by_name[REPAYMENT_GAUGE]
    layout[f"CHART-{chart.id}"] = chart_node(
        chart,
        "ROW_TREND_GAUGE",
        4,
        38,
        parents=nested_row_prefix + ["ROW_TREND_GAUGE"],
    )

    chart = by_name[MONTHLY_VOLUME_CHART]
    layout[f"CHART-{chart.id}"] = chart_node(
        chart,
        "ROW_OPERATIONS",
        6,
        34,
        parents=nested_row_prefix + ["ROW_OPERATIONS"],
    )

    chart = by_name[RECONCILIATION_GAUGE]
    layout[f"CHART-{chart.id}"] = chart_node(
        chart,
        "ROW_OPERATIONS",
        3,
        34,
        parents=nested_row_prefix + ["ROW_OPERATIONS"],
    )

    chart = by_name[PAYMENT_SUCCESS_GAUGE]
    layout[f"CHART-{chart.id}"] = chart_node(
        chart,
        "ROW_OPERATIONS",
        3,
        34,
        parents=nested_row_prefix + ["ROW_OPERATIONS"],
    )

    layout["MD_EXEC_FOOTER"] = markdown_node(
        "MD_EXEC_FOOTER",
        "ROW_EXEC_FOOTER",
        executive_footer_markdown(),
        12,
        5,
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
    overflow: hidden !important;
    position: relative !important;
}}

.dashboard-component-chart-holder,
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
    font-size: 13px !important;
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
   -------------------------------------------------------------------------- */

.pdm-left-nav {{
    width: 100%;
    height: 100%;
    min-height: 100%;
    box-sizing: border-box;
    padding: 16px 12px;
    border-radius: 12px;
    background: linear-gradient(180deg, var(--pdm-navy-2) 0%, var(--pdm-navy) 100%);
    color: #FFFFFF;
    box-shadow: 0 4px 12px rgba(11, 31, 58, 0.20);
}}

.pdm-left-nav-brand {{
    padding: 4px 4px 13px 4px;
    color: #FFFFFF;
    font-size: 17px;
    line-height: 1;
    font-weight: 900;
    letter-spacing: 0.07em;
    border-bottom: 3px solid var(--pdm-yellow);
}}

.pdm-left-nav-section {{
    margin-top: 16px;
}}

.pdm-left-nav-heading {{
    margin-bottom: 6px;
    color: #9CC5F5;
    font-size: 10px;
    line-height: 1.1;
    font-weight: 900;
    letter-spacing: 0.09em;
}}

.pdm-left-nav-item {{
    margin: 2px 0;
    padding: 7px 8px;
    border-radius: 7px;
    color: #E7EEF7;
    font-size: 11px;
    line-height: 1.15;
    font-weight: 650;
}}

.pdm-left-nav-item.active {{
    background: #FFFFFF;
    color: var(--pdm-navy);
    font-weight: 850;
    box-shadow: inset 4px 0 0 var(--pdm-yellow);
}}

.pdm-left-nav-item:not(.active) {{
    border-left: 2px solid transparent;
}}

.pdm-left-nav-meta {{
    margin-top: 20px;
    padding-top: 12px;
    border-top: 1px solid rgba(255, 255, 255, 0.22);
    color: #C5D4E5;
    font-size: 9px;
    line-height: 1.6;
}}

/* Remove the normal white Markdown card around the left navigation. */
.dashboard-component-chart-holder:has(.pdm-left-nav) {{
    border: none !important;
    border-radius: 12px !important;
    background: transparent !important;
    box-shadow: none !important;
    padding: 0 !important;
}}

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
        font-size: 9px;
        padding: 5px 5px;
    }}
}}
"""


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

        required_overview = {
            "kpi_reporting_month", "kpi_previous_month",
            "current_month_total_approved_amount", "previous_month_total_approved_amount",
            "current_month_total_settled_amount", "previous_month_total_settled_amount",
            "current_month_total_credited_amount", "previous_month_total_credited_amount",
            "current_month_total_disbursed_amount", "previous_month_total_disbursed_amount",
            "current_month_total_outstanding_amount", "previous_month_total_outstanding_amount",
            "current_month_high_risk_case_count", "previous_month_high_risk_case_count",
            "repayment_rate", "reconciliation_rate", "payment_success_rate",
        }
        missing = required_overview - column_names(overview)
        if missing:
            raise RuntimeError(f"{EXECUTIVE_OVERVIEW} missing: {sorted(missing)}")

        required_monthly = {"reporting_month", "approved_amount", "disbursed_amount", "current_outstanding_amount", "approved_beneficiary_count", "approved_loan_count"}
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

        kpi_datasets: Dict[str, Any] = {}
        for spec in KPI_SPECS:
            kpi_datasets[spec["title"]] = virtual_dataset(
                session, SqlaTable,
                name=f"vds_pdm_kpi_{spec['slug']}_mom",
                sql=kpi_sql(spec),
                base=overview,
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
            sql=geojson_map_sql(),
            base=district,
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
                aliases=[spec.get("legacy_title")],
                description=f"{spec['title']} with one-month comparison and sparkline.",
            ))

        charts.append(
            upsert_chart(
                session,
                Slice,
                name=MAP_CHART,
                dataset=map_ds,
                viz_type="deck_geojson",
                params=map_params(),
                owner_user=owner_user,
                description=(
                    "Uganda district-level PDM geographic risk map rendered "
                    "with Deck.gl GeoJSON using only districts with matched geometry."
                ),
            )
        )
        charts.append(upsert_chart(session, Slice, name=FUNNEL_CHART, dataset=funnel_ds, viz_type="funnel", params=funnel_params(), owner_user=owner_user, description="Approved → Settled → Credited → Disbursed."))
        charts.append(upsert_chart(session, Slice, name=MONTHLY_TREND_CHART, dataset=monthly, viz_type="echarts_timeseries_line", params=monthly_trend_params(), owner_user=owner_user, description="Monthly approved, disbursed and outstanding funding movement."))
        charts.append(upsert_chart(session, Slice, name=MONTHLY_VOLUME_CHART, dataset=monthly, viz_type="echarts_timeseries_bar", params=monthly_volume_params(), owner_user=owner_user, description="Monthly approved beneficiary and loan volumes."))
        charts.append(upsert_chart(session, Slice, name=REPAYMENT_GAUGE, dataset=overview, viz_type="gauge_chart", params=gauge_params("repayment_rate", "Repayment Rate"), owner_user=owner_user, description="Total repaid divided by total disbursed."))
        charts.append(upsert_chart(session, Slice, name=RECONCILIATION_GAUGE, dataset=overview, viz_type="gauge_chart", params=gauge_params("reconciliation_rate", "Reconciliation Rate"), owner_user=owner_user, description="Reconciled payments divided by observed payments."))
        charts.append(upsert_chart(session, Slice, name=PAYMENT_SUCCESS_GAUGE, dataset=overview, viz_type="gauge_chart", params=gauge_params("payment_success_rate", "Payment Success Rate"), owner_user=owner_user, description="Credited amount divided by instructed amount."))

        dashboard.position_json = position_json(charts)
        dashboard.slices = charts
        dashboard.css = pdm_dashboard_css(
            by_name_map := next(
                chart.id for chart in charts
                if chart.slice_name == MAP_CHART
            )
        )
        dashboard.json_metadata = json.dumps({
            "timed_refresh_immune_slices": [],
            "expanded_slices": {},
            "refresh_frequency": 0,
            "default_filters": "{}",
            "label_colors": {},
            "chart_configuration": {},
            "native_filter_configuration": [],
            "cross_filters_enabled": True,
        })
        session.commit()

        print("=" * 80)
        print(f"Updated dashboard {dashboard.id}: {dashboard.dashboard_title}")
        print(f"Charts managed: {len(charts)}")
        print(f"6 KPI cards: one row, uppercase titles, centred ▲/▼ comparison vs {previous_month_label}")
        print("Modern charts: Funnel, ECharts Line, ECharts Bar, 3 Gauges")
        print("Executive design: Uganda header + dark-blue Leadership navigation on the left")
        print(f"Map: Deck.gl GeoJSON using {GEOJSON_MAP_DATASET}.geojson")
        print("Risk colours: 0 GREY | 1 GREEN | 2 YELLOW | 3 ORANGE | 4 RED")
        print("Map safety: NULL/blank GeoJSON rows excluded before Deck.gl query")
        print("=" * 80)


if __name__ == "__main__":
    update_dashboard()
