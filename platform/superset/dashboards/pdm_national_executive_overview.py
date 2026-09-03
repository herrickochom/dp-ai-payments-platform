"""Build the PDM executive dashboard from datasets registered in Superset.

Run inside the Superset container:
    python /app/platform/superset/dashboards/pdm_national_executive_overview.py

This idempotent updater has no runtime dependency on the dbt project.
It resolves the dashboard by title (optionally pinned with PDM_DASHBOARD_ID)
and creates it when missing, so metadata-DB rebuilds that shift dashboard
IDs do not break the run.
"""

import json
import os

from superset.app import create_app

# Optional pin to a specific metadata-DB dashboard ID (PDM_DASHBOARD_ID=2).
# When unset, the dashboard is resolved by title and created if it does not
# exist, so metadata-DB rebuilds that shift autoincrement IDs do not break
# the updater. update_dashboard() overwrites this with the resolved ID.
DASHBOARD_ID = int(os.environ["PDM_DASHBOARD_ID"]) if os.getenv("PDM_DASHBOARD_ID") else None
DASHBOARD_TITLE = "PDM National Executive Intelligence Dashboard"

NAVY, BLUE, TEAL = "#17365D", "#1F77B4", "#2A9D8F"
GREEN, LIME = "#2E7D5B", "#7BC96F"
AMBER, ORANGE, RED = "#F9C74F", "#F9844A", "#F94144"
PAGE_BG, CARD_BG, BORDER = "#05070B", "#111827", "#334155"
TEXT, TEXT_MUTED = "#FFFFFF", "#D1D5DB"
NO_DATA = "#6B7280"

DATASETS = {
    "overview": "cns_pdm_executive_overview",
    "fund_flow": "cns_pdm_fund_flow_funnel",
    "geographic": "cns_pdm_geographic_risk",
    "district_geographic": "cns_pdm_district_geographic_risk",
    "risk": "cns_pdm_fraud_risk_insights",
    "intervention": "cns_pdm_loan_intervention_dashboard",
}

OPTIONAL_LEADERSHIP_DATASETS = {
    "trend": "cns_pdm_executive_monthly_trend",
    "beneficiary": "cns_pdm_beneficiary_insights",
    "local_government": "cns_pdm_local_government_performance",
    "parish": "cns_pdm_parish_performance",
    "social_impact": "cns_pdm_social_impact",
}

# Country Map does not match on district names. It matches the query entity
# against the ISO property embedded in Superset's Uganda GeoJSON.
MAP_ENTITY_CANDIDATES = (
    "superset_district_iso",
    "district_iso_code",
    "iso_3166_2",
)

LABEL_COLORS = {
    "LOW": LIME, "MEDIUM": AMBER, "HIGH": ORANGE, "SEVERE": RED,
    "NO DATA": NO_DATA, "NO_DATA": NO_DATA,
    "PROGRESSING": LIME, "REQUIRES_INTERVENTION": ORANGE,
    "Approved": BLUE, "Instructed": TEAL, "Settled": "#457B9D",
    "Credited": GREEN, "Cash-out": NAVY,
}

DASHBOARD_CSS = f"""
.dashboard-content,
.dashboard-grid {{
  background: {PAGE_BG} !important;
  color: {TEXT} !important;
}}

.dashboard-grid {{
  padding: 10px 14px 18px 194px !important;
}}

.dashboard-header-container {{
  background: {CARD_BG} !important;
  border: 1px solid {BORDER} !important;
  border-radius: 8px !important;
  box-shadow: 0 2px 8px rgba(23,54,93,.06) !important;
  margin: 8px 14px 4px !important;
  padding: 7px 12px !important;
}}

.dashboard-header-container h1,
.dashboard-header-container h2 {{
  color: {TEXT} !important;
  font-size: 22px !important;
  font-weight: 800 !important;
}}

.dashboard-component-chart-holder {{
  background: {CARD_BG} !important;
  border: 1px solid {BORDER} !important;
  border-radius: 8px !important;
  box-shadow: 0 2px 7px rgba(23,54,93,.055) !important;
  color: {TEXT} !important;
  overflow: hidden !important;
}}

.slice_container {{
  background: {CARD_BG} !important;
}}

.chart-header,
.slice-header {{
  color: {TEXT} !important;
  font-size: 12px !important;
  font-weight: 800 !important;
  letter-spacing: .35px !important;
  text-transform: uppercase !important;
}}

.dashboard-component-chart-holder .header-title,
.dashboard-component-chart-holder .header-title a,
.dashboard-component-chart-holder table,
.dashboard-component-chart-holder th,
.dashboard-component-chart-holder td,
.dashboard-component-chart-holder label,
.dashboard-component-chart-holder span,
.dashboard-component-chart-holder p {{
  color: {TEXT} !important;
}}

.dashboard-component-chart-holder svg text {{
  fill: {TEXT} !important;
}}

[id^="CHART-KPI-"] {{
  border-top: 4px solid {BLUE} !important;
}}

#CHART-KPI-HIGH-RISK {{
  border-top-color: {RED} !important;
}}

.superset-legacy-chart-big-number {{
  color: {TEXT} !important;
  opacity: 1 !important;
  visibility: visible !important;
}}

.superset-legacy-chart-big-number .text-container {{
  align-items: center !important;
  display: flex !important;
  justify-content: center !important;
  min-height: 90px !important;
}}

.superset-legacy-chart-big-number .header-line {{
  color: {TEXT} !important;
  font-size: 20px !important;
  font-weight: 600 !important;
}}

.superset-legacy-chart-big-number .subheader-line {{
  color: {TEXT_MUTED} !important;
  font-size: 12px !important;
  font-weight: 700 !important;
  line-height: 1.25 !important;
  text-transform: none !important;
}}

/* LEFT NAVIGATION */
.dashboard-grid .dragdroppable-row:has(.dashboard-component-markdown h4),
.dashboard-grid .dragdroppable-column:has(.dashboard-component-markdown h4) {{
  min-height: 0 !important;
  overflow: visible !important;
  position: relative !important;
  z-index: 1000 !important;
}}

.dashboard-component-markdown:has(h4) {{
  background: #0B1220 !important;
  border: 1px solid {BORDER} !important;
  border-left: 4px solid {BLUE} !important;
  border-radius: 8px !important;
  bottom: 18px !important;
  box-sizing: border-box !important;
  left: 14px !important;
  overflow-y: auto !important;
  padding: 12px 10px !important;
  position: fixed !important;
  top: 88px !important;
  width: 166px !important;
  z-index: 1000 !important;
}}

.dashboard-component-markdown:has(h4) h4 {{
  display: none !important;
}}

.dashboard-component-markdown:has(h4) p {{
  display: block !important;
  margin: 2px 0 !important;
  padding: 0 !important;
  width: 100% !important;
}}

.dashboard-component-markdown:has(h4) a {{
  border-left: 3px solid transparent !important;
  border-radius: 5px !important;
  color: {TEXT_MUTED} !important;
  display: block !important;
  font-size: 12px !important;
  font-weight: 700 !important;
  line-height: 1.25 !important;
  padding: 9px 9px !important;
  text-decoration: none !important;
  width: 100% !important;
}}

.dashboard-component-markdown:has(h4) p:first-of-type a {{
  background: rgba(31,119,180,.20) !important;
  border-left-color: {BLUE} !important;
  color: {TEXT} !important;
}}

.dashboard-component-markdown:has(h4) a:hover,
.dashboard-component-markdown:has(h4) a:focus {{
  background: rgba(42,157,143,.18) !important;
  border-left-color: {TEAL} !important;
  color: {TEXT} !important;
  outline: none !important;
}}

[id^="CHART-"] {{
  scroll-margin-top: 96px !important;
}}

/* MAP LEGEND IS INSIDE THE MAP; THERE IS NO LEGEND DASHBOARD COMPONENT. */
#CHART-GEOGRAPHIC-RISK,
[data-test-chart-name="UGANDA GEOGRAPHIC RISK INTELLIGENCE"],
[data-test-chart-name="Uganda Geographic Risk Intelligence"] {{
  border-top: 4px solid {TEAL} !important;
  overflow: hidden !important;
  position: relative !important;
}}

#CHART-GEOGRAPHIC-RISK .slice_container,
[data-test-chart-name="UGANDA GEOGRAPHIC RISK INTELLIGENCE"] .slice_container,
[data-test-chart-name="Uganda Geographic Risk Intelligence"] .slice_container {{
  position: relative !important;
}}

#CHART-GEOGRAPHIC-RISK .slice_container::after,
[data-test-chart-name="UGANDA GEOGRAPHIC RISK INTELLIGENCE"] .slice_container::after,
[data-test-chart-name="Uganda Geographic Risk Intelligence"] .slice_container::after {{
  background: rgba(5,7,11,.92) !important;
  border: 1px solid {BORDER} !important;
  border-left: 4px solid {TEAL} !important;
  border-radius: 8px !important;
  bottom: 14px !important;
  box-shadow: 0 2px 8px rgba(0,0,0,.35) !important;
  color: {TEXT} !important;
  content: "RISK SEVERITY\\A🟩 LOW (1)\\A🟨 MEDIUM (2)\\A🟧 HIGH (3)\\A🟥 SEVERE (4)\\A⬜ NO DATA" !important;
  font-size: 11px !important;
  font-weight: 800 !important;
  line-height: 1.62 !important;
  padding: 9px 11px !important;
  pointer-events: none !important;
  position: absolute !important;
  right: 14px !important;
  white-space: pre-line !important;
  z-index: 80 !important;
}}

#CHART-FUND-FLOW {{ border-top: 4px solid {BLUE} !important; }}
#CHART-RISK-BANDS {{ border-top: 4px solid {ORANGE} !important; }}
#CHART-RISK-INDICATORS {{ border-top: 4px solid {RED} !important; }}
#CHART-INTERVENTION-STATUS {{ border-top: 4px solid {ORANGE} !important; }}
#CHART-INTERVENTION-PRIORITY {{ border-top: 4px solid {RED} !important; }}
#CHART-DISBURSEMENTS-TREND {{ border-top: 4px solid {BLUE} !important; }}
#CHART-REPAYMENTS-TREND {{ border-top: 4px solid {TEAL} !important; }}
#CHART-REGIONAL-FUNDING {{ border-top: 4px solid {BLUE} !important; }}
#CHART-PARISH-PORTFOLIO {{ border-top: 4px solid {TEAL} !important; }}
#CHART-LOCAL-RISK-REGISTER {{ border-top: 4px solid {AMBER} !important; }}

.dashboard-component-markdown {{
  background: transparent !important;
  color: {TEXT} !important;
}}

#MARKDOWN-EXECUTIVE-SUBTITLE {{
  background: linear-gradient(90deg, #111827 0%, #0B1220 100%) !important;
  border: 1px solid {BORDER} !important;
  border-radius: 8px !important;
  padding: 8px 14px !important;
}}

#MARKDOWN-EXECUTIVE-SUBTITLE p {{
  color: {TEXT_MUTED} !important;
  font-size: 13px !important;
  margin: 0 !important;
}}

#MARKDOWN-ANALYSIS-SECTION,
#MARKDOWN-INTERVENTION-SECTION,
#MARKDOWN-LEADERSHIP-SECTION {{
  border-left: 4px solid {BLUE} !important;
  padding: 4px 10px !important;
}}

#MARKDOWN-INTERVENTION-SECTION {{
  border-left-color: {ORANGE} !important;
}}

#MARKDOWN-LEADERSHIP-SECTION {{
  border-left-color: {TEAL} !important;
}}

#MARKDOWN-DASHBOARD-FOOTER {{
  border-top: 1px solid {BORDER} !important;
  color: {TEXT_MUTED} !important;
  font-size: 11px !important;
  padding: 8px 2px 0 !important;
  text-align: right !important;
}}

@media (max-width: 900px) {{
  .dashboard-grid {{
    padding-left: 158px !important;
  }}

  .dashboard-component-markdown:has(h4) {{
    left: 6px !important;
    width: 140px !important;
  }}
}}
"""


def metric(column, label, aggregate="SUM"):
    return {
        "expressionType": "SIMPLE", "column": {"column_name": column},
        "aggregate": aggregate, "sqlExpression": None,
        "hasCustomLabel": True, "label": label,
    }


def params(dataset, viz_type, **options):
    result = {
        "datasource": f"{dataset.id}__table", "viz_type": viz_type,
        "adhoc_filters": [], "row_limit": 10000, "color_scheme": "supersetColors",
        "echart_options": json.dumps({
            "backgroundColor": "transparent",
            "textStyle": {"color": TEXT},
            "legend": {"textStyle": {"color": TEXT}},
            "xAxis": {"axisLabel": {"color": TEXT}, "nameTextStyle": {"color": TEXT}},
            "yAxis": {"axisLabel": {"color": TEXT}, "nameTextStyle": {"color": TEXT}},
        }),
        "extra_form_data": {}, "dashboards": [DASHBOARD_ID],
    }
    result.update(options)
    return result


def upsert_chart(session, dashboard, dataset, name, viz_type, chart_params):
    from superset.models.slice import Slice

    chart = next(
        (c for c in dashboard.slices if c.slice_name.strip().casefold() == name.casefold()),
        None,
    )
    chart = chart or session.query(Slice).filter(Slice.slice_name == name).one_or_none()
    if chart is None:
        chart = Slice(slice_name=name, datasource_id=dataset.id,
                      datasource_type="table", viz_type=viz_type)
        session.add(chart)
        session.flush()
    chart.slice_name, chart.datasource_id = name, dataset.id
    chart.datasource_type, chart.viz_type = "table", viz_type
    chart.params = json.dumps(chart_params)
    if chart not in dashboard.slices:
        dashboard.slices.append(chart)
    return chart


def dataset_column_names(dataset):
    """Return physical/virtual column names registered on a Superset dataset."""
    return {column.column_name for column in dataset.columns}


def resolve_map_entity_column(dataset):
    """Resolve the ISO key used by Superset's bundled Uganda Country Map.

    The Country Map query emits the selected entity as ``country_id`` and the
    frontend matches that value to ``feature.properties.ISO`` in uganda.geojson.
    District names therefore cannot colour the polygons.
    """
    columns = dataset_column_names(dataset)
    for candidate in MAP_ENTITY_CANDIDATES:
        if candidate in columns:
            return candidate
    raise RuntimeError(
        "cns_pdm_district_geographic_risk needs a Superset-map district ISO column. "
        "Add one of: " + ", ".join(MAP_ENTITY_CANDIDATES) + ". "
        "Values must match the ISO properties in Superset's bundled "
        "uganda.geojson (for example UG-411 for Ntungamo). District names "
        "such as 'Ntungamo' will render the geometry but will not colour it."
    )



def fetch_kpi_snapshot(dataset):
    """Read the one-row executive overview for dynamic KPI subheaders.

    The dashboard updater is intentionally best-effort here: if Superset has not
    yet synchronised the new dbt columns, the main KPI cards still render and the
    subheader falls back to 'MoM unavailable'.
    """
    from sqlalchemy import text as sql_text

    columns = dataset_column_names(dataset)
    required = {
        "total_approved_amount_mom_pct", "total_approved_amount_mom_arrow",
        "total_approved_amount_mom_status",
        "total_settled_amount_mom_pct", "total_settled_amount_mom_arrow",
        "total_settled_amount_mom_status",
        "total_credited_amount_mom_pct", "total_credited_amount_mom_arrow",
        "total_credited_amount_mom_status",
        "total_disbursed_amount_mom_pct", "total_disbursed_amount_mom_arrow",
        "total_disbursed_amount_mom_status",
        "total_outstanding_amount_mom_pct", "total_outstanding_amount_mom_arrow",
        "total_outstanding_amount_mom_status",
        "high_risk_case_count_mom_pct", "high_risk_case_count_mom_arrow",
        "high_risk_case_count_mom_status",
    }
    if not required.issubset(columns):
        return None

    select_columns = ", ".join(sorted(required))
    qualified = f'"{dataset.schema}"."{dataset.table_name}"'
    statement = sql_text(f"select {select_columns} from {qualified} limit 1")

    try:
        with dataset.database.get_sqla_engine(schema=dataset.schema) as engine:
            with engine.connect() as connection:
                row = connection.execute(statement).mappings().first()
                return dict(row) if row else None
    except Exception as exc:
        print(f"WARNING: Could not load KPI MoM snapshot: {exc}")
        return None


def kpi_subheader(snapshot, base_column, unit):
    """Build e.g. 'UGX · 🟢 ↑ 8.4% vs last month'."""
    if not snapshot:
        return f"{unit} · MoM unavailable"

    arrow = snapshot.get(f"{base_column}_mom_arrow") or "—"
    pct = snapshot.get(f"{base_column}_mom_pct")
    status = snapshot.get(f"{base_column}_mom_status") or "NO_PRIOR"

    if pct is None or status == "NO_PRIOR":
        return f"{unit} · — No prior month"

    marker = {
        "FAVOURABLE": "🟢",
        "UNFAVOURABLE": "🔴",
        "NEUTRAL": "⚪",
    }.get(status, "⚪")

    return f"{unit} · {marker} {arrow} {abs(float(pct)):.1f}% vs last month"


def configure_charts(session, dashboard, datasets):
    charts = {c.slice_name.strip(): c for c in dashboard.slices}
    recovered = charts.get("TOTAL RECOVERED") or charts.get("Total Recovered")
    if recovered is not None:
        recovered.slice_name = "TOTAL DISBURSED"
    high_risk = charts.get("HIGH RISK") or charts.get("High Risk")
    if high_risk is not None:
        high_risk.slice_name = "HIGH RISK CASES"
    kpis = (
        ("Total Approved", "total_approved_amount", "Total Approved", "UGX"),
        ("Total Settled", "total_settled_amount", "Total Settled", "UGX"),
        ("Total Credited", "total_credited_amount", "Total Credited", "UGX"),
        ("Total Disbursed", "total_disbursed_amount", "Total Disbursed", "UGX"),
        ("Outstanding", "total_outstanding_amount", "Outstanding", "UGX"),
        ("High Risk Cases", "high_risk_case_count", "High Risk Cases", "CASES"),
    )
    kpi_snapshot = fetch_kpi_snapshot(datasets["overview"])
    for name, column, label, unit in kpis:
        charts[name] = upsert_chart(
            session, dashboard, datasets["overview"], name.upper(), "big_number_total",
            params(
                datasets["overview"],
                "big_number_total",
                metric=metric(column, label),
                header_font_size=.42,
                subtitle_font_size=.20,
                show_metric_name=False,
                subheader=kpi_subheader(kpi_snapshot, column, unit),
                subheader_font_size=.24,
                y_axis_format="SMART_NUMBER",
                time_format="smart_date",
                force_timestamp_formatting=False,
            ),
        )

    charts["PDM Fund Flow"] = upsert_chart(
        session, dashboard, datasets["fund_flow"], "PDM Fund Flow", "funnel",
        params(datasets["fund_flow"], "funnel", groupby=["stage"],
               metric=metric("amount", "Amount"), row_limit=10, sort_by_metric=True,
               percent_calculation_type="first_step", show_legend=False,
               label_type=3, tooltip_label_type=5, number_format="SMART_NUMBER",
               show_labels=True, show_tooltip_labels=True),
    )

    pie_common = dict(
        sort_by_metric=True, show_legend=True, legendType="scroll",
        legendOrientation="bottom", number_format="SMART_NUMBER",
        show_labels=True, labels_outside=True, label_line=True,
        show_labels_threshold=0,
        showTooltipTotal=True, showTooltipPercentage=True,
        donut=True, innerRadius=50, outerRadius=74,
    )
    geographic_map = (
        charts.get("Uganda District Risk Map")
        or
        charts.get("Uganda District Portfolio Exposure")
        or charts.get("Uganda Geographic Risk Map")
        or charts.get("Geographic Risk Distribution")
    )
    if geographic_map is not None:
        geographic_map.slice_name = "UGANDA GEOGRAPHIC RISK INTELLIGENCE"
    map_entity = resolve_map_entity_column(datasets["district_geographic"])
    charts["Geographic Risk Distribution"] = upsert_chart(
        session, dashboard, datasets["district_geographic"],
        "UGANDA GEOGRAPHIC RISK INTELLIGENCE", "country_map",
        params(
            datasets["district_geographic"],
            "country_map",
            select_country="uganda",
            entity=map_entity,
            metric=metric(
                "geographic_risk_score",
                "District risk score: 1 Low · 2 Medium · 3 High · 4 Severe",
                "MAX",
            ),
            # CountryMap uses categorical colours whenever color_scheme is set.
            # Explicitly null it so the risk metric drives the sequential scale.
            color_scheme=None,
            linear_color_scheme="schemeYlOrRd",
            number_format=".0f",
            row_limit=5000,
        ),
    )
    charts["Risk Cases by Risk Band"] = upsert_chart(
        session, dashboard, datasets["risk"], "Risk Cases by Risk Band", "echarts_timeseries_bar",
        params(datasets["risk"], "echarts_timeseries_bar", x_axis="risk_band",
               xAxisForceCategorical=True,
               metrics=[metric("risk_case_sk", "Cases", "COUNT_DISTINCT")],
               groupby=[], orientation="horizontal", order_desc=True,
               sort_series_type="sum", sort_series_ascending=True,
               show_value=True, only_total=True, show_legend=False,
               x_axis_title="Risk cases", y_axis_title="",
               x_axis_number_format="SMART_NUMBER", y_axis_format="SMART_NUMBER",
               truncateXAxis=False, truncateYAxis=False, rich_tooltip=True),
    )
    coverage = charts.get("Reporting Coverage & Freshness") or charts.get("Top Risk Indicators")
    if coverage is not None:
        coverage.slice_name = "Reporting Coverage & Freshness"
    charts["Reporting Coverage & Freshness"] = upsert_chart(
        session, dashboard, datasets["overview"], "Reporting Coverage & Freshness", "table",
        params(datasets["overview"], "table", query_mode="raw",
               all_columns=["as_of_date", "reporting_scope_status", "reporting_region_count",
                            "reporting_district_count", "reporting_parish_count", "sacco_count",
                            "beneficiary_count", "loan_count"],
               row_limit=1, include_search=False, table_filter=False,
               page_length=1, align_pn=False, color_pn=True),
    )
    charts["Loan Intervention Status"] = upsert_chart(
        session, dashboard, datasets["intervention"], "Loan Intervention Status", "pie",
        params(datasets["intervention"], "pie", groupby=["intervention_status"],
               metric=metric("loan_id", "Loans", "COUNT_DISTINCT"),
               label_type=5, show_total=True,
               total_label="Total cases", **pie_common),
    )
    charts["Intervention Priority by Parish"] = upsert_chart(
        session, dashboard, datasets["intervention"], "Intervention Priority by Parish",
        "echarts_timeseries_bar",
        params(datasets["intervention"], "echarts_timeseries_bar", x_axis="parish",
               xAxisForceCategorical=True,
               metrics=[metric("loan_id", "Cases", "COUNT_DISTINCT")], groupby=[],
               limit=8, timeseries_limit_metric=metric("loan_id", "Cases", "COUNT_DISTINCT"),
               order_desc=True, orientation="horizontal", sort_series_type="sum",
               sort_series_ascending=True, show_value=True, only_total=True,
               show_legend=False, x_axis_title="Cases requiring intervention",
               y_axis_title="", x_axis_number_format="SMART_NUMBER",
               y_axis_format="SMART_NUMBER", truncateXAxis=False, truncateYAxis=False,
               rich_tooltip=True, showTooltipTotal=True,
               showTooltipPercentage=True),
    )
    charts["Regional Funding Delivery"] = upsert_chart(
        session, dashboard, datasets["geographic"], "Regional Funding Delivery",
        "echarts_timeseries_bar",
        params(datasets["geographic"], "echarts_timeseries_bar", x_axis="region",
               xAxisForceCategorical=True,
               metrics=[metric("approved_amount", "Approved"),
                        metric("disbursed_amount", "Disbursed")],
               groupby=[], order_desc=True, orientation="vertical",
               sort_series_type="sum", sort_series_ascending=False,
               show_value=True, only_total=False, show_legend=True,
               legendType="plain", legendOrientation="top",
               x_axis_title="Region", y_axis_title="UGX",
               y_axis_format="SMART_NUMBER", truncateXAxis=False,
               rich_tooltip=True, showTooltipTotal=True),
    )
    charts["Parish Portfolio and Repayment"] = upsert_chart(
        session, dashboard, datasets["geographic"], "Parish Portfolio and Repayment",
        "bubble",
        params(datasets["geographic"], "bubble", entity="parish", series="region",
               x=metric("outstanding_amount", "Outstanding"),
               y=metric("principal_repayment_rate", "Repayment rate", "AVG"),
               size=metric("loan_count", "Loans"), max_bubble_size="50",
               x_axis_label="Outstanding (UGX)", y_axis_label="Repayment rate",
               x_axis_format="SMART_NUMBER", y_axis_format=".1%",
               show_legend=True),
    )
    charts["Local Government Risk Register"] = upsert_chart(
        session, dashboard, datasets["geographic"], "Local Government Risk Register",
        "table",
        params(datasets["geographic"], "table", query_mode="raw",
               all_columns=["region", "district", "parish", "geographic_risk_band",
                            "loan_count", "outstanding_amount",
                            "principal_repayment_rate", "high_identity_alert_count"],
               order_by_cols=['["high_identity_alert_count", false]'],
               row_limit=25, include_search=True, table_filter=True,
               page_length=10, align_pn=False, color_pn=True),
    )
    trend_dataset = datasets.get("trend", datasets["overview"])
    trend_axis = "reporting_month" if "trend" in datasets else "as_of_date"
    old_repayment_trend = charts.get("Repayments Trend")
    if old_repayment_trend is not None:
        old_repayment_trend.slice_name = "Payment Reconciliation Trend"
    for name, column, scheme, number_format in (
        ("Disbursements Trend", "total_disbursed_amount", "supersetColors", "SMART_NUMBER"),
        ("Payment Reconciliation Trend", "reconciliation_rate", "bnbColors", ".1%"),
    ):
        if "trend" in datasets:
            column = {
                "total_disbursed_amount": "disbursed_amount",
                "reconciliation_rate": "payment_reconciliation_rate",
            }[column]
        charts[name] = upsert_chart(
            session, dashboard, trend_dataset, name, "echarts_timeseries_line",
            params(trend_dataset, "echarts_timeseries_line", x_axis=trend_axis,
                   time_grain_sqla="P1M", metrics=[metric(column, name.replace(" Trend", ""))],
                   groupby=[], order_desc=False, row_limit=1000, truncate_metric=True,
                   show_legend=False, show_value=True, rich_tooltip=True,
                   x_axis_time_format="%b %Y", y_axis_format=number_format,
                   area=True, opacity=.22, markerEnabled=True, markerSize=5,
                   color_scheme=scheme),
        )
    return charts


def chart_component(chart, component_id, row_id, column_id, width, height):
    return {
        "children": [], "id": component_id,
        "meta": {"chartId": chart.id, "height": height, "sliceName": chart.slice_name,
                 "uuid": str(chart.uuid), "width": width},
        "parents": ["ROOT_ID", "GRID_ID", row_id, column_id], "type": "CHART",
    }


def add_row(layout, row_id, specs):
    children = []
    for component_id, chart, width, height in specs:
        column_id = component_id.replace("CHART-", "COLUMN-")
        children.append(column_id)
        layout[column_id] = {
            "children": [component_id], "id": column_id,
            "meta": {"background": "BACKGROUND_TRANSPARENT", "width": width},
            "parents": ["ROOT_ID", "GRID_ID", row_id], "type": "COLUMN",
        }
        layout[component_id] = chart_component(chart, component_id, row_id, column_id, width, height)
    layout[row_id] = {
        "children": children, "id": row_id,
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"], "type": "ROW",
    }




def add_markdown_row(layout, row_id, component_id, code, height):
    column_id = component_id.replace("MARKDOWN-", "COLUMN-")
    layout[row_id] = {
        "children": [column_id], "id": row_id,
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
        "parents": ["ROOT_ID", "GRID_ID"], "type": "ROW",
    }
    layout[column_id] = {
        "children": [component_id], "id": column_id,
        "meta": {"background": "BACKGROUND_TRANSPARENT", "width": 12},
        "parents": ["ROOT_ID", "GRID_ID", row_id], "type": "COLUMN",
    }
    layout[component_id] = {
        "children": [], "id": component_id,
        "meta": {"code": code, "height": height, "width": 12},
        "parents": ["ROOT_ID", "GRID_ID", row_id, column_id], "type": "MARKDOWN",
    }


def build_layout(charts):
    rows = [
        "ROW-EXECUTIVE-MENU", "ROW-EXECUTIVE-SUBTITLE", "ROW-KPI", "ROW-ANALYSIS-SECTION",
        "ROW-ANALYSIS", "ROW-INTERVENTION-SECTION", "ROW-INTERVENTION",
        "ROW-LEADERSHIP-SECTION", "ROW-LEADERSHIP", "ROW-DASHBOARD-FOOTER",
    ]
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"children": ["GRID_ID"], "id": "ROOT_ID", "type": "ROOT"},
        "HEADER_ID": {"id": "HEADER_ID", "meta": {"text": DASHBOARD_TITLE}, "type": "HEADER"},
        "GRID_ID": {"children": rows, "id": "GRID_ID", "parents": ["ROOT_ID"], "type": "GRID"},
    }
    add_markdown_row(
        layout, "ROW-EXECUTIVE-MENU", "MARKDOWN-EXECUTIVE-MENU",
        "#### NAVIGATION\\n\\n"
        "[Overview](#CHART-KPI-APPROVED)\\n\\n"
        "[Geography](#CHART-GEOGRAPHIC-RISK)\\n\\n"
        "[Interventions](#CHART-INTERVENTION-STATUS)\\n\\n"
        "[Beneficiaries](#CHART-PARISH-PORTFOLIO)\\n\\n"
        "[Finance](#CHART-FUND-FLOW)\\n\\n"
        "[Risk and Fraud](#CHART-RISK-BANDS)\\n\\n"
        "[Reports](#CHART-LOCAL-RISK-REGISTER)\\n\\n"
        "[Data Quality](#CHART-RISK-INDICATORS)\\n\\n"
        "[PDM Analysis](#CHART-REGIONAL-FUNDING)",
        18,
    )
    add_markdown_row(
        layout, "ROW-EXECUTIVE-SUBTITLE", "MARKDOWN-EXECUTIVE-SUBTITLE",
        "Operational monitoring view · LIMITED SAMPLE until national coverage thresholds are met · Values reflect records currently received by the platform",
        4,
    )
    add_row(layout, "ROW-KPI", [
        ("CHART-KPI-APPROVED", charts["Total Approved"], 2, 14),
        ("CHART-KPI-SETTLED", charts["Total Settled"], 2, 14),
        ("CHART-KPI-CREDITED", charts["Total Credited"], 2, 14),
        ("CHART-KPI-RECOVERED", charts["Total Disbursed"], 2, 14),
        ("CHART-KPI-OUTSTANDING", charts["Outstanding"], 2, 14),
        ("CHART-KPI-HIGH-RISK", charts["High Risk Cases"], 2, 14),
    ])
    add_markdown_row(
        layout, "ROW-ANALYSIS-SECTION", "MARKDOWN-ANALYSIS-SECTION",
        "### FUND FLOW & RISK MONITORING",
        3,
    )
    add_row(layout, "ROW-ANALYSIS", [
        ("CHART-FUND-FLOW", charts["PDM Fund Flow"], 3, 34),
        ("CHART-GEOGRAPHIC-RISK", charts["Geographic Risk Distribution"], 5, 34),
        ("CHART-RISK-BANDS", charts["Risk Cases by Risk Band"], 2, 34),
        ("CHART-RISK-INDICATORS", charts["Reporting Coverage & Freshness"], 2, 34),
    ])
    add_markdown_row(
        layout, "ROW-INTERVENTION-SECTION", "MARKDOWN-INTERVENTION-SECTION",
        "### INTERVENTION PRIORITIES & FINANCIAL TRENDS",
        3,
    )
    add_row(layout, "ROW-INTERVENTION", [
        ("CHART-INTERVENTION-STATUS", charts["Loan Intervention Status"], 3, 30),
        ("CHART-INTERVENTION-PRIORITY", charts["Intervention Priority by Parish"], 4, 30),
        ("CHART-DISBURSEMENTS-TREND", charts["Disbursements Trend"], 2, 30),
        ("CHART-REPAYMENTS-TREND", charts["Payment Reconciliation Trend"], 3, 30),
    ])
    add_markdown_row(
        layout, "ROW-LEADERSHIP-SECTION", "MARKDOWN-LEADERSHIP-SECTION",
        "### REGIONAL DELIVERY & LOCAL GOVERNMENT PERFORMANCE",
        3,
    )
    add_row(layout, "ROW-LEADERSHIP", [
        ("CHART-REGIONAL-FUNDING", charts["Regional Funding Delivery"], 4, 32),
        ("CHART-PARISH-PORTFOLIO", charts["Parish Portfolio and Repayment"], 4, 32),
        ("CHART-LOCAL-RISK-REGISTER", charts["Local Government Risk Register"], 4, 32),
    ])
    add_markdown_row(
        layout, "ROW-DASHBOARD-FOOTER", "MARKDOWN-DASHBOARD-FOOTER",
        "Source: PDM Analytics Lakehouse · 'As of' is the latest source event, not refresh time · Monthly loan values are approval-cohort measures · Map reference coordinates are used where agent GPS is unavailable",
        3,
    )
    return layout


def update_dashboard():
    app = create_app()
    with app.app_context():
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.dashboard import Dashboard

        global DASHBOARD_ID

        # Resolve the dashboard: env pin first, then by title (the natural key
        # this updater enforces on every run), then create it. A metadata-DB
        # rebuild can change the autoincrement ID, so the title is what makes
        # this updater idempotent across environments.
        dashboard = None
        if DASHBOARD_ID is not None:
            dashboard = db.session.query(Dashboard).filter(
                Dashboard.id == DASHBOARD_ID).one_or_none()
            if dashboard is None:
                print(f"WARNING: pinned dashboard id {DASHBOARD_ID} not found, "
                      "falling back to title lookup")
        if dashboard is None:
            # Case-insensitive: the title may be re-cased from the Superset UI.
            from sqlalchemy import func as sa_func

            dashboard = db.session.query(Dashboard).filter(
                sa_func.lower(Dashboard.dashboard_title) == DASHBOARD_TITLE.lower()
            ).one_or_none()
        if dashboard is None:
            dashboard = Dashboard(
                dashboard_title=DASHBOARD_TITLE,
                slug="pdm-national-executive-intelligence",
            )
            db.session.add(dashboard)
            db.session.flush()
            print(f"Created dashboard {dashboard.id}: {DASHBOARD_TITLE}")
        DASHBOARD_ID = dashboard.id
        registered = {d.table_name: d for d in db.session.query(SqlaTable)
                      .filter(SqlaTable.schema == "consumption").all()}
        missing = sorted(set(DATASETS.values()) - set(registered))
        if missing:
            raise RuntimeError(f"Superset is missing registered datasets: {missing}")
        datasets = {key: registered[name] for key, name in DATASETS.items()}
        datasets.update({
            key: registered[name]
            for key, name in OPTIONAL_LEADERSHIP_DATASETS.items()
            if name in registered
        })
        charts = configure_charts(db.session, dashboard, datasets)
        dashboard.position_json = json.dumps(build_layout(charts))
        metadata = json.loads(dashboard.json_metadata or "{}")
        metadata.update({
            "label_colors": LABEL_COLORS, "color_scheme": "supersetColors",
            "cross_filters_enabled": True, "refresh_frequency": 0,
            "timed_refresh_immune_slices": [], "expanded_slices": {},
        })
        metadata.setdefault("native_filter_configuration", [])
        dashboard.json_metadata = json.dumps(metadata)
        dashboard.css = DASHBOARD_CSS
        dashboard.dashboard_title = DASHBOARD_TITLE
        dashboard.published = True
        db.session.commit()
        print(
            "Uganda map entity column: "
            f"{resolve_map_entity_column(datasets['district_geographic'])}"
        )
        print(f"Updated dashboard {dashboard.id}: {dashboard.dashboard_title}")
        print(f"Dashboard charts: {len(dashboard.slices)}")


if __name__ == "__main__":
    update_dashboard()
