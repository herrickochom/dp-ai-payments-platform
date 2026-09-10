#!/usr/bin/env python3
"""Build/update the detailed PDM Leadership dashboard in Superset 6.1."""
from __future__ import annotations

import json
from typing import Any, Dict, List

from superset.app import create_app

from pdm_national_executive_overview import (
    assign_owner,
    column_names,
    mark_temporal,
    metric,
    physical_dataset,
    resolve_owner_user,
    trino_database,
    upsert_chart,
)

NAV = (
    '<div class="pdm-nav"><a href="/superset/dashboard/pdm-national-executive-intelligence/">Executive Overview</a>'
    '<a href="/superset/dashboard/pdm-leadership/">Leadership</a>'
    '<a href="/superset/dashboard/pdm-operations/">Operations</a>'
    '<a href="/superset/dashboard/pdm-investigators/">Investigators</a></div>'
)


def big_number(column: str, label: str, fmt: str = ",d", aggregate: str = "SUM") -> Dict[str, Any]:
    return {"metric": metric(column, label, aggregate), "adhoc_filters": [], "row_limit": 1,
            "show_timestamp": False, "show_metric_name": False, "y_axis_format": fmt}


def time_line(date_column: str, metrics: List[Dict[str, Any]], *, month: bool = False,
              number_format: str = ",.2f") -> Dict[str, Any]:
    return {"granularity_sqla": date_column, "time_grain_sqla": "P1M" if month else "P1D",
            "time_range": "No filter", "metrics": metrics, "groupby": [], "adhoc_filters": [],
            "row_limit": 10000, "show_legend": True, "legendOrientation": "top",
            "x_axis_time_format": "%b %Y" if month else "%-d %b %Y",
            "y_axis_format": number_format, "x_axis_title": "Month" if month else "Date",
            "rich_tooltip": True, "show_value": False}


def ranked(category: str, value: str, label: str, fmt: str = ",.2f",
           aggregate: str = "SUM") -> Dict[str, Any]:
    return {"groupby": [category], "metrics": [metric(value, label, aggregate)],
            "adhoc_filters": [], "row_limit": 20, "order_desc": True,
            "orientation": "horizontal", "show_legend": False, "show_value": True,
            "y_axis_format": fmt, "x_axis_title": label}


def pie(category: str, value: str, label: str, aggregate: str = "SUM") -> Dict[str, Any]:
    return {"groupby": [category], "metric": metric(value, label, aggregate),
            "adhoc_filters": [], "row_limit": 20, "show_legend": True,
            "legendOrientation": "right", "label_type": "key_percent", "number_format": ",d"}


def table(columns: List[str], metrics: List[Dict[str, Any]] | None = None) -> Dict[str, Any]:
    return {"all_columns": columns, "metrics": metrics or [], "adhoc_filters": [],
            "row_limit": 1000, "order_desc": True, "include_search": True,
            "table_filter": True, "align_pn": True, "page_length": 25}


def dashboard_object(session: Any, Dashboard: Any, title: str, slug: str, owner: Any) -> Any:
    dashboard = session.query(Dashboard).filter(Dashboard.slug == slug).order_by(Dashboard.id).first()
    if dashboard is None:
        dashboard = Dashboard(dashboard_title=title, slug=slug, published=True)
        session.add(dashboard)
    else:
        dashboard.dashboard_title, dashboard.slug, dashboard.published = title, slug, True
    assign_owner(dashboard, owner)
    session.flush()
    return dashboard


def markdown_node(node_id: str, row_id: str, text: str, width: int = 12, height: int = 4) -> Dict[str, Any]:
    return {"id": node_id, "type": "MARKDOWN", "meta": {"code": text, "width": width,
            "height": height}, "parents": ["ROOT_ID", "GRID_ID", row_id]}


def chart_node(chart: Any, row_id: str, width: int, height: int) -> Dict[str, Any]:
    return {"id": f"CHART-{chart.id}", "type": "CHART",
            "meta": {"chartId": chart.id, "sliceName": chart.slice_name,
                     "width": width, "height": height},
            "parents": ["ROOT_ID", "GRID_ID", row_id]}


def layout_json(title: str, active: str, charts: List[Any], sections: List[tuple[str, List[str]]]) -> str:
    by_name = {chart.slice_name: chart for chart in charts}
    layout: Dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {"id": "GRID_ID", "type": "GRID", "parents": ["ROOT_ID"], "children": []},
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": title}},
    }
    rows: List[str] = []
    nav_row = "ROW_NAV"
    rows.append(nav_row)
    layout[nav_row] = {"id": nav_row, "type": "ROW", "parents": ["ROOT_ID", "GRID_ID"],
                       "children": ["MD_NAV"],
                       "meta": {"background": "BACKGROUND_TRANSPARENT"}}
    layout["MD_NAV"] = markdown_node("MD_NAV", nav_row, NAV.replace(f">{active}<", f' class="active">{active}<'), 12, 3)
    for section_index, (section, names) in enumerate(sections):
        heading_row = f"ROW_SECTION_{section_index}"
        heading_id = f"MD_SECTION_{section_index}"
        rows.append(heading_row)
        layout[heading_row] = {"id": heading_row, "type": "ROW", "parents": ["ROOT_ID", "GRID_ID"],
                               "children": [heading_id],
                               "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        layout[heading_id] = markdown_node(heading_id, heading_row, f"## {section}", 12, 2)
        for start in range(0, len(names), 3):
            row_id = f"ROW_{section_index}_{start // 3}"
            batch = names[start:start + 3]
            rows.append(row_id)
            children = [f"CHART-{by_name[name].id}" for name in batch]
            layout[row_id] = {"id": row_id, "type": "ROW", "parents": ["ROOT_ID", "GRID_ID"],
                              "children": children,
                              "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            width = 12 // len(batch)
            for name in batch:
                chart = by_name[name]
                layout[f"CHART-{chart.id}"] = chart_node(chart, row_id, width, 38 if len(batch) < 3 else 28)
    layout["GRID_ID"]["children"] = rows
    return json.dumps(layout, separators=(",", ":"))


def native_filters(filter_specs: List[tuple[str, str, str]], datasets: Dict[str, Any],
                   charts: List[Any], chart_datasets: Dict[int, Any]) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    parents: List[str] = []
    for label, dataset_name, column in filter_specs:
        dataset = datasets[dataset_name]
        if column not in column_names(dataset):
            continue
        filter_id = f"NATIVE_FILTER-{dataset_name}-{column}"
        excluded = [chart.id for chart in charts if column not in column_names(chart_datasets[chart.id])]
        result.append({"id": filter_id, "name": label, "filterType": "filter_select",
                       "targets": [{"datasetId": dataset.id, "column": {"name": column}}],
                       "controlValues": {"enableEmptyFilter": False, "defaultToFirstItem": False,
                                         "creatable": False, "multiSelect": True,
                                         "searchAllOptions": True, "inverseSelection": False},
                       "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
                       "cascadeParentIds": list(parents),
                       "scope": {"rootPath": ["ROOT_ID"], "excluded": excluded},
                       "type": "NATIVE_FILTER"})
        parents.append(filter_id)
    return result


CSS = """
.pdm-nav{display:flex;gap:10px;padding:12px 18px;background:#173f35;border-radius:8px}
.pdm-nav a{color:#fff!important;font-size:16px;font-weight:600;padding:8px 14px;text-decoration:none}
.pdm-nav a.active{background:#f2c94c;color:#173f35!important;border-radius:6px}
.dashboard-component-chart-holder{border-radius:8px;box-shadow:0 1px 5px #0002}
.dashboard-markdown h2{color:#173f35;border-bottom:3px solid #f2c94c;padding-bottom:6px}
"""


def build_suite_dashboard(*, title: str, slug: str, active: str,
                          dataset_specs: Dict[str, str], temporal: Dict[str, str],
                          chart_specs: List[Dict[str, Any]],
                          sections: List[tuple[str, List[str]]],
                          filter_specs: List[tuple[str, str, str]]) -> None:
    app = create_app()
    with app.app_context():
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        session = db.session
        owner = resolve_owner_user()
        database = trino_database(session, Database)
        dashboard = dashboard_object(session, Dashboard, title, slug, owner)
        datasets = {key: physical_dataset(session, SqlaTable, database=database, table_name=name)
                    for key, name in dataset_specs.items()}
        for key, date_column in temporal.items():
            mark_temporal(datasets[key], date_column, session)

        charts: List[Any] = []
        chart_datasets: Dict[int, Any] = {}
        for spec in chart_specs:
            dataset = datasets[spec["dataset"]]
            required = set(spec.get("required", []))
            missing = required - column_names(dataset)
            if missing:
                raise RuntimeError(f"{dataset.table_name} missing columns: {sorted(missing)}")
            chart = upsert_chart(session, Slice, name=spec["name"], dataset=dataset,
                                 viz_type=spec["viz"], params=spec["params"],
                                 description=spec.get("description", spec["name"]), owner_user=owner)
            charts.append(chart)
            chart_datasets[chart.id] = dataset

        dashboard.position_json = layout_json(title, active, charts, sections)
        dashboard.css = CSS
        metadata = json.loads(dashboard.json_metadata or "{}")
        metadata["native_filter_configuration"] = native_filters(filter_specs, datasets, charts, chart_datasets)
        metadata["chart_configuration"] = {}
        dashboard.json_metadata = json.dumps(metadata, separators=(",", ":"))
        dashboard.slices = charts
        assign_owner(dashboard, owner)
        session.commit()
        print(f"Built {title}: {len(charts)} charts, {len(metadata['native_filter_configuration'])} filters")


DATASETS = {
    "overview": "cns_pdm_executive_overview",
    "monthly": "cns_pdm_executive_monthly_trend",
    "district": "cns_pdm_district_geographic_risk",
    "parish": "cns_pdm_parish_geographic_risk",
    "local": "cns_pdm_local_government_performance",
    "social": "cns_pdm_social_impact",
    "ai": "cns_pdm_district_ai_risk",
    "priority": "cns_pdm_executive_intervention_priorities",
    "drill": "cns_pdm_executive_geographic_drilldown",
}

CHARTS = [
    {"name": "LEADERSHIP BENEFICIARIES", "dataset": "overview", "viz": "big_number_total", "params": big_number("beneficiary_count", "Beneficiaries"), "required": ["beneficiary_count"]},
    {"name": "LEADERSHIP APPROVED FUNDS", "dataset": "overview", "viz": "big_number_total", "params": big_number("total_approved_amount", "Approved (UGX)", ",.2f"), "required": ["total_approved_amount"]},
    {"name": "LEADERSHIP OUTSTANDING", "dataset": "overview", "viz": "big_number_total", "params": big_number("total_outstanding_amount", "Outstanding (UGX)", ",.2f"), "required": ["total_outstanding_amount"]},
    {"name": "MONTHLY PROGRAMME PERFORMANCE", "dataset": "monthly", "viz": "echarts_timeseries_line", "params": time_line("reporting_month", [metric("approved_amount", "Approved"), metric("disbursed_amount", "Disbursed"), metric("repaid_amount", "Repaid")], month=True), "required": ["reporting_month", "approved_amount", "disbursed_amount", "repaid_amount"]},
    {"name": "DISTRICT OUTSTANDING RANKING", "dataset": "district", "viz": "echarts_timeseries_bar", "params": ranked("district", "outstanding_amount", "Outstanding (UGX)"), "required": ["district", "outstanding_amount"]},
    {"name": "DISTRICT GEOGRAPHIC RISK", "dataset": "district", "viz": "pie", "params": pie("geographic_risk_band", "district", "Districts", "COUNT_DISTINCT"), "required": ["geographic_risk_band", "district"]},
    {"name": "PARISH PERFORMANCE", "dataset": "parish", "viz": "table", "params": table(["region", "district", "parish", "beneficiary_count", "loan_count", "approved_amount", "disbursed_amount", "outstanding_amount", "geographic_risk_band"]), "required": ["region", "district", "parish"]},
    {"name": "LOCAL GOVERNMENT PERFORMANCE", "dataset": "local", "viz": "table", "params": table(["region", "district", "parish", "beneficiary_count", "loan_count", "approved_amount", "disbursed_amount", "outstanding_amount", "local_performance_status"]), "required": ["region", "district", "parish"]},
    {"name": "DISTRICT AI DEFAULT RISK", "dataset": "ai", "viz": "echarts_timeseries_bar", "params": ranked("district", "avg_probability_default_90d", "Default risk", ".2%", "AVG"), "required": ["district", "avg_probability_default_90d"]},
    {"name": "LEADERSHIP INTERVENTION PRIORITIES", "dataset": "priority", "viz": "table", "params": table(["intervention_rank", "region", "district", "geographic_risk_band", "ai_intervention_band", "avg_probability_default_90d", "outstanding_amount", "recommended_action", "interpretation"]), "required": ["intervention_rank", "district", "interpretation"]},
    {"name": "SOCIAL IMPACT REACH", "dataset": "social", "viz": "echarts_timeseries_bar", "params": ranked("group_name", "funded_beneficiary_count", "Beneficiaries", ",d"), "required": ["group_name", "funded_beneficiary_count"]},
]

SECTIONS = [
    ("LEADERSHIP OVERVIEW", ["LEADERSHIP BENEFICIARIES", "LEADERSHIP APPROVED FUNDS", "LEADERSHIP OUTSTANDING"]),
    ("PROGRAMME PERFORMANCE", ["MONTHLY PROGRAMME PERFORMANCE"]),
    ("GEOGRAPHIC PERFORMANCE", ["DISTRICT OUTSTANDING RANKING", "DISTRICT GEOGRAPHIC RISK", "PARISH PERFORMANCE"]),
    ("LOCAL GOVERNMENT PERFORMANCE", ["LOCAL GOVERNMENT PERFORMANCE"]),
    ("AI DEFAULT RISK", ["DISTRICT AI DEFAULT RISK"]),
    ("INTERVENTION PRIORITIES", ["LEADERSHIP INTERVENTION PRIORITIES"]),
    ("SOCIAL IMPACT", ["SOCIAL IMPACT REACH"]),
]


def update_dashboard() -> None:
    build_suite_dashboard(title="PDM LEADERSHIP PERFORMANCE", slug="pdm-leadership", active="Leadership",
                          dataset_specs=DATASETS, temporal={"monthly": "reporting_month", "ai": "observation_date"},
                          chart_specs=CHARTS, sections=SECTIONS,
                          filter_specs=[("Region", "drill", "region"), ("District", "drill", "district"),
                                        ("County", "drill", "county"), ("Sub County", "drill", "sub_county"),
                                        ("Parish", "drill", "parish"), ("Village", "drill", "village")])


if __name__ == "__main__":
    update_dashboard()
