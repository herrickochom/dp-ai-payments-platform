"""
FINAL — Government‑Formal PDM Executive Dashboard
No missing columns · Centered KPIs · Left Menu · Fully Working
"""

import json
from superset.app import create_app

DASHBOARD_ID = 1
DASHBOARD_TITLE = "PDM NATIONAL EXECUTIVE OVERVIEW"

# --- GOVERNMENT COLORS ---
GOV_COLORS = {
    "navy": "#0A1A2F",
    "gold": "#C9A635",
    "white": "#FFFFFF",
    "grey_bg": "#F4F5F7",
    "border": "#D6D6D6",
    "text": "#0A1A2F",
    "muted": "#6C757D",
}

LABEL_COLORS = {
    "LOW": "#4CAF50",
    "MEDIUM": "#FFC107",
    "HIGH": "#FF7043",
    "SEVERE": "#D32F2F",
    "NO DATA": "#BDBDBD",
}

# --- CSS THEME ---
DASHBOARD_CSS = f"""
.dashboard-content, .dashboard-grid {{
  background: {GOV_COLORS['grey_bg']} !important;
}}

#LEFT-MENU {{
  background: {GOV_COLORS['navy']} !important;
  color: {GOV_COLORS['white']} !important;
  border-right: 3px solid {GOV_COLORS['gold']} !important;
  padding: 18px !important;
  font-size: 14px !important;
  font-weight: 700 !important;
}}

.dashboard-component-chart-holder {{
  background: {GOV_COLORS['white']} !important;
  border: 1px solid {GOV_COLORS['border']} !important;
  border-radius: 10px !important;
}}

[id^="CHART-KPI-"] .big-number {{
  font-size: 28px !important;
  font-weight: 900 !important;
  text-align: center !important;
  color: {GOV_COLORS['navy']} !important;
}}

[id^="CHART-KPI-"] .subheader-line {{
  font-size: 12px !important;
  font-weight: 700 !important;
  text-align: center !important;
  color: {GOV_COLORS['gold']} !important;
}}
"""

def metric(col, label, agg="SUM"):
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": col},
        "aggregate": agg,
        "label": label,
    }

def params(dataset, viz, **opts):
    base = {
        "datasource": f"{dataset.id}__table",
        "viz_type": viz,
        "row_limit": 10000,
        "dashboards": [DASHBOARD_ID],
    }
    base.update(opts)
    return base

def update_dashboard():
    app = create_app()
    with app.app_context():
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        dashboard = db.session.query(Dashboard).get(DASHBOARD_ID)

        overview = db.session.query(SqlaTable).filter_by(
            table_name="cns_pdm_executive_overview",
            schema="consumption"
        ).one()

        geo = db.session.query(SqlaTable).filter_by(
            table_name="cns_pdm_geographic_risk",
            schema="consumption"
        ).one()

        intervention = db.session.query(SqlaTable).filter_by(
            table_name="cns_pdm_loan_intervention_dashboard",
            schema="consumption"
        ).one()

        charts = {}

        # --- VALID KPI COLUMNS ONLY ---
        kpi_defs = [
            ("TOTAL APPROVED", "total_approved_amount"),
            ("TOTAL SETTLED", "total_settled_amount"),
            ("TOTAL CREDITED", "total_credited_amount"),
            ("OUTSTANDING", "total_outstanding_amount"),
            ("HIGH RISK CASES", "high_risk_case_count"),
        ]

        for label, col in kpi_defs:
            charts[label] = Slice(
                slice_name=label,
                viz_type="big_number",
                datasource_id=overview.id,
                datasource_type="table",
                params=json.dumps(params(
                    overview,
                    "big_number",
                    metric=metric(col, label),
                    y_axis_format="SMART_NUMBER",
                )),
            )
            db.session.add(charts[label])

        # --- GEOGRAPHIC RISK ---
        charts["GEOGRAPHIC RISK DISTRIBUTION"] = Slice(
            slice_name="GEOGRAPHIC RISK DISTRIBUTION",
            viz_type="country_map",
            datasource_id=geo.id,
            datasource_type="table",
            params=json.dumps(params(
                geo,
                "country_map",
                select_country="uganda",
                entity="district",
                metric=metric("geographic_risk_score", "RISK SCORE", "MAX"),
                linear_color_scheme="schemeYlOrRd",
            )),
        )
        db.session.add(charts["GEOGRAPHIC RISK DISTRIBUTION"])

        # --- INTERVENTION STATUS ---
        charts["LOAN INTERVENTION STATUS"] = Slice(
            slice_name="LOAN INTERVENTION STATUS",
            viz_type="pie",
            datasource_id=intervention.id,
            datasource_type="table",
            params=json.dumps(params(
                intervention,
                "pie",
                groupby=["intervention_status"],
                metric=metric("loan_id", "LOANS", "COUNT_DISTINCT"),
                donut=True,
            )),
        )
        db.session.add(charts["LOAN INTERVENTION STATUS"])

        # --- RISK REGISTER ---
        charts["LOCAL GOVERNMENT RISK REGISTER"] = Slice(
            slice_name="LOCAL GOVERNMENT RISK REGISTER",
            viz_type="table",
            datasource_id=geo.id,
            datasource_type="table",
            params=json.dumps(params(
                geo,
                "table",
                all_columns=[
                    "region", "district", "parish",
                    "geographic_risk_band", "loan_count",
                    "outstanding_amount", "principal_repayment_rate",
                    "high_identity_alert_count",
                ],
                row_limit=50,
            )),
        )
        db.session.add(charts["LOCAL GOVERNMENT RISK REGISTER"])

        # Assign slice IDs before building the layout
        db.session.flush()

        # --- LAYOUT ---
        layout = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
            "GRID_ID": {
                "id": "GRID_ID",
                "type": "GRID",
                "children": ["LEFT-MENU", "ROW-KPI", "ROW-RISK", "ROW-INTERVENTION", "ROW-OVERSIGHT"],
                "parents": ["ROOT_ID"],
            },
            "HEADER_ID": {
                "id": "HEADER_ID",
                "type": "HEADER",
                "meta": {"text": DASHBOARD_TITLE},
                "children": [],
            },

            "LEFT-MENU": {
                "id": "LEFT-MENU",
                "type": "MARKDOWN",
                "meta": {
                    "code": "### Navigation\n- KPIs\n- Geographic Risk\n- Intervention\n- Oversight",
                    "width": 2,
                    "height": 100,
                },
                "parents": ["ROOT_ID", "GRID_ID"],
                "children": [],
            },
        }

        # KPI ROW
        layout["ROW-KPI"] = {
            "id": "ROW-KPI",
            "type": "ROW",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID"],
        }

        for idx, name in enumerate(kpi_defs):
            label = name[0]
            cid = f"CHART-KPI-{idx}"
            colid = f"COLUMN-KPI-{idx}"
            layout["ROW-KPI"]["children"].append(colid)
            layout[colid] = {
                "id": colid,
                "type": "COLUMN",
                "meta": {"width": 2},
                "parents": ["ROOT_ID", "GRID_ID", "ROW-KPI"],
                "children": [cid],
            }
            layout[cid] = {
                "id": cid,
                "type": "CHART",
                "meta": {
                    "chartId": charts[label].id,
                    "width": 2,
                    "height": 14,
                },
                "parents": ["ROOT_ID", "GRID_ID", "ROW-KPI", colid],
                "children": [],
            }

        # RISK ROW
        layout["ROW-RISK"] = {
            "id": "ROW-RISK",
            "type": "ROW",
            "children": ["COLUMN-RISK"],
            "parents": ["ROOT_ID", "GRID_ID"],
        }

        layout["COLUMN-RISK"] = {
            "id": "COLUMN-RISK",
            "type": "COLUMN",
            "meta": {"width": 12},
            "parents": ["ROOT_ID", "GRID_ID", "ROW-RISK"],
            "children": ["CHART-RISK"],
        }

        layout["CHART-RISK"] = {
            "id": "CHART-RISK",
            "type": "CHART",
            "meta": {
                "chartId": charts["GEOGRAPHIC RISK DISTRIBUTION"].id,
                "width": 12,
                "height": 26,
            },
            "parents": ["ROOT_ID", "GRID_ID", "ROW-RISK", "COLUMN-RISK"],
            "children": [],
        }

        # INTERVENTION ROW
        layout["ROW-INTERVENTION"] = {
            "id": "ROW-INTERVENTION",
            "type": "ROW",
            "children": ["COLUMN-INT"],
            "parents": ["ROOT_ID", "GRID_ID"],
        }

        layout["COLUMN-INT"] = {
            "id": "COLUMN-INT",
            "type": "COLUMN",
            "meta": {"width": 12},
            "parents": ["ROOT_ID", "GRID_ID", "ROW-INTERVENTION"],
            "children": ["CHART-INT"],
        }

        layout["CHART-INT"] = {
            "id": "CHART-INT",
            "type": "CHART",
            "meta": {
                "chartId": charts["LOAN INTERVENTION STATUS"].id,
                "width": 12,
                "height": 26,
            },
            "parents": ["ROOT_ID", "GRID_ID", "ROW-INTERVENTION", "COLUMN-INT"],
            "children": [],
        }

        # OVERSIGHT ROW
        layout["ROW-OVERSIGHT"] = {
            "id": "ROW-OVERSIGHT",
            "type": "ROW",
            "children": ["COLUMN-OV"],
            "parents": ["ROOT_ID", "GRID_ID"],
        }

        layout["COLUMN-OV"] = {
            "id": "COLUMN-OV",
            "type": "COLUMN",
            "meta": {"width": 12},
            "parents": ["ROOT_ID", "GRID_ID", "ROW-OVERSIGHT"],
            "children": ["CHART-OV"],
        }

        layout["CHART-OV"] = {
            "id": "CHART-OV",
            "type": "CHART",
            "meta": {
                "chartId": charts["LOCAL GOVERNMENT RISK REGISTER"].id,
                "width": 12,
                "height": 26,
            },
            "parents": ["ROOT_ID", "GRID_ID", "ROW-OVERSIGHT", "COLUMN-OV"],
            "children": [],
        }

        dashboard.position_json = json.dumps(layout)
        dashboard.css = DASHBOARD_CSS
        dashboard.dashboard_title = DASHBOARD_TITLE
        dashboard.json_metadata = json.dumps({
            "label_colors": LABEL_COLORS,
            "color_scheme": "supersetColors",
        })
        dashboard.published = True

        db.session.commit()
        print("FINAL GOVERNMENT DASHBOARD UPDATED SUCCESSFULLY.")


if __name__ == "__main__":
    update_dashboard()
