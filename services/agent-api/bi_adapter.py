import json
from typing import Any, Protocol

import requests
from pydantic import BaseModel, Field

from authority import Authority
from bi_policy import check_bi_publication
from builder_models import DashboardSpec, DataSourceSpec, VisualizationSpec
from classification import ClassificationError, TrustedClassificationResolver
from config import Settings
from governance_models import DataClassification
from models import Permissions


class AdapterError(RuntimeError): pass
class AdapterPermissionDenied(AdapterError): pass


class PublicationRequest(BaseModel):
    dashboard: DashboardSpec
    publish: bool = False
    permissions: Permissions = Field(default_factory=Permissions)


class DashboardAgentRequest(BaseModel):
    objective: str
    publish: bool = False
    # Governance authority is mandatory for governed reads (A2.1), so the
    # request must be able to carry it.
    context: dict[str, Any] = Field(default_factory=dict)
    permissions: Permissions = Field(default_factory=Permissions)


class BIAdapter(Protocol):
    def plan(self, dashboard: DashboardSpec) -> dict[str, Any]: ...
    def publish(self, dashboard: DashboardSpec, permissions: Permissions) -> dict[str, Any]: ...


SUPERSET_TYPES = {"kpi": "big_number_total", "table": "table",
                  "bar": "echarts_timeseries_bar", "line": "echarts_timeseries_line",
                  "scatter": "bubble"}


class SupersetAdapter:
    """Replaceable REST adapter; core builder models contain no Superset fields."""
    def __init__(
        self,
        settings: Settings,
        session=None,
        classification_resolver: TrustedClassificationResolver | None = None,
    ):
        self.settings = settings
        self.session = session or requests.Session()
        self.classification_resolver = (
            classification_resolver
            if classification_resolver is not None
            else TrustedClassificationResolver()
        )

    def plan(self, dashboard: DashboardSpec) -> dict[str, Any]:
        self._validated(dashboard)
        sources = {v.data_source.id: v.data_source for v in dashboard.visualizations}
        return {"mode": "dry_run", "adapter": "superset", "dashboard_spec_id": dashboard.id,
            "actions": ([{"action": "upsert_dataset", "stable_id": source.id,
                           "dataset": source.dataset} for source in sources.values()]
                        + [{"action": "upsert_chart", "stable_id": chart.id,
                            "superset_type": SUPERSET_TYPES[chart.type]} for chart in dashboard.visualizations]
                        + [{"action": "upsert_dashboard", "stable_id": dashboard.id,
                            "title": dashboard.title}]),
            "layout": self.translate_layout(dashboard),
            "filters": self.translate_filters(dashboard), "status": "validated"}

    def publish(self, dashboard: DashboardSpec, authority) -> dict[str, Any]:
        """Publish a dashboard using SERVER-DERIVED authority.

        A2.2/A2.7: ``authority`` must be an `authority.Authority` issued by the
        server-side resolver.  A caller-supplied capability model such as
        ``models.Permissions`` is not accepted here and can never authorise a
        publication, no matter what it contains.
        """

        self._require_publication_authority(authority)

        self._validate_publication_governance(dashboard)
        self._require_publisher_identity()
        plan = self.plan(dashboard)
        headers = self._login()
        database_id = self._database_id(headers)
        datasets: dict[str, int] = {}
        charts: dict[str, int] = {}
        completed = []
        try:
            for visual in dashboard.visualizations:
                source = visual.data_source
                if source.id not in datasets:
                    datasets[source.id] = self._upsert_dataset(source, database_id, headers)
                    completed.append({"kind": "dataset", "spec_id": source.id,
                                      "superset_id": datasets[source.id]})
                charts[visual.id] = self._upsert_chart(visual, datasets[source.id], headers)
                completed.append({"kind": "chart", "spec_id": visual.id,
                                  "superset_id": charts[visual.id]})
            dashboard_id = self._upsert_dashboard(dashboard, charts, datasets, headers)
            completed.append({"kind": "dashboard", "spec_id": dashboard.id,
                              "superset_id": dashboard_id})
            self._associate_charts(charts.values(), dashboard_id, headers)
        except Exception as exc:
            raise AdapterError(f"Superset publication failed after {completed}: {exc}") from exc
        return {**plan, "mode": "published", "status": "completed", "artifacts": completed,
                "dashboard_url": f"{self.settings.superset_public_url}/superset/dashboard/{dashboard_id}/"}

    @staticmethod
    def _require_publication_authority(authority) -> None:
        """Fail closed unless publication authority came from the server.

        The type check is a real structural barrier, not a naming convention:
        `authority.Authority` is a frozen dataclass that is never built from
        request payload data, so no caller-supplied model can satisfy it.
        """

        if not isinstance(authority, Authority):
            raise AdapterPermissionDenied(
                "BI publishing permission is required and must be "
                "server-authorised; request-supplied capabilities are not "
                "accepted as publication authority"
            )

        if not authority.permits(
            "can_publish_bi_assets",
            require_authenticated=True,
        ):
            raise AdapterPermissionDenied(
                "BI publishing permission is required and must be "
                "server-authorised; request-supplied capabilities are not "
                "accepted as publication authority"
            )

    def _validate_publication_governance(self, dashboard: DashboardSpec) -> None:
        """
        Fail closed before any Superset side effect.

        Superset publication registers the physical source dataset, so a
        RESTRICTED dataset cannot be published even when a particular chart
        happens to use only a public-looking field. Publish a governed aggregate
        or reporting dataset instead.

        Restricted projected/filter fields are also rejected explicitly.
        """

        fields_by_source: dict[str, set[str]] = {}
        sources: dict[str, Any] = {}

        for visual in dashboard.visualizations:
            source = visual.data_source
            sources[source.id] = source
            fields_by_source.setdefault(source.id, set()).update(
                encoding.field
                for encoding in visual.encoding
            )

        for dashboard_filter in dashboard.filters:
            fields_by_source.setdefault(
                dashboard_filter.data_source_id,
                set(),
            ).add(dashboard_filter.field)

        for source_id, source in sources.items():
            try:
                resource = self.classification_resolver.resolve_resource(
                    source.dataset,
                    sorted(fields_by_source.get(source_id, set())),
                )
            except ClassificationError as exc:
                raise AdapterPermissionDenied(
                    "BI publication denied because trusted data "
                    f"classification failed for {source.dataset}: {exc}"
                ) from exc

            if resource.classification == DataClassification.RESTRICTED:
                raise AdapterPermissionDenied(
                    "BI publication denied for RESTRICTED dataset "
                    f"{source.dataset}; publish a governed aggregate "
                    "or reporting dataset instead"
                )

            #
            # F2-2: publication is bounded by the SERVER-OWNED approved BI
            # allowlist. Holding the bi_publisher role plus an INTERNAL
            # classification is NOT sufficient; the dataset itself must be
            # approved for automated BI publication. A caller cannot extend
            # this policy.
            #
            refusal = check_bi_publication(source.dataset)

            if refusal is not None:
                raise AdapterPermissionDenied(
                    "BI publication denied because "
                    f"{refusal.reason}: {refusal.dataset}"
                )

            restricted_fields = sorted(
                field.field
                for field in resource.field_classifications
                if field.classification == DataClassification.RESTRICTED
            )

            if restricted_fields:
                raise AdapterPermissionDenied(
                    "BI publication denied because restricted fields are "
                    f"present in {source.dataset}: "
                    + ", ".join(restricted_fields)
                )

    @staticmethod
    def _validated(dashboard):
        if dashboard.status != "validated" or any(v.status != "validated" or
                v.data_source.status != "validated" for v in dashboard.visualizations):
            raise AdapterError("adapter accepts only validated builder specifications")

    @staticmethod
    def translate_layout(dashboard):
        return [{"id": item.visualization_id, "x": item.x, "y": item.y,
                 "width": item.width, "height": item.height} for item in dashboard.layout]

    @staticmethod
    def translate_filters(dashboard):
        return [{"field": item.field, "data_source_id": item.data_source_id,
                 "filter_type": "filter_select"} for item in dashboard.filters]

    def chart_payload(self, visual: VisualizationSpec, dataset_id: int) -> dict[str, Any]:
        dimensions = [e.field for e in visual.encoding if e.role == "dimension"]
        measures = [e for e in visual.encoding if e.role == "measure"]
        def metric(item):
            aggregate = (item.aggregation or "SUM").upper().replace("COUNT_DISTINCT", "COUNT_DISTINCT")
            return {"expressionType": "SIMPLE", "column": {"column_name": item.field},
                    "aggregate": aggregate, "label": item.field,
                    "optionName": f"metric_{item.field}_{aggregate.lower()}"}
        params: dict[str, Any] = {"viz_type": SUPERSET_TYPES[visual.type],
                                 "datasource": f"{dataset_id}__table", "row_limit": visual.limit}
        if visual.type == "kpi": params.update(metric=metric(measures[0]), show_metric_name=True)
        elif visual.type == "table": params.update(all_columns=[e.field for e in visual.encoding], metrics=[])
        elif visual.type in {"bar", "line"}:
            params.update(groupby=dimensions, metrics=[metric(item) for item in measures],
                          orientation="vertical", show_legend=True)
        elif visual.type == "scatter":
            params.update(entity=dimensions[0] if dimensions else None,
                          x=metric(measures[0]), y=metric(measures[1]))
        return {"slice_name": f"[{visual.id}] {visual.title}",
                "viz_type": SUPERSET_TYPES[visual.type], "datasource_id": dataset_id,
                "datasource_type": "table", "params": json.dumps(params, separators=(",", ":")),
                "description": f"Agent artifact {visual.id}"}

    def _require_publisher_identity(self) -> None:
        """Fail closed unless the dedicated publisher identity is configured.

        F2-1: automated publication must never authenticate as the Superset
        administrator, and must never fall back to SUPERSET_ADMIN_* when the
        publisher credential is absent.
        """

        username = (self.settings.superset_publisher_username or "").strip()

        if not username or not (self.settings.superset_publisher_password or ""):
            raise AdapterError(
                "SUPERSET_PUBLISHER_USERNAME and SUPERSET_PUBLISHER_PASSWORD are "
                "required for publishing; automated publication never falls "
                "back to administrator credentials"
            )

    def _login(self):
        self._require_publisher_identity()
        response = self.session.post(f"{self.settings.superset_url}/api/v1/security/login",
            json={"username": self.settings.superset_publisher_username,
                  "password": self.settings.superset_publisher_password, "provider": "db", "refresh": True}, timeout=20)
        response.raise_for_status()
        headers = {"Authorization": f"Bearer {response.json()['access_token']}"}
        csrf = self.session.get(f"{self.settings.superset_url}/api/v1/security/csrf_token/",
                                headers=headers, timeout=20)
        csrf.raise_for_status(); headers["X-CSRFToken"] = csrf.json()["result"]
        return headers

    def _get(self, path, headers, query):
        response = self.session.get(f"{self.settings.superset_url}{path}", headers=headers,
                                    params={"q": query}, timeout=30)
        response.raise_for_status(); return response.json().get("result", [])

    def _database_id(self, headers):
        rows = self._get("/api/v1/database/", headers,
            f"(filters:!((col:database_name,opr:eq,value:'{self.settings.superset_database_name}')))" )
        if not rows: raise AdapterError(f"Superset database not found: {self.settings.superset_database_name}")
        return rows[0]["id"]

    def _write(self, method, path, headers, payload):
        response = self.session.request(method, f"{self.settings.superset_url}{path}",
                                        headers=headers, json=payload, timeout=30)
        if not response.ok:
            raise AdapterError(f"{method} {path} returned {response.status_code}: {response.text[:1000]}")
        body = response.json(); return body.get("id") or body.get("result", {}).get("id")

    def _upsert_dataset(self, source, database_id, headers):
        catalog, schema, table = source.dataset.split(".")
        rows = self._get("/api/v1/dataset/", headers,
            f"(filters:!((col:database,opr:rel_o_m,value:{database_id}),(col:schema,opr:eq,value:'{schema}'),(col:table_name,opr:eq,value:'{table}')))" )
        if rows: return rows[0]["id"]
        return self._write("POST", "/api/v1/dataset/", headers,
                           {"database": database_id, "schema": schema, "table_name": table})

    def _upsert_chart(self, visual, dataset_id, headers):
        name = f"[{visual.id}] {visual.title}"
        rows = self._get("/api/v1/chart/", headers,
                         f"(filters:!((col:slice_name,opr:eq,value:'{name}')))" )
        payload = self.chart_payload(visual, dataset_id)
        return (self._write("PUT", f"/api/v1/chart/{rows[0]['id']}", headers, payload)
                or rows[0]["id"]) if rows else self._write("POST", "/api/v1/chart/", headers, payload)

    def _upsert_dashboard(self, dashboard, charts, datasets, headers):
        slug = dashboard.id.replace("_", "-")
        rows = self._get("/api/v1/dashboard/", headers,
                         f"(filters:!((col:slug,opr:eq,value:'{slug}')))" )
        position = self._position_json(dashboard, charts)
        # Superset validates dashboard metadata keys strictly. Stable provenance
        # lives in the dashboard slug and returned artifact map; only supported
        # native-filter metadata is persisted here.
        metadata = {"native_filter_configuration": self.translate_filters(dashboard)}
        # F3-4: a dashboard is materialised in the Superset workspace only.
        # `published` is left False and no anonymous/public role is assigned,
        # so repository-controlled publication can never make an
        # identity-bearing asset anonymously reachable. Operator role binding
        # is a runtime concern (F3_RUNTIME_DASHBOARD_ACCESS_REVIEW).
        payload = {"dashboard_title": dashboard.title, "slug": slug, "published": False,
                   "position_json": json.dumps(position, separators=(",", ":")),
                   "json_metadata": json.dumps(metadata, separators=(",", ":")),
                   "css": "", "owners": []}
        return (self._write("PUT", f"/api/v1/dashboard/{rows[0]['id']}", headers, payload)
                or rows[0]["id"]) if rows else self._write("POST", "/api/v1/dashboard/", headers, payload)

    def _associate_charts(self, chart_ids, dashboard_id, headers):
        for chart_id in chart_ids:
            self._write("PUT", f"/api/v1/chart/{chart_id}", headers,
                        {"dashboards": [dashboard_id]})

    @staticmethod
    def _position_json(dashboard, charts):
        result = {"DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
            "GRID_ID": {"id": "GRID_ID", "type": "GRID", "parents": ["ROOT_ID"], "children": []},
            "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": dashboard.title}}}
        for index, item in enumerate(dashboard.layout):
            row, node = f"ROW-{index}", f"CHART-{charts[item.visualization_id]}"
            result["GRID_ID"]["children"].append(row)
            result[row] = {"id": row, "type": "ROW", "parents": ["ROOT_ID", "GRID_ID"],
                           "children": [node], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            result[node] = {"id": node, "type": "CHART", "parents": ["ROOT_ID", "GRID_ID", row],
                            "meta": {"chartId": charts[item.visualization_id],
                                     "sliceName": item.visualization_id,
                                     "width": item.width, "height": item.height}}
        return result
