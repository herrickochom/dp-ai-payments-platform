from typing import Any

from builder_models import (DashboardBuildRequest, DataSourceSpec, LayoutItem,
                            VisualizationSpec)
from builders import recommend_visualizations
from models import Evidence, Permissions
from tools import PermissionDenied, ToolRegistry


class VisualizationAgent:
    identity = "visualization"

    def __init__(self, analytics): self.analytics = analytics

    def run(self, objective: str, permissions: Permissions, tools: ToolRegistry):
        if not permissions.can_build_visualizations:
            raise PermissionDenied("visualization build permission is required")
        analytical, evidence, warnings = self.analytics.run(objective, permissions, tools)
        raw_source = analytical.get("data_source")
        if not raw_source:
            return {"analytical_result": analytical, "visualizations": []}, evidence, warnings
        source = DataSourceSpec.model_validate(raw_source)
        # Deterministic fallback is the Phase 3 default. A future configured
        # provider may propose specs, but every proposal must use this same
        # builder validation path.
        proposals = recommend_visualizations(source, objective, permissions)
        visualizations = [tools.build_visualization(item) for item in proposals]
        evidence.extend(Evidence(kind="visualization_agent", reference=item.id,
            details={"strategy": "deterministic_fallback", "type": item.type})
                        for item in visualizations)
        return {"analytical_result": analytical,
                "visualizations": [item.model_dump() for item in visualizations],
                "strategy": "deterministic_fallback"}, evidence, warnings


class DashboardAgent:
    identity = "dashboard"

    def __init__(self, visualization_agent): self.visualization_agent = visualization_agent

    def run(self, objective: str, permissions: Permissions, tools: ToolRegistry):
        if not permissions.can_build_dashboards:
            raise PermissionDenied("dashboard build permission is required")
        visual_result, evidence, warnings = self.visualization_agent.run(objective, permissions, tools)
        visualizations = [VisualizationSpec.model_validate(item)
                          for item in visual_result["visualizations"]]
        if not visualizations:
            return {**visual_result, "dashboard": None}, evidence, warnings
        layout = [LayoutItem(visualization_id=item.id, x=0, y=index * 4,
                             width=12, height=4) for index, item in enumerate(visualizations)]
        dashboard = tools.build_dashboard(DashboardBuildRequest(
            title=objective.rstrip("."), description="Composed by the bounded Dashboard Agent",
            visualizations=visualizations, layout=layout, permissions=permissions))
        evidence.append(Evidence(kind="dashboard_agent", reference=dashboard.id,
            details={"strategy": "deterministic_fallback",
                     "primary_visualization": visualizations[0].id}))
        return {**visual_result, "dashboard": dashboard.model_dump(),
                "primary_visualization": visualizations[0].id}, evidence, warnings

