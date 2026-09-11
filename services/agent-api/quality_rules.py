import re
from pathlib import Path
from typing import Any

import yaml

from quality_models import DQRule


def _arguments(configuration: Any) -> dict[str, Any]:
    if not isinstance(configuration, dict):
        return {}
    return configuration.get("arguments", configuration)


class DQRuleCatalogue:
    """Read-only projection of dbt schema tests into vendor-neutral DQ rules."""

    def __init__(self, models_path: str, catalog: str = "iceberg"):
        self.models_path = Path(models_path)
        self.catalog = catalog

    def list_rules(self, dataset: str | None = None) -> list[DQRule]:
        rules: list[DQRule] = []
        if not self.models_path.exists():
            return rules
        for path in sorted((*self.models_path.rglob("*.yml"), *self.models_path.rglob("*.yaml"))):
            document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            schema = self._schema(path)
            for model in document.get("models", []):
                model_name = model.get("name")
                if not model_name:
                    continue
                qualified = f"{self.catalog}.{schema}.{model_name}"
                if dataset and qualified != dataset:
                    continue
                for column in model.get("columns", []):
                    for test in column.get("data_tests", column.get("tests", [])):
                        rule = self._rule(qualified, column.get("name"), test, path)
                        if rule:
                            rules.append(rule)
        return rules

    def get(self, rule_id: str) -> DQRule | None:
        return next((rule for rule in self.list_rules() if rule.rule_id == rule_id), None)

    def _schema(self, path: Path) -> str:
        relative = path.relative_to(self.models_path)
        return relative.parts[0] if len(relative.parts) > 1 else path.stem.replace("_schema", "")

    def _rule(self, dataset: str, field: str | None, test: Any, path: Path) -> DQRule | None:
        if isinstance(test, str):
            name, configuration = test, {}
        elif isinstance(test, dict) and len(test) == 1:
            name, configuration = next(iter(test.items()))
        else:
            return None
        aliases = {"relationships": "referential_integrity"}
        rule_type = aliases.get(name, name)
        if rule_type not in {"not_null", "unique", "accepted_values", "referential_integrity"}:
            return None
        parameters = _arguments(configuration)
        if rule_type == "referential_integrity":
            reference = str(parameters.get("to", ""))
            match = re.fullmatch(r"ref\(['\"]([A-Za-z0-9_]+)['\"]\)", reference)
            if not match or not parameters.get("field"):
                return None
            parameters = {"reference_dataset": f"{self.catalog}.{dataset.split('.')[1]}.{match.group(1)}",
                          "reference_field": parameters["field"]}
        elif rule_type == "accepted_values":
            parameters = {"values": parameters.get("values", [])}
        safe_field = re.sub(r"[^A-Za-z0-9_]+", "_", field or "dataset")
        return DQRule(
            rule_id=f"dbt.{dataset}.{safe_field}.{rule_type}", dataset=dataset,
            rule_type=rule_type, field=field,
            severity="high" if rule_type in {"not_null", "unique", "referential_integrity"} else "medium",
            parameters=parameters, source="dbt_test", provenance=str(path.relative_to(self.models_path)),
        )
