import os
from dataclasses import dataclass
from pathlib import Path

from services.shared.security.secret_provider import resolve_secret


_config_file = Path(__file__).resolve()
_repository_dbt_models = (
    _config_file.parents[2] / "transform" / "dbt" / "models"
    if len(_config_file.parents) > 2 else Path("/app/dbt-models")
)


@dataclass(frozen=True)
class Settings:
    trino_host: str = os.getenv("TRINO_HOST", "localhost")
    trino_port: int = int(os.getenv("TRINO_PORT", "8080"))
    trino_user: str = os.getenv("TRINO_USER", "agent-api")
    trino_catalog: str = os.getenv("TRINO_CATALOG", "iceberg")
    trino_schema: str = os.getenv("TRINO_SCHEMA", "consumption")
    trino_http_scheme: str = os.getenv("TRINO_HTTP_SCHEME", "http")
    trino_password: str | None = resolve_secret("TRINO_PASSWORD")
    trino_tls_ca: str | None = os.getenv("TRINO_TLS_CA")
    max_rows: int = int(os.getenv("AGENT_MAX_ROWS", "1000"))
    query_timeout_seconds: int = int(os.getenv("AGENT_QUERY_TIMEOUT_SECONDS", "30"))
    max_query_length: int = int(os.getenv("AGENT_MAX_QUERY_LENGTH", "20000"))
    max_tool_calls: int = int(os.getenv("AGENT_MAX_TOOL_CALLS", "20"))
    model_provider: str = os.getenv("AGENT_MODEL_PROVIDER", "disabled")
    model_name: str = os.getenv("AGENT_MODEL", "")
    rag_enabled: bool = os.getenv("AGENT_RAG_ENABLED", "false").lower() == "true"
    superset_url: str = os.getenv("SUPERSET_URL", "http://localhost:8088")
    superset_username: str = os.getenv("SUPERSET_USERNAME", "admin")
    superset_password: str | None = resolve_secret("SUPERSET_PASSWORD")
    superset_database_name: str = os.getenv("SUPERSET_DATABASE_NAME", "PDM Trino")
    superset_public_url: str = os.getenv("SUPERSET_PUBLIC_URL", "http://localhost:8088")
    dbt_models_path: str = os.getenv(
        "DBT_MODELS_PATH",
        str(_repository_dbt_models if _repository_dbt_models.exists() else Path("/app/dbt-models")),
    )
    dq_max_rules_per_request: int = int(os.getenv("DQ_MAX_RULES_PER_REQUEST", "4"))
    dq_max_sample_rows: int = int(os.getenv("DQ_MAX_SAMPLE_ROWS", "5"))
    insight_change_threshold: float = float(os.getenv("INSIGHT_CHANGE_THRESHOLD", "0.20"))
    audit_log_path: str = os.getenv("AGENT_AUDIT_LOG_PATH", "/tmp/dp-agent/audit.jsonl")
    request_timeout_seconds: int = int(os.getenv("AGENT_REQUEST_TIMEOUT_SECONDS", "60"))
    tool_timeout_seconds: int = int(os.getenv("AGENT_TOOL_TIMEOUT_SECONDS", "10"))
    tool_max_retries: int = int(os.getenv("AGENT_TOOL_MAX_RETRIES", "1"))
    max_request_bytes: int = int(os.getenv("AGENT_MAX_REQUEST_BYTES", "1000000"))
    auth_enabled: bool = os.getenv("AGENT_AUTH_ENABLED", "false").lower() == "true"

    def __post_init__(self):
        if not 1 <= self.trino_port <= 65535:
            raise ValueError("TRINO_PORT must be between 1 and 65535")
        if self.trino_http_scheme not in {"http", "https"}:
            raise ValueError("TRINO_HTTP_SCHEME must be http or https")
        if self.max_rows < 1 or self.max_tool_calls < 1:
            raise ValueError("agent limits must be positive")
        if self.query_timeout_seconds < 1 or self.request_timeout_seconds < 1:
            raise ValueError("timeouts must be positive")
        if not 1 <= self.tool_timeout_seconds <= 120:
            raise ValueError("AGENT_TOOL_TIMEOUT_SECONDS must be between 1 and 120")
        if not 0 <= self.tool_max_retries <= 3:
            raise ValueError("AGENT_TOOL_MAX_RETRIES must be between 0 and 3")
        if self.max_request_bytes < 1024:
            raise ValueError("AGENT_MAX_REQUEST_BYTES must be at least 1024")
