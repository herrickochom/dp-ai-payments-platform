import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    trino_host: str = os.getenv("TRINO_HOST", "localhost")
    trino_port: int = int(os.getenv("TRINO_PORT", "8080"))
    trino_user: str = os.getenv("TRINO_USER", "agent-api")
    trino_catalog: str = os.getenv("TRINO_CATALOG", "iceberg")
    trino_schema: str = os.getenv("TRINO_SCHEMA", "consumption")
    trino_http_scheme: str = os.getenv("TRINO_HTTP_SCHEME", "http")
    trino_password: str | None = os.getenv("TRINO_PASSWORD")
    max_rows: int = int(os.getenv("AGENT_MAX_ROWS", "1000"))
    query_timeout_seconds: int = int(os.getenv("AGENT_QUERY_TIMEOUT_SECONDS", "30"))
    max_query_length: int = int(os.getenv("AGENT_MAX_QUERY_LENGTH", "20000"))
    max_tool_calls: int = int(os.getenv("AGENT_MAX_TOOL_CALLS", "20"))
    model_provider: str = os.getenv("AGENT_MODEL_PROVIDER", "disabled")
    model_name: str = os.getenv("AGENT_MODEL", "")
    rag_enabled: bool = os.getenv("AGENT_RAG_ENABLED", "false").lower() == "true"

