from typing import Any

import trino
from trino.auth import BasicAuthentication

from config import Settings


class TrinoGateway:
    def __init__(self, settings: Settings):
        self.settings = settings

    def _connection(self):
        kwargs: dict[str, Any] = {
            "host": self.settings.trino_host,
            "port": self.settings.trino_port,
            "user": self.settings.trino_user,
            "catalog": self.settings.trino_catalog,
            "schema": self.settings.trino_schema,
            "http_scheme": self.settings.trino_http_scheme,
            "request_timeout": self.settings.query_timeout_seconds,
            "session_properties": {
                "query_max_execution_time": f"{self.settings.query_timeout_seconds}s"
            },
        }
        if self.settings.trino_password:
            kwargs["auth"] = BasicAuthentication(
                self.settings.trino_user, self.settings.trino_password
            )
        return trino.dbapi.connect(**kwargs)

    def execute(self, sql: str) -> tuple[list[str], list[list[Any]], str | None]:
        connection = self._connection()
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            columns = [item[0] for item in (cursor.description or [])]
            return columns, [list(row) for row in rows], getattr(cursor, "query_id", None)
        finally:
            cursor.close()
            connection.close()

    def health(self) -> dict[str, Any]:
        columns, rows, query_id = self.execute("SELECT 1 AS healthy")
        return {"status": "healthy" if rows == [[1]] else "unhealthy", "query_id": query_id}

