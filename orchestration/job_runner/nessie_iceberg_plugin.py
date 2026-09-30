"""DEPRECATED: DuckDB's Iceberg extension cannot construct valid URLs for
Nessie branches (see docs/governance/...). Superseded by the Trino Iceberg
REST connector configured in platform/trino/etc/catalog/nessie.properties.
Do not use. Scheduled for removal."""
from __future__ import annotations

import os
import re

from dbt.adapters.duckdb.plugins import BasePlugin

from services.shared.security.runtime_security import validate_nessie_security


_BRANCH = re.compile(r"transform_(?:tr|be)_[a-f0-9]{32}")
_WAREHOUSE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}")
_SECRET_NAME = "nessie_transform"
_CATALOG_ALIAS = "lakehouse"


class Plugin(BasePlugin):
    """Configure one DuckDB connection without exposing bearer credentials."""

    def configure_connection(self, conn) -> None:
        endpoint = os.environ.get("NESSIE_ENDPOINT", "").strip()
        token = os.environ.get("NESSIE_TRANSFORM_TOKEN", "")
        branch = os.environ.get("DBT_NESSIE_BRANCH", "").strip()
        warehouse = os.environ.get("NESSIE_WAREHOUSE", "").strip()

        validate_nessie_security(
            endpoint,
            auth_mode=os.getenv("NESSIE_AUTH_MODE", "bearer"),
            token=token,
        )
        if not token:
            raise ValueError("Nessie transform token is required")
        if not _BRANCH.fullmatch(branch):
            raise ValueError("invalid execution-scoped Nessie branch")
        if not _WAREHOUSE.fullmatch(warehouse):
            raise ValueError("invalid Nessie warehouse name")

        # Nessie 0.108.x advertises iceberg-base-uri <endpoint>/iceberg and
        # prefix-pattern "{ref}|{warehouse}", so the execution branch never
        # appears in the endpoint (a branch segment there returns 404 on
        # every REST operation).
        # ATTACH the warehouse via the Nessie Iceberg REST endpoint.
        # The branch is in the endpoint path; the warehouse name is the
        # ATTACH identifier. ACCESS_DELEGATION_MODE 'none' tells DuckDB to
        # use the locally configured S3 credentials instead of requesting
        # vended credentials from Nessie.
        catalog_endpoint = f"{endpoint.rstrip('/')}/iceberg/{branch}"
        conn.execute(
            f"CREATE SECRET {_SECRET_NAME} (TYPE iceberg, TOKEN ?)",
            [token],
        )
        conn.execute(
            f"ATTACH '{warehouse}' AS {_CATALOG_ALIAS} "
            f"(TYPE iceberg, ENDPOINT ?, "
            f"SECRET {_SECRET_NAME}, ACCESS_DELEGATION_MODE 'none')",
            [catalog_endpoint],
        )
