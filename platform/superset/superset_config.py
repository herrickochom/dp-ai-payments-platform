# =============================================================================
# Apache Superset Configuration
# PDM Analytics Platform
# =============================================================================

import os


# =============================================================================
# APPLICATION SECURITY
# =============================================================================

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

WTF_CSRF_ENABLED = True
ENABLE_PROXY_FIX = True

SQLALCHEMY_TRACK_MODIFICATIONS = False

# =============================================================================
# SUPERSET METADATA DATABASE
#
# This is NOT the PDM analytics database.
# It stores Superset dashboards, charts, users, roles, datasets, etc.
# =============================================================================

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

SUPERSET_METADATA_DB = os.getenv("SUPERSET_METADATA_DB", "superset")

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://"
    f"{POSTGRES_USER}:"
    f"{POSTGRES_PASSWORD}@"
    f"{POSTGRES_HOST}:"
    f"{POSTGRES_PORT}/"
    f"{SUPERSET_METADATA_DB}"
)


# =============================================================================
# REDIS
#
# Docker Compose service name is "redis".
# =============================================================================

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


# =============================================================================
# CACHE CONFIGURATION
# =============================================================================

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "pdm_superset_metadata_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 0,
}


DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "pdm_superset_data_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 1,
}


FILTER_STATE_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,
    "CACHE_KEY_PREFIX": "pdm_superset_filter_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 2,
}


EXPLORE_FORM_DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,
    "CACHE_KEY_PREFIX": "pdm_superset_explore_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 3,
}


# =============================================================================
# CELERY / ASYNC QUERIES
#
# This prepares Superset for SQL Lab async queries and later scheduled
# reports. A separate Celery worker service is required before relying on
# asynchronous execution.
# =============================================================================

class CeleryConfig:
    broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/4"

    imports = (
        "superset.sql_lab",
    )

    result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/5"

    worker_prefetch_multiplier = 10
    task_track_started = True
    task_acks_late = True


CELERY_CONFIG = CeleryConfig


# Keep disabled until a dedicated SQL Lab results backend is required.
RESULTS_BACKEND = None


# =============================================================================
# FEATURE FLAGS
# =============================================================================

FEATURE_FLAGS = {
    # Jinja/template support in SQL Lab / datasets
    "ENABLE_TEMPLATE_PROCESSING": True,

    # Dashboard-level role-based access
    "DASHBOARD_RBAC": True,

    # Drill from charts into underlying records
    "DRILL_TO_DETAIL": True,

    # Cross-filter dashboard interactions
    "DASHBOARD_CROSS_FILTERS": True,

    # Modern dashboard filtering
    "DASHBOARD_NATIVE_FILTERS": True,
}


# =============================================================================
# QUERY / DASHBOARD LIMITS
# =============================================================================

ROW_LIMIT = 100000
SQL_MAX_ROW = 100000

SUPERSET_WEBSERVER_TIMEOUT = 120
SQLLAB_ASYNC_TIME_LIMIT_SEC = 300


# =============================================================================
# APPLICATION BRANDING
# =============================================================================

APP_NAME = "PDM Analytics"


# =============================================================================
# CSV / EXPORT
# =============================================================================

CSV_EXTENSIONS = {"csv"}
