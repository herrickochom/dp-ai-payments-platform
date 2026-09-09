# =============================================================================
# Apache Superset 6.1 Configuration
# PDM National Executive Intelligence Platform
# =============================================================================

from __future__ import annotations

import os
from datetime import timedelta

from cachelib.redis import RedisCache


# =============================================================================
# APPLICATION / SECURITY
# =============================================================================

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

WTF_CSRF_ENABLED = True
ENABLE_PROXY_FIX = True
SQLALCHEMY_TRACK_MODIFICATIONS = False
HASH_ALGORITHM = "sha256"

SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"
PERMANENT_SESSION_LIFETIME = timedelta(hours=12)
SESSION_COOKIE_SECURE = (
    os.getenv("SUPERSET_SESSION_COOKIE_SECURE", "false").lower() == "true"
)


# =============================================================================
# SUPERSET METADATA DATABASE
# =============================================================================

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
SUPERSET_METADATA_DB = os.getenv("SUPERSET_METADATA_DB", "superset")

SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://"
    f"{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{SUPERSET_METADATA_DB}"
)

SQLALCHEMY_ENGINE_OPTIONS = {
    "pool_pre_ping": True,
    "pool_recycle": 300,
    "pool_size": 10,
    "max_overflow": 20,
}


# =============================================================================
# REDIS / CACHE
# =============================================================================

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

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

RESULTS_BACKEND = RedisCache(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=6,
    key_prefix="pdm_superset_results_",
)


# =============================================================================
# CELERY
# =============================================================================

class CeleryConfig:
    broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/4"
    result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/5"

    imports = (
        "superset.sql_lab",
        "superset.tasks.cache",
    )

    worker_prefetch_multiplier = 1
    task_track_started = True
    task_acks_late = True
    task_time_limit = 600
    task_soft_time_limit = 540


CELERY_CONFIG = CeleryConfig


# =============================================================================
# FEATURE FLAGS
# =============================================================================

FEATURE_FLAGS = {
    # Keep enabled only for trusted dataset/chart authors.
    "ENABLE_TEMPLATE_PROCESSING": True,

    # Executive dashboard security and interaction.
    "DASHBOARD_RBAC": True,
    "DRILL_TO_DETAIL": True,
    "DASHBOARD_CROSS_FILTERS": True,

    # Superset 6.1 granular export permissions.
    "GRANULAR_EXPORT_CONTROLS": True,

    # Requires additional WebSocket infrastructure; keep off for this phase.
    "GLOBAL_ASYNC_QUERIES": False,

    # Requires browser + celery beat + SMTP/Slack/Webhook configuration.
    "ALERT_REPORTS": False,
}


# =============================================================================
# SUPERSET 6.x THEMING
# =============================================================================

ENABLE_UI_THEME_ADMINISTRATION = True

THEME_DEFAULT = {
    "token": {
        "brandAppName": "PDM Executive Intelligence",
        "colorPrimary": "#0F4C5C",
        "colorInfo": "#2563EB",
        "colorSuccess": "#2E7D32",
        "colorWarning": "#D4A017",
        "colorError": "#B42318",
        "colorText": "#17202A",
        "colorTextSecondary": "#5B6573",
        "colorBgLayout": "#F5F7FA",
        "colorBgContainer": "#FFFFFF",
        "colorBorderSecondary": "#E4E8EE",
        "borderRadius": 8,
        "borderRadiusLG": 12,
        "fontFamily": "Inter, -apple-system, BlinkMacSystemFont, sans-serif",
        "fontSize": 14,
    },
    "echartsOptionsOverrides": {
        "animationDuration": 350,
        "grid": {
            "left": "6%",
            "right": "5%",
            "top": "12%",
            "bottom": "12%",
            "containLabel": True,
        },
        "tooltip": {
            "trigger": "axis",
            "backgroundColor": "rgba(17, 24, 39, 0.94)",
            "borderWidth": 0,
            "textStyle": {
                "color": "#FFFFFF",
                "fontFamily": "Inter, sans-serif",
            },
        },
        "legend": {
            "top": 4,
            "left": "center",
            "itemGap": 18,
            "textStyle": {
                "color": "#374151",
                "fontSize": 12,
            },
        },
        "textStyle": {
            "fontFamily": "Inter, sans-serif",
        },
    },
    "echartsOptionsOverridesByChartType": {
        "echarts_timeseries": {
            "xAxis": {
                "axisLine": {"lineStyle": {"color": "#CBD5E1"}},
                "axisLabel": {"color": "#64748B"},
            },
            "yAxis": {
                "splitLine": {"lineStyle": {"color": "#EEF2F7"}},
                "axisLabel": {"color": "#64748B"},
            },
        },
        "echarts_mixed_timeseries": {
            "xAxis": {"axisLabel": {"color": "#64748B"}},
            "yAxis": {
                "splitLine": {"lineStyle": {"color": "#EEF2F7"}},
                "axisLabel": {"color": "#64748B"},
            },
        },
        "echarts_bar": {
            "yAxis": {
                "splitLine": {"lineStyle": {"color": "#EEF2F7"}},
            },
        },
        "echarts_pie": {
            "legend": {
                "orient": "horizontal",
                "top": "bottom",
                "left": "center",
            },
        },
        "echarts_sankey": {
            "tooltip": {"trigger": "item"},
        },
    },
}

THEME_DARK = {
    "algorithm": "dark",
    "token": {
        "brandAppName": "PDM Executive Intelligence",
        "colorPrimary": "#48A9A6",
        "colorSuccess": "#6BCB77",
        "colorWarning": "#F2C94C",
        "colorError": "#FF6B6B",
        "colorBgLayout": "#0B1220",
        "colorBgContainer": "#111827",
        "borderRadius": 8,
        "fontFamily": "Inter, -apple-system, BlinkMacSystemFont, sans-serif",
    },
}


# =============================================================================
# QUERY / DASHBOARD LIMITS
# =============================================================================

ROW_LIMIT = 100000
SQL_MAX_ROW = 100000
SUPERSET_WEBSERVER_TIMEOUT = 120
SQLLAB_ASYNC_TIME_LIMIT_SEC = 300
CACHE_DEFAULT_TIMEOUT = 300


# =============================================================================
# APPLICATION BRANDING / UX
# =============================================================================

APP_NAME = "PDM Executive Intelligence"
DEFAULT_TIMEZONE = "Africa/Kampala"


# =============================================================================
# EXPORTS
# =============================================================================

CSV_EXTENSIONS = {"csv"}

# =============================================================================
# RATE LIMITING
#
# Redis DB 7 is reserved for Flask-Limiter / Superset rate-limit state.
# Keeping this state in Redis makes rate limiting consistent across the
# Superset web processes and Celery-related application instances.
# =============================================================================

RATELIMIT_STORAGE_URI = (
    f"redis://{REDIS_HOST}:{REDIS_PORT}/7"
)

RATELIMIT_STRATEGY = "fixed-window"

# ---------------------------------------------------------------------------
# Dashboard trusted HTML
# Local development configuration
# ---------------------------------------------------------------------------

HTML_SANITIZATION = False