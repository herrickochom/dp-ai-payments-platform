# =============================================================================
# Apache Superset Configuration for the Loyalty Marktech Data Platform
# =============================================================================
import os

# -----------------------------------------------------------------------------
# Main Application Configuration
# -----------------------------------------------------------------------------
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY")

# FIXED: Use the actual PostgreSQL credentials from your .env file
SQLALCHEMY_DATABASE_URI = f"postgresql://{os.environ.get('POSTGRES_USER')}:{os.environ.get('POSTGRES_PASSWORD')}@{os.environ.get('POSTGRES_HOST')}:{os.environ.get('POSTGRES_PORT')}/{os.environ.get('POSTGRES_DB')}"

# -----------------------------------------------------------------------------
# Caching and Asynchronous Task Configuration (Celery via Redis)
# -----------------------------------------------------------------------------
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_URL': 'redis://redis-cache:6379/0',
}

class CeleryConfig:
    broker_url = 'redis://redis-cache:6379/1'
    imports = ('superset.sql_lab',)
    result_backend = 'redis://redis-cache:6379/2'
    worker_prefetch_multiplier = 10
    task_track_started = True

CELERY_CONFIG = CeleryConfig
RESULTS_BACKEND = None

# -----------------------------------------------------------------------------
# Feature Flags
# -----------------------------------------------------------------------------
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_RBAC": True,
    "DRILL_TO_DETAIL": True,
    "HORIZONTAL_FILTER_BAR": True,
    "DASHBOARD_CROSS_FILTERS": True,
}

# -----------------------------------------------------------------------------
# Other Optional Configurations
# -----------------------------------------------------------------------------
SQLALCHEMY_TRACK_MODIFICATIONS = False
CSV_EXTENSIONS = {'csv'}