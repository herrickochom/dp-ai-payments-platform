#!/usr/bin/env bash
# Compatibility wrapper. topics.yaml is the only topic source of truth.
set -Eeuo pipefail
exec python3 "${TOPIC_ADMIN_SCRIPT:-/app/topic_admin.py}"
