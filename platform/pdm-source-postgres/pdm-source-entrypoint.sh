#!/bin/sh
set -eu
: "${POSTGRES_DB:?PDM_SOURCE_DB is required}"
: "${POSTGRES_USER:?PDM_SOURCE_USER is required}"
: "${POSTGRES_PASSWORD:?PDM_SOURCE_PASSWORD is required}"
: "${PDM_CDC_USER:?PDM_CDC_USER is required}"
: "${PDM_CDC_PASSWORD:?PDM_CDC_PASSWORD is required}"
exec /usr/local/bin/docker-entrypoint.sh "$@"
