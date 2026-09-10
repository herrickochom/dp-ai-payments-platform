#!/bin/sh
set -eu

registry_url="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"
compatibility="${SCHEMA_COMPATIBILITY:-BACKWARD_TRANSITIVE}"
if [ -n "${SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO:-}" ]; then
  auth_header="Authorization: Basic $(printf '%s' "${SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO}" | base64)"
else
  auth_header="X-No-Authentication: local-development"
fi

curl --fail --silent --show-error -H "$auth_header" \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -X PUT "${registry_url}/config" \
  --data "{\"compatibility\":\"${compatibility}\"}"
echo "Schema Registry compatibility set to ${compatibility}"
