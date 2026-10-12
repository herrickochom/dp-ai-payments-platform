#!/bin/sh
set -eu

# Configure Schema Registry global compatibility and register the
# streaming-route derived-topic subjects.
#
# Global compatibility is set first, then each subject is registered with
# its schema. Registration uses POST to
# /subjects/<subject>/versions, which is idempotent when the schema is
# unchanged and returns 409 if a different schema is already registered
# under the same subject.

registry_url="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"
compatibility="${SCHEMA_COMPATIBILITY:-BACKWARD_TRANSITIVE}"
schema_dir="${SCHEMA_DIR:-/contracts/kafka}"

if [ -n "${SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO:-}" ]; then
  auth_header="Authorization: Basic $(printf '%s' "${SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO}" | base64)"
else
  auth_header="X-No-Authentication: local-development"
fi

# -----------------------------------------------------------------------------
# Step 1 - set the global compatibility mode
# -----------------------------------------------------------------------------
curl --fail --silent --show-error -H "$auth_header" \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -X PUT "${registry_url}/config" \
  --data "{\"compatibility\":\"${compatibility}\"}"
echo "Schema Registry compatibility set to ${compatibility}"

# -----------------------------------------------------------------------------
# Step 2 - register the streaming-route derived subjects
# -----------------------------------------------------------------------------
# Each mapping is <topic-name>:<schema-file>. The subject name is derived
# from the topic using TopicNameStrategy (<topic>-value).

register_subject() {
  subject="$1"
  schema_file="$2"

  if [ ! -f "$schema_file" ]; then
    echo "warning: schema file not found: $schema_file" >&2
    return 0
  fi

  # Embed the schema JSON as a string inside the request body. The
  # escaping uses Python-free shell: read the file, escape backslashes
  # and double quotes, and wrap in a JSON object.
  schema_json="$(cat "$schema_file")"
  # Escape backslashes, double quotes, and newlines for safe JSON embedding.
  schema_json="$(printf '%s' "$schema_json" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' | tr -d '\n')"

  body="{\"schema\":\"${schema_json}\",\"schemaType\":\"AVRO\"}"

  http_code="$(curl --silent --show-error -o /dev/null -w '%{http_code}' \
    -H "$auth_header" \
    -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    -X POST "${registry_url}/subjects/${subject}/versions" \
    --data "$body")"

  case "$http_code" in
    200) echo "registered subject ${subject}" ;;
    409) echo "subject ${subject} already registered with a different schema (409)" ;;
    *)   echo "error: failed to register ${subject} (HTTP ${http_code})" >&2
         return 1 ;;
  esac
}

register_subject "payments.lifecycle.transitions.v1-value" "${schema_dir}/lifecycle_transition.avsc"
register_subject "payments.lifecycle.state.v1-value"       "${schema_dir}/lifecycle_state.avsc"
register_subject "payments.lifecycle.exceptions.v1-value"  "${schema_dir}/lifecycle_exception.avsc"
register_subject "payments.lifecycle.dlq.v1-value"         "${schema_dir}/lifecycle_dlq.avsc"
register_subject "payments.lifecycle.audit.v1-value"       "${schema_dir}/lifecycle_audit.avsc"

echo "Schema Registry initialization completed"