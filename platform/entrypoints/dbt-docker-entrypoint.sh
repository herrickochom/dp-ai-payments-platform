#!/bin/sh
set -e

# Run the Python readiness check script. It will loop until Trino is ready.
python3 /usr/app/dbt/wait_for_trino.py

# Once the script exits successfully, execute the main command passed to the container
# (e.g., "dbt debug", "dbt run", etc.)
exec "$@"
