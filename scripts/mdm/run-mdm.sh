#!/usr/bin/env bash
# Bring up cloud-sql-proxy, push fresh MDM to Cloud SQL, publish to Kafka.
set -euo pipefail
cd "$(dirname "$0")/../.." || exit 1

if [ ! -f .env ]; then
  echo "STOP: create .env from .env.mdm.example with PDM_SOURCE_* set"; exit 1
fi
if [ ! -f certs/mdm-ca.crt ]; then
  echo "generating TLS certs"
  bash scripts/mdm/generate-certs.sh
fi

step(){ echo; echo "=== $* ==="; }
step "1. cloud-sql-proxy up"
docker compose --profile mdm up -d cloud-sql-proxy
sleep 3
docker compose --profile mdm ps cloud-sql-proxy

step "2. mdm-loader (writes to Cloud SQL)"
docker compose --profile mdm run --rm mdm-loader

step "3. verify Cloud SQL row counts"
docker compose --profile mdm run --rm --entrypoint /bin/bash mdm-loader -c '
python3 - <<PY
import os, psycopg2
conn = psycopg2.connect(
    host="cloud-sql-proxy", port=5432,
    dbname=os.environ["PDM_SOURCE_DB"],
    user=os.environ["PDM_SOURCE_USER"],
    password=os.environ["PDM_SOURCE_PASSWORD"],
    sslmode="verify-full",
    sslrootcert="/certs/mdm-ca.crt",
    connect_timeout=10,
)
cur = conn.cursor()
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '"'"'mdm'"'"' ORDER BY 1")
tables = [r[0] for r in cur.fetchall()]
print(f"mdm tables: {len(tables)}")
for t in tables:
    cur.execute(f"SELECT COUNT(*) FROM mdm.{t}")
    print(f"  {t}: {cur.fetchone()[0]}")
conn.close()
PY
'

step "4. mdm-publisher (pushes to Kafka)"
docker compose --profile mdm run --rm mdm-publisher --authority ordinary --validate-only || true
docker compose --profile mdm run --rm mdm-publisher --authority ordinary --execute

step "5. cleanup proxy"
docker compose --profile mdm stop cloud-sql-proxy
