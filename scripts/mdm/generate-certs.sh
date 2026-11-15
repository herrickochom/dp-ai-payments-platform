#!/usr/bin/env bash
# One-time: generate self-signed CA + server cert for cloud-sql-proxy TLS front-end.
# Server cert SAN must include DNS:cloud-sql-proxy (the service name clients use).
set -euo pipefail
cd "$(dirname "$0")/../.." || exit 1
OUT=certs
mkdir -p "$OUT"

if [ -f "$OUT/mdm-ca.crt" ] && [ "${FORCE:-0}" != "1" ]; then
  echo "certificates already exist at $OUT/ (set FORCE=1 to regenerate)"
  exit 0
fi

echo "generating CA"
openssl req -x509 -newkey rsa:2048 -days 3650 -nodes \
  -keyout "$OUT/mdm-ca.key" -out "$OUT/mdm-ca.crt" \
  -subj "/CN=MDM-Proxy-CA" >/dev/null 2>&1

echo "generating server key + CSR"
openssl req -newkey rsa:2048 -nodes \
  -keyout "$OUT/mdm-server.key" -out "$OUT/mdm-server.csr" \
  -subj "/CN=cloud-sql-proxy" >/dev/null 2>&1

echo "signing server cert with CA (SAN: cloud-sql-proxy, localhost)"
openssl x509 -req -in "$OUT/mdm-server.csr" \
  -CA "$OUT/mdm-ca.crt" -CAkey "$OUT/mdm-ca.key" -CAcreateserial \
  -out "$OUT/mdm-server.crt" -days 3650 \
  -extfile <(printf "subjectAltName=DNS:cloud-sql-proxy,DNS:localhost,IP:127.0.0.1") >/dev/null 2>&1

rm -f "$OUT/mdm-server.csr"
chmod 600 "$OUT"/*.key
chmod 644 "$OUT"/*.crt
echo "done:"
ls -la "$OUT"/
