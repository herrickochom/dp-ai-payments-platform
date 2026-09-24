"""C10.3E Block 1: Kafka TLS/SASL focused tests.

Verify the Kafka production transport contract as a base/overlay split: the
local base compose keeps Kafka on a certificate-free PLAINTEXT topology so
`docker compose up` works without production material, while the production
SASL/SSL transport (listener, keystore/truststore, SASL port) lives in
docker-compose.production-tls.yaml and is asserted there. Neither side may
drift silently.
"""

from __future__ import annotations

import re

import pytest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = (ROOT / "docker-compose.yaml").read_text()
PRODUCTION_TLS_COMPOSE = (
    ROOT / "docker-compose.production-tls.yaml"
).read_text()


def service(name, compose_text=COMPOSE):
    match = re.search(
        rf"(?ms)^  {re.escape(name)}:\n(.*?)(?=^  [\w-]+:|\Z)",
        compose_text,
    )
    assert match is not None, f"Service {name} not found in compose"
    return match.group(1)


def test_kafka_broker_has_sasl_ssl_listener():
    # Base compose: local topology stays PLAINTEXT-only — no production SASL
    # listener may be required to start Kafka without certificates.
    block = service("kafka")
    assert "EXTERNAL_SASL" not in block
    # Production overlay: the SASL listener terminates SSL on 9095.
    prod_block = service("kafka", PRODUCTION_TLS_COMPOSE)
    assert "EXTERNAL_SASL://0.0.0.0:9095" in prod_block
    assert "EXTERNAL_SASL:SSL" in prod_block


def test_kafka_broker_ssl_config_seams():
    # Base compose: local startup must not require broker keystores.
    block = service("kafka")
    assert "KAFKA_SSL_KEYSTORE_LOCATION" not in block
    assert "KAFKA_SSL_KEYSTORE_PASSWORD" not in block
    # Production overlay: full keystore/truststore seam plus mounted certs.
    prod_block = service("kafka", PRODUCTION_TLS_COMPOSE)
    assert "KAFKA_SSL_KEYSTORE_LOCATION" in prod_block
    assert "KAFKA_SSL_KEYSTORE_PASSWORD" in prod_block
    assert "KAFKA_SSL_TRUSTSTORE_LOCATION" in prod_block
    assert "KAFKA_SSL_TRUSTSTORE_PASSWORD" in prod_block
    assert "/etc/kafka/certs" in prod_block


def test_kafka_init_has_sasl_tls_config():
    block = service("kafka-init")
    assert "KAFKA_SECURITY_PROTOCOL" in block
    assert "KAFKA_SASL_MECHANISM" in block
    assert "KAFKA_SASL_USERNAME" in block
    assert "KAFKA_SASL_PASSWORD" in block
    assert "KAFKA_SSL_CA_LOCATION" in block


def test_kafka_local_plaintext_preserved():
    block = service("kafka")
    assert "INTERNAL:PLAINTEXT" in block
    assert "EXTERNAL:PLAINTEXT" in block


def test_kafka_sasl_port_exposed():
    # Base compose exposes only the plaintext external port; the SASL port is
    # a production-overlay concern.
    block = service("kafka")
    assert "${KAFKA_EXTERNAL_PORT:-9094}:9094" in block
    assert "${KAFKA_SASL_PORT:-9095}:9095" not in block
    prod_block = service("kafka", PRODUCTION_TLS_COMPOSE)
    assert "${KAFKA_SASL_PORT:-9095}:9095" in prod_block


def test_producer_has_kafka_security_config():
    block = service("payment-producer")
    assert "KAFKA_SECURITY_PROTOCOL" in block
    assert "KAFKA_SASL_MECHANISM" in block
    assert "KAFKA_SASL_USERNAME" in block
    assert "KAFKA_SASL_PASSWORD" in block


def test_consumer_has_kafka_security_config():
    block = service("payment-consumer-events")
    assert "KAFKA_SECURITY_PROTOCOL" in block
    assert "KAFKA_SASL_MECHANISM" in block
    assert "KAFKA_SASL_USERNAME" in block
    assert "KAFKA_SASL_PASSWORD" in block
