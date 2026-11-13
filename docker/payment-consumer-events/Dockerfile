FROM python:3.11-slim

WORKDIR /app
COPY services/kafka-consumer-events/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY services/kafka-consumer-events/kafka_consumer_events.py /app/consumer.py
COPY services/kafka-consumer-events/raw_data_healthcheck.py /app/raw_data_healthcheck.py
COPY services/kafka-consumer-events/replay_dlq.py /app/replay_dlq.py
COPY services/shared/security/__init__.py /app/services/shared/security/__init__.py
COPY services/shared/security/runtime_security.py /app/services/shared/security/runtime_security.py
COPY services/shared/security/secret_provider.py /app/services/shared/security/secret_provider.py
COPY services/shared/cdc /app/services/shared/cdc
COPY platform/cdc/contracts/cdc_raw_event.avsc /app/contracts/cdc_raw_event.avsc

# Explicit image path for the authoritative schema: the module is flattened
# to /app/consumer.py here and cannot derive the repository-relative default.
ENV CDC_RAW_SCHEMA_PATH=/app/contracts/cdc_raw_event.avsc

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python3", "/app/consumer.py"]
