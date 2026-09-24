FROM python:3.11-slim

WORKDIR /app
COPY services/payment-producer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY services/payment-producer/kafka_producer.py /app/producer.py
COPY services/shared/security/__init__.py /app/services/shared/security/__init__.py
COPY services/shared/security/runtime_security.py /app/services/shared/security/runtime_security.py
COPY services/shared/security/secret_provider.py /app/services/shared/security/secret_provider.py
COPY platform/kafka/payment_event.avsc /app/schemas/payment_event.avsc

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python3", "/app/producer.py"]
