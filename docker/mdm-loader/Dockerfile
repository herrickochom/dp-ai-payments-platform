FROM python:3.11-slim
WORKDIR /app
COPY services/shared/requirements-runtime.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
# Both the generator entrypoint and the postgres_store library are needed.
COPY services/payment-xml-generator/ /app/generator/
COPY services/shared/ /app/services/shared/
COPY services/shared/__init__.py /app/services/__init__.py 2>/dev/null || touch /app/services/__init__.py
ENV PYTHONUNBUFFERED=1
ENV DATA_ROOT=/app/data
ENV PYTHONPATH=/app:/app/generator
WORKDIR /app/generator
ENTRYPOINT ["python3", "mdm_generator.py"]
CMD ["--target", "postgres"]
