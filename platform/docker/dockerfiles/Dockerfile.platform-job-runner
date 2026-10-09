FROM trinodb/trino:477 AS transform-trino

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV JAVA_HOME=/usr/lib/jvm/temurin/jdk-24.0.2+12
ENV PATH=/usr/lib/jvm/temurin/jdk-24.0.2+12/bin:$PATH

WORKDIR /app/job_runner

COPY orchestration/job_runner/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir \
    -r /tmp/requirements.txt \
    && install -d -m 0755 /opt/duckdb/extensions \
    && python -c "import duckdb; c = duckdb.connect(':memory:', config={'extension_directory': '/opt/duckdb/extensions'}); c.install_extension('httpfs'); c.install_extension('avro'); c.close()" \
    && install -d -m 0755 /opt/dbt/bin \
    && ln -s /usr/local/bin/dbt /opt/dbt/bin/dbt

COPY orchestration/job_runner /app/job_runner
COPY orchestration/transform_runtime /app/orchestration/transform_runtime
COPY services/shared/data_protection.py /app/job_runner/data_protection.py
COPY services/shared/tokeniser_job.py /app/job_runner/tokeniser_job.py
COPY services/shared/materialise_token_link.py /app/job_runner/materialise_token_link.py
COPY services/shared/security/__init__.py /app/services/shared/security/__init__.py
COPY services/shared/security/secret_provider.py /app/services/shared/security/secret_provider.py
COPY services/shared/security/runtime_security.py /app/services/shared/security/runtime_security.py
COPY transform/dbt /app/dbt
COPY --from=transform-trino /usr/lib/trino /usr/lib/trino
COPY --from=transform-trino /usr/lib/jvm/temurin/jdk-24.0.2+12 /usr/lib/jvm/temurin/jdk-24.0.2+12
COPY --chown=50001:50001 platform/trino/etc/password.db /opt/transform-trino-security/password.db
COPY platform/trino/etc/rules.json /opt/transform-trino-security/rules.json
COPY --chown=50001:50001 platform/trino/etc/trino-keystore.jks /opt/transform-trino-security/trino-keystore.jks

RUN useradd \
      --uid 50001 \
      --create-home \
      --shell /usr/sbin/nologin \
      platformrunner \
    && install -d -o platformrunner -g platformrunner /var/lib/platform-job-runner/work \
    && chown -R platformrunner:platformrunner /app/job_runner /app/orchestration /app/dbt

USER 50001:50001

EXPOSE 8090

CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8090"]
