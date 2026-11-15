FROM python:3.11-slim
WORKDIR /app
COPY services/shared/requirements-runtime.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
# Whole services tree so `services.shared.security.*` and `services.shared.mdm.*` are importable.
COPY services/ /app/services/
COPY platform/mdm/ /app/platform/mdm/
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENTRYPOINT ["python3", "-m", "services.mdm-publisher.mdm_publisher"]
CMD ["--authority", "ordinary", "--execute"]
