FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade \
    pip \
    setuptools \
    wheel

RUN pip install --no-cache-dir \
    dbt-core==1.9.4 \
    dbt-trino==1.9.3

WORKDIR /usr/app/dbt

ENTRYPOINT ["dbt"]
CMD ["--version"]
