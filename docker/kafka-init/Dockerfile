FROM confluentinc/cp-kafka:7.5.0

USER root

RUN yum install -y jq curl python3 python3-pip \
    && python3 -m pip install --no-cache-dir pyyaml==6.0.1 \
    && yum clean all

COPY platform/kafka/create-topics.sh /app/create-topics.sh
COPY platform/kafka/topics.yaml /app/topics.yaml
COPY platform/kafka/topic_admin.py /app/topic_admin.py

RUN chmod +x /app/create-topics.sh

ENTRYPOINT ["python3", "/app/topic_admin.py"]
