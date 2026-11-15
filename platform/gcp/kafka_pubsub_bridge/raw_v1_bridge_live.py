"""Bounded opt-in Kafka -> Pub/Sub Raw v1 bridge. Never runs on import."""
import argparse
import importlib.util
import os
from pathlib import Path

def load_bridge():
    """Use the canonical seven-field Raw v1 transport."""
    import base64
    import hashlib
    import json
    from types import SimpleNamespace

    def publish_wire_then_commit(consumer, publisher, topic_path, msg):
        from confluent_kafka import TopicPartition

        wire = msg.value()
        stamp = msg.timestamp()
        if (
            type(wire) is not bytes
            or len(wire) <= 5
            or wire[0] != 0
            or not isinstance(stamp, tuple)
            or len(stamp) != 2
            or type(stamp[1]) is not int
            or stamp[1] < 0
        ):
            raise RuntimeError("invalid Kafka source wire/provenance")

        schema_id = int.from_bytes(wire[1:5], "big")
        if schema_id < 1:
            raise RuntimeError("invalid Schema Registry ID")

        if (
            type(msg.partition()) is not int
            or msg.partition() < 0
            or type(msg.offset()) is not int
            or msg.offset() < 0
        ):
            raise RuntimeError("invalid Kafka coordinates")

        body = json.dumps({
            "kafka_topic": msg.topic(),
            "kafka_partition": msg.partition(),
            "kafka_offset": msg.offset(),
            "kafka_timestamp_ms": stamp[1],
            "schema_registry_id": schema_id,
            "payload_base64": base64.b64encode(wire).decode("ascii"),
            "payload_sha256": hashlib.sha256(wire).hexdigest(),
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")

        message_id = publisher.publish(
            topic_path, body
        ).result(timeout=30)

        if not isinstance(message_id, str) or not message_id:
            raise RuntimeError("Pub/Sub publish not confirmed")

        next_offset = msg.offset() + 1
        result = consumer.commit(
            offsets=[TopicPartition(
                msg.topic(), msg.partition(), next_offset
            )],
            asynchronous=False,
        )

        if (
            not result
            or len(result) != 1
            or result[0].error is not None
            or result[0].topic != msg.topic()
            or result[0].partition != msg.partition()
            or result[0].offset != next_offset
        ):
            raise RuntimeError("Kafka commit not confirmed")

        return message_id

    return SimpleNamespace(
        publish_wire_then_commit=publish_wire_then_commit
    )


def run(args):
    from confluent_kafka import Consumer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from google.cloud import pubsub_v1
    import ast
    contract = (
        Path(__file__).resolve().parents[1]
        / "pubsub_raw_ingestion" / "raw_v1.py"
    )
    tree = ast.parse(contract.read_text())
    topics = None
    for node in tree.body:
        if isinstance(node, ast.Assign):
            if any(
                isinstance(t, ast.Name) and t.id == "TOPIC_MAP"
                for t in node.targets
            ):
                topics = ast.literal_eval(node.value)
    if not isinstance(topics, dict) or len(topics) != 24:
        raise RuntimeError("invalid legacy topic contract")
    approved_legacy_topics = lambda: frozenset(topics)
    if args.source_topic not in approved_legacy_topics():
        raise RuntimeError("Kafka topic not approved for legacy ingestion")
    bridge=load_bridge()
    registry=SchemaRegistryClient({'url':args.schema_registry_url})

    # Reuse the existing Raw v1 Schema Registry-backed decoder.
    import importlib.util
    decoder_path = (
        Path(__file__).resolve().parents[1]
        / "pubsub_raw_ingestion" / "raw_v1_cloud.py"
    )
    decoder_spec = importlib.util.spec_from_file_location(
        "raw_v1_publisher_cloud_adapter", decoder_path
    )
    cloud = importlib.util.module_from_spec(decoder_spec)
    decoder_spec.loader.exec_module(cloud)
    decoder = cloud.RegistryDecoder(registry)

    consumer=Consumer({'bootstrap.servers':args.kafka_bootstrap,'group.id':args.group,
                       'enable.auto.commit':False,'auto.offset.reset':'earliest'})
    publisher=pubsub_v1.PublisherClient()
    topic_path=publisher.topic_path(args.project,args.topic)
    consumer.subscribe([args.source_topic])
    processed=0
    try:
        while processed<args.max_messages:
            msg=consumer.poll(5)
            if msg is None: break
            if msg.error(): raise RuntimeError('Kafka poll failed; offset uncommitted')
            wire=msg.value()
            # Validate the actual Avro datum before any Pub/Sub publication.
            if not isinstance(wire,bytes) or len(wire)<=5 or wire[0]!=0:
                raise RuntimeError('non-Confluent source message; offset uncommitted')

            schema_id = int.from_bytes(wire[1:5], 'big')
            if schema_id < 1:
                raise RuntimeError('invalid source schema ID; offset uncommitted')

            registry.get_schema(schema_id)
            event = decoder(wire, msg.topic())

            if (
                type(event) is not dict
                or type(event.get('event_id')) is not str
                or not event['event_id']
            ):
                raise RuntimeError(
                    'invalid decoded source event; offset uncommitted'
                )

            message_id=bridge.publish_wire_then_commit(consumer,publisher,topic_path,msg)
            print(f'PASS: published Kafka {msg.topic()}:{msg.partition()}:{msg.offset()} -> Pub/Sub {message_id}')
            processed+=1
    finally:
        consumer.close()
    return processed

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--live',action='store_true')
    p.add_argument('--kafka-bootstrap',default=os.getenv('PDM_KAFKA_BOOTSTRAP',''))
    p.add_argument('--schema-registry-url',default=os.getenv('PDM_SCHEMA_REGISTRY_URL',''))
    p.add_argument('--source-topic',default=os.getenv('PDM_SOURCE_TOPIC',''))
    p.add_argument('--project',default=os.getenv('PDM_GCP_PROJECT',''))
    p.add_argument('--topic',default='pdm-dev-payments-source-events-v1')
    p.add_argument('--group',default='gcp-raw-v1-bounded-bridge')
    p.add_argument('--max-messages',type=int,default=1)
    p.add_argument('--recovery-plan', default='')
    args=p.parse_args()
    if args.recovery_plan:
        run_recovery(args)
        return
    if not args.live:
        print('SAFE: Raw v1 bridge installed; no Kafka/cloud access. --live required.')
        return
    if args.topic!='pdm-dev-payments-source-events-v1': raise SystemExit('STOP: unapproved Pub/Sub topic')
    if not all((args.kafka_bootstrap,args.schema_registry_url,args.source_topic,args.project)) or not 1<=args.max_messages<=10:
        raise SystemExit('STOP: missing explicit source configuration or invalid bound')
    run(args)



def validate_recovery_plan(plan, approved_topics):
    """Validate an explicit, bounded legacy recovery request."""
    if not isinstance(plan, dict):
        raise ValueError("recovery plan must be an object")

    expected = {
        "topic", "partition", "start_offset",
        "end_offset", "consumer_group"
    }

    if set(plan) != expected:
        raise ValueError("unexpected recovery plan fields")

    topic = plan["topic"]
    partition = plan["partition"]
    start = plan["start_offset"]
    end = plan["end_offset"]
    group = plan["consumer_group"]

    if not isinstance(topic, str) or topic not in approved_topics:
        raise ValueError("unapproved legacy topic")

    if any(
        type(value) is not int or value < 0
        for value in (partition, start, end)
    ):
        raise ValueError("invalid Kafka coordinates")

    if not start < end <= start + 10:
        raise ValueError("recovery range must contain 1..10 offsets")

    if (
        not isinstance(group, str)
        or not group.startswith("gcp-raw-v1-recovery-")
        or len(group) > 128
        or not all(c.isalnum() or c in "._-" for c in group)
    ):
        raise ValueError("invalid dedicated recovery group")

    return topic, partition, start, end, group


def run_recovery(args):
    """Bounded explicit legacy Kafka recovery into existing Pub/Sub transport."""
    import ast
    import json

    if not args.live:
        raise RuntimeError("recovery requires --live")

    if os.environ.get("DP_LEGACY_RECOVERY_APPROVAL") != "APPROVED-BOUNDED-RECOVERY":
        raise RuntimeError("recovery approval missing")

    if args.topic != "pdm-dev-payments-source-events-v1":
        raise RuntimeError("unapproved Pub/Sub destination")

    if not all((
        args.kafka_bootstrap,
        args.schema_registry_url,
        args.project,
        args.recovery_plan,
    )):
        raise RuntimeError("explicit recovery configuration required")

    contract_path = (
        Path(__file__).resolve().parents[1]
        / "pubsub_raw_ingestion/raw_v1.py"
    )

    contract_tree = ast.parse(contract_path.read_text())
    topic_map = None

    for node in contract_tree.body:
        if isinstance(node, ast.Assign):
            if any(
                isinstance(target, ast.Name)
                and target.id == "TOPIC_MAP"
                for target in node.targets
            ):
                topic_map = ast.literal_eval(node.value)

    if not isinstance(topic_map, dict) or len(topic_map) != 24:
        raise RuntimeError("legacy contract mismatch")

    plan_path = Path(args.recovery_plan)

    if not plan_path.is_file():
        raise RuntimeError("recovery plan missing")

    plan = json.loads(plan_path.read_text(encoding="utf-8"))

    topic, partition, start, end, group = validate_recovery_plan(
        plan, frozenset(topic_map)
    )

    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from google.cloud import pubsub_v1
    import importlib.util

    consumer = Consumer({
        "bootstrap.servers": args.kafka_bootstrap,
        "group.id": group,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "error",
    })

    try:
        metadata = consumer.list_topics(topic=topic, timeout=15)
        topic_metadata = metadata.topics.get(topic)

        if topic_metadata is None or topic_metadata.error:
            raise RuntimeError("Kafka topic unavailable")

        if partition not in topic_metadata.partitions:
            raise RuntimeError("Kafka partition unavailable")

        tp = TopicPartition(topic, partition)

        low, high = consumer.get_watermark_offsets(
            tp, timeout=15, cached=False
        )

        if not low <= start < end <= high:
            raise RuntimeError("offset range outside Kafka retention")

        committed = consumer.committed([tp], timeout=15)

        if not isinstance(committed, list) or len(committed) != 1:
            raise RuntimeError("Kafka checkpoint lookup failed")

        checkpoint = committed[0]

        if checkpoint.error is not None:
            raise RuntimeError("Kafka checkpoint lookup error")

        # -1001 is Kafka's OFFSET_INVALID sentinel.
        if checkpoint.offset not in (-1001, start):
            raise RuntimeError(
                "existing recovery checkpoint does not match requested start"
            )

        registry = SchemaRegistryClient({
            "url": args.schema_registry_url
        })

        decoder_path = (
            Path(__file__).resolve().parents[1]
            / "pubsub_raw_ingestion/raw_v1_cloud.py"
        )

        spec = importlib.util.spec_from_file_location(
            "raw_v1_recovery_cloud", decoder_path
        )

        cloud = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cloud)

        decoder = cloud.RegistryDecoder(registry)

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(args.project, args.topic)

        publisher.get_topic(request={"topic": topic_path})

        consumer.assign([
            TopicPartition(topic, partition, start)
        ])

        bridge = load_bridge()
        processed = 0

        for expected_offset in range(start, end):
            msg = consumer.poll(10)

            if msg is None or msg.error():
                raise RuntimeError("Kafka recovery poll failed")

            if (
                msg.topic() != topic
                or msg.partition() != partition
                or msg.offset() != expected_offset
            ):
                raise RuntimeError("Kafka recovery coordinate mismatch")

            wire = msg.value()

            if (
                type(wire) is not bytes
                or len(wire) <= 5
                or wire[0] != 0
            ):
                raise RuntimeError("invalid Confluent Avro wire")

            schema_id = int.from_bytes(wire[1:5], "big")

            if schema_id < 1:
                raise RuntimeError("invalid Schema Registry ID")

            registry.get_schema(schema_id)

            event = decoder(wire, topic)

            if (
                type(event) is not dict
                or type(event.get("event_id")) is not str
                or not event["event_id"]
            ):
                raise RuntimeError("invalid decoded source event")

            message_id = bridge.publish_wire_then_commit(
                consumer, publisher, topic_path, msg
            )

            print(
                f"RECOVERY_PUBLISHED topic={topic} "
                f"partition={partition} offset={expected_offset} "
                f"pubsub_message_id={message_id}",
                flush=True,
            )

            processed += 1

        print(f"PUBSUB_CONFIRMED={processed}")
        print("GCS_VERIFIED=NO")
        return processed

    finally:
        consumer.close()

if __name__=='__main__': main()
