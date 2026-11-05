"""Bounded opt-in Kafka -> Pub/Sub Raw v2 bridge. Never runs on import."""
import argparse
import importlib.util
import os
from pathlib import Path

def load_bridge():
    spec=importlib.util.spec_from_file_location('kp001_bridge',Path(__file__).resolve().with_name('bridge.py'))
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def run(args):
    from confluent_kafka import Consumer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from google.cloud import pubsub_v1
    bridge=load_bridge()
    registry=SchemaRegistryClient({'url':args.schema_registry_url})
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
            # Verify registry recognises source schema ID before publishing.
            if not isinstance(wire,bytes) or len(wire)<=5 or wire[0]!=0:
                raise RuntimeError('non-Confluent source message; offset uncommitted')
            registry.get_schema(int.from_bytes(wire[1:5],'big'))
            message_id=bridge.publish_wire_then_commit(consumer,publisher,topic_path,msg)
            print(f'PASS: published Kafka {msg.topic()}:{msg.partition()}:{msg.offset()} -> Pub/Sub {message_id}')
            processed+=1
    finally:
        consumer.close()
    return processed

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--live',action='store_true')
    p.add_argument('--kafka-bootstrap',default=os.getenv('KP_KAFKA_BOOTSTRAP',''))
    p.add_argument('--schema-registry-url',default=os.getenv('KP_SCHEMA_REGISTRY_URL',''))
    p.add_argument('--source-topic',default=os.getenv('KP_SOURCE_TOPIC',''))
    p.add_argument('--project',default=os.getenv('KP_GCP_PROJECT',''))
    p.add_argument('--topic',default='pdm-native-events-poc-v1')
    p.add_argument('--group',default='gcp-kp001-raw-v2-bounded')
    p.add_argument('--max-messages',type=int,default=1)
    args=p.parse_args()
    if not args.live:
        print('SAFE: Raw v2 bridge installed; no Kafka/cloud access. --live required.')
        return
    if args.topic!='pdm-native-events-poc-v1': raise SystemExit('STOP: unapproved Pub/Sub topic')
    if not all((args.kafka_bootstrap,args.schema_registry_url,args.source_topic,args.project)) or not 1<=args.max_messages<=10:
        raise SystemExit('STOP: missing explicit source configuration or invalid bound')
    run(args)

if __name__=='__main__': main()
