"""Bounded opt-in Pub/Sub -> governed GCS Raw v2 worker. Never runs on import."""
import argparse
import importlib.util
import os
from pathlib import Path

def load_cloud():
    p=Path(__file__).resolve().with_name('raw_v2_cloud.py')
    spec=importlib.util.spec_from_file_location('raw_v2_cloud',p)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def run(args):
    from google.cloud import pubsub_v1,storage
    from confluent_kafka.schema_registry import SchemaRegistryClient
    cloud=load_cloud()
    subscriber=pubsub_v1.SubscriberClient()
    subscription=subscriber.subscription_path(args.project,args.subscription)
    client=storage.Client(project=args.project)
    bucket=client.lookup_bucket(args.bucket)
    if bucket is None: raise RuntimeError('approved GCS bucket not accessible; no acknowledgements')
    if bucket.name != args.bucket: raise RuntimeError('bucket mismatch')
    store=cloud.GCSConditionalStore(bucket)
    acker=cloud.PubSubAcknowledger(subscriber,subscription)
    decoder=cloud.RegistryDecoder(SchemaRegistryClient({'url':args.schema_registry_url}))
    processed=0
    try:
        while processed<args.max_messages:
            response=subscriber.pull(request={'subscription':subscription,'max_messages':1},timeout=20)
            if not response.received_messages: break
            received=response.received_messages[0]
            # Any error stops processing without acknowledging this message.
            name,state=cloud.process_message(received.message.data,received.ack_id,store,acker,decoder)
            print(f'PASS: {state}: {name}')
            processed+=1
    finally:
        subscriber.close()
    return processed

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--live',action='store_true')
    p.add_argument('--project',default=os.getenv('KP_GCP_PROJECT',''))
    p.add_argument('--subscription',default=os.getenv('KP_RAW_V2_SUBSCRIPTION',''))
    p.add_argument('--bucket',default='dp-pdm-ai-platform-dev')
    p.add_argument('--schema-registry-url',default=os.getenv('KP_SCHEMA_REGISTRY_URL',''))
    p.add_argument('--max-messages',type=int,default=1)
    args=p.parse_args()
    if not args.live:
        print('SAFE: Raw v2 worker installed; no cloud access. --live required.')
        return
    if args.bucket!='dp-pdm-ai-platform-dev': raise SystemExit('STOP: unapproved destination bucket')
    if not all((args.project,args.subscription,args.schema_registry_url)) or not 1<=args.max_messages<=10:
        raise SystemExit('STOP: explicit project, subscription, registry URL and 1..10 messages required')
    run(args)

if __name__=='__main__': main()
