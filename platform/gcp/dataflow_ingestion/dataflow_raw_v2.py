"""KP-010: actual Pub/Sub -> Apache Beam/Dataflow -> GCS Raw v2.
Requires pre-existing Pub/Sub subscription, bucket, IAM and approved runtime.
No resources are provisioned here. Never acknowledges failed GCS writes.
"""
import argparse
import json
import os
from pathlib import Path


def build_pipeline(argv):
    import apache_beam as beam
    from apache_beam.options.pipeline_options import PipelineOptions
    from apache_beam.io.gcp.pubsub import ReadFromPubSub

    class WriteRawV2(beam.DoFn):
        def __init__(self,bucket,registry_url):
            self.bucket_name=bucket
            self.registry_url=registry_url
        def setup(self):
            from google.cloud import storage
            from confluent_kafka.schema_registry import SchemaRegistryClient
            from confluent_kafka.schema_registry.avro import AvroDeserializer
            from confluent_kafka.serialization import SerializationContext,MessageField
            self.storage=storage.Client()
            self.bucket=self.storage.bucket(self.bucket_name)
            self.deserializer=AvroDeserializer(SchemaRegistryClient({'url':self.registry_url}))
            self.SerializationContext=SerializationContext
            self.MessageField=MessageField
        def process(self,data):
            from google.api_core.exceptions import PreconditionFailed
            from kp010 import raw_v2
            obj,wire=raw_v2.transport(data)
            event=self.deserializer(wire,self.SerializationContext(obj['kafka_topic'],self.MessageField.VALUE))
            record=raw_v2.record_for(event,obj)
            name=raw_v2.name_for(obj)
            candidate=raw_v2.ocf_encode(record)
            blob=self.bucket.blob(name)
            try:
                blob.upload_from_string(candidate,content_type='application/avro',if_generation_match=0)
                status='CREATED'
            except PreconditionFailed:
                status='ALREADY_PRESENT'
            actual=raw_v2.ocf_read(blob.download_as_bytes())
            if actual!=record:
                raise raw_v2.CollisionError('immutable Kafka coordinate collision: '+name)
            yield json.dumps({'object':name,'status':status,'topic':obj['kafka_topic'],
                              'partition':obj['kafka_partition'],'offset':obj['kafka_offset']},sort_keys=True)

    p=argparse.ArgumentParser()
    p.add_argument('--subscription',required=True)
    p.add_argument('--bucket',required=True)
    p.add_argument('--schema-registry-url',required=True)
    p.add_argument('--project',required=True)
    p.add_argument('--region',required=True)
    p.add_argument('--runner',default='DirectRunner')
    p.add_argument('--job-name',default='dp-kp010-raw-v2')
    p.add_argument('--staging-location')
    p.add_argument('--temp-location')
    p.add_argument('--service-account-email')
    p.add_argument('--max-num-workers',type=int,default=1)
    p.add_argument('--approve-chargeable-dataflow',action='store_true')
    args,beam_args=p.parse_known_args(argv)
    if args.bucket!='dp-pdm-ai-platform-dev': p.error('unapproved bucket')
    if not args.subscription.startswith(f'projects/{args.project}/subscriptions/'):
        p.error('subscription project mismatch')
    if args.runner!='DataflowRunner': p.error('DirectRunner prohibited for unbounded live subscription')
    if not args.approve_chargeable_dataflow: p.error('explicit Dataflow cost approval required')
    if not all((args.staging_location,args.temp_location,args.service_account_email)):
        p.error('existing staging/temp locations and approved service account required')
    options=PipelineOptions(beam_args,runner=args.runner,project=args.project,region=args.region,
                            job_name=args.job_name,staging_location=args.staging_location,
                            temp_location=args.temp_location,service_account_email=args.service_account_email,
                            max_num_workers=args.max_num_workers,streaming=True,save_main_session=True,
                            setup_file=str(Path(__file__).with_name('setup.py')))
    with beam.Pipeline(options=options) as pipeline:
        (pipeline | 'ReadExistingSubscription' >> ReadFromPubSub(subscription=args.subscription)
                  | 'WriteImmutableRawV2' >> beam.ParDo(WriteRawV2(args.bucket,args.schema_registry_url))
                  | 'DeliveryEvidenceLog' >> beam.Map(print))

if __name__=='__main__':
    import sys
    build_pipeline(sys.argv[1:])
