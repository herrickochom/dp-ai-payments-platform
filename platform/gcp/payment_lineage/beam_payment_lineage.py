"""Bounded synthetic-payment Beam producer. Does not create cloud resources.

Writes payment records and execution-bound lineage to two BigQuery tables.
The writes are NOT atomic across tables. Independent expected-set verification
and a successful Dataflow terminal state are mandatory for acceptance.
"""
from __future__ import annotations
import argparse,hashlib,json,re

def digest_payment(record):
    return hashlib.sha256(json.dumps(record,sort_keys=True,separators=(',',':')).encode()).hexdigest()

def validate_identity(execution_id,spec_digest,attempt,job_id):
    if not re.fullmatch('[0-9a-f]{64}',execution_id) or not re.fullmatch('[0-9a-f]{64}',spec_digest):
        raise ValueError('invalid execution or spec digest')
    if type(attempt) is not int or attempt<1 or not re.fullmatch('[a-zA-Z0-9_-]{1,128}',job_id):
        raise ValueError('invalid attempt or provider job ID')

def make_rows(record,*,execution_id,spec_digest,attempt,job_id):
    validate_identity(execution_id,spec_digest,attempt,job_id)
    if not isinstance(record,dict) or not re.fullmatch('[a-zA-Z0-9_-]{1,128}',str(record.get('event_id',''))):
        raise ValueError('invalid event')
    if record.get('synthetic') is not True or not isinstance(record.get('amount_minor'),int) or isinstance(record.get('amount_minor'),bool) or record['amount_minor']<0:
        raise ValueError('only valid synthetic payments accepted')
    output={'event_id':record['event_id'],'amount_minor':record['amount_minor'],
            'currency':record.get('currency','GBP'),'synthetic':True,
            'execution_id':execution_id,'attempt_number':attempt,'dataflow_job_id':job_id,
            'spec_digest':spec_digest}
    lineage={'execution_id':execution_id,'attempt_number':attempt,'dataflow_job_id':job_id,
             'spec_digest':spec_digest,'event_id':record['event_id'],
             'output_sha256':digest_payment(output)}
    return output,lineage

def main(argv=None):
    parser=argparse.ArgumentParser()
    parser.add_argument('--input',required=True,help='gs:// immutable bounded JSONL input')
    parser.add_argument('--output_table',required=True)
    parser.add_argument('--lineage_table',required=True)
    parser.add_argument('--execution_id',required=True)
    parser.add_argument('--spec_digest',required=True)
    parser.add_argument('--attempt',type=int,required=True)
    parser.add_argument('--dataflow_job_id',required=True)
    parser.add_argument('--max_records',type=int,default=100)
    args,beam_args=parser.parse_known_args(argv)
    if not args.input.startswith('gs://') or args.max_records<1 or args.max_records>1000:
        raise ValueError('bounded GCS input and max_records 1..1000 required')
    if not re.fullmatch(r'[\w-]+:[\w-]+\.[\w-]+',args.output_table) or not re.fullmatch(r'[\w-]+:[\w-]+\.[\w-]+',args.lineage_table):
        raise ValueError('invalid BigQuery table identifiers')
    validate_identity(args.execution_id,args.spec_digest,args.attempt,args.dataflow_job_id)
    import apache_beam as beam
    from apache_beam.options.pipeline_options import PipelineOptions,StandardOptions,WorkerOptions
    opts=PipelineOptions(beam_args)
    if opts.view_as(StandardOptions).streaming: raise ValueError('streaming is forbidden for bounded pilot')
    workers=opts.view_as(WorkerOptions)
    if workers.max_num_workers is not None and workers.max_num_workers>1:
        raise ValueError('worker cap exceeded')
    with beam.Pipeline(options=opts) as pipeline:
        lines=(pipeline|'Read bounded input'>>beam.io.ReadFromText(args.input))
        # A single bounded manifest is required upstream; no unbounded source.
        rows=(lines|'Decode'>>beam.Map(json.loads)|'Build lineage'>>beam.Map(
            make_rows,execution_id=args.execution_id,spec_digest=args.spec_digest,
            attempt=args.attempt,job_id=args.dataflow_job_id))
        (rows|'Output'>>beam.Map(lambda pair:pair[0])|'Write output'>>beam.io.WriteToBigQuery(
            args.output_table,write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER))
        (rows|'Lineage'>>beam.Map(lambda pair:pair[1])|'Write lineage'>>beam.io.WriteToBigQuery(
            args.lineage_table,write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER))
if __name__=='__main__': main()
