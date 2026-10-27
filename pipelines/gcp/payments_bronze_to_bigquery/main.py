"""Batch ingestion of payment events from GCS into BigQuery."""

import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from transform import transform_payment_event


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--output_table", required=True)

    args, beam_args = parser.parse_known_args()

    options = PipelineOptions(beam_args)

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Read Bronze JSON" >> beam.io.ReadFromText(args.input)
            | "Validate Payment Events" >> beam.Map(transform_payment_event)
            | "Write BigQuery" >> beam.io.WriteToBigQuery(
                args.output_table,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )


if __name__ == "__main__":
    main()
