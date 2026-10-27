"""Batch payment ingestion with durable invalid-record quarantine."""

import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from routing import ValidateAndRoute


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output_table", required=True)
    parser.add_argument("--quarantine_table", required=True)
    parser.add_argument("--bq_temp_location", required=True)

    args, beam_args = parser.parse_known_args()
    options = PipelineOptions(beam_args)

    with beam.Pipeline(options=options) as pipeline:
        records = (
            pipeline
            | "Read Bronze JSON" >> beam.io.ReadFromText(args.input)
            | "Validate and Route" >> beam.ParDo(
                ValidateAndRoute()
            ).with_outputs("invalid", main="valid")
        )

        (
            records.valid
            | "Write Valid Payments" >> beam.io.WriteToBigQuery(
                args.output_table,
                custom_gcs_temp_location=args.bq_temp_location,
                method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )

        (
            records.invalid
            | "Write Quarantined Payments" >> beam.io.WriteToBigQuery(
                args.quarantine_table,
                custom_gcs_temp_location=args.bq_temp_location,
                method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )


if __name__ == "__main__":
    main()
