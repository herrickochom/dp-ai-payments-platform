"""Batch ingestion of payment events from GCS into BigQuery."""

import argparse
import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


REQUIRED_FIELDS = (
    "event_id",
    "payment_id",
    "event_type",
    "amount_minor",
    "currency",
    "event_timestamp",
)


def transform_payment_event(line):
    """Validate and transform one JSON payment event."""

    event = json.loads(line)

    missing = [
        field for field in REQUIRED_FIELDS
        if field not in event or event[field] is None
    ]

    if missing:
        raise ValueError(f"Missing payment fields: {missing}")

    amount = event["amount_minor"]

    if isinstance(amount, bool) or not isinstance(amount, int):
        raise ValueError("amount_minor must be an integer")

    if amount < 0:
        raise ValueError("amount_minor cannot be negative")

    timestamp = datetime.fromisoformat(
        event["event_timestamp"].replace("Z", "+00:00")
    )

    if timestamp.tzinfo is None:
        raise ValueError("event_timestamp must include a timezone")

    timestamp = timestamp.astimezone(timezone.utc)

    return {
        "event_id": str(event["event_id"]),
        "payment_id": str(event["payment_id"]),
        "event_type": str(event["event_type"]),
        "amount_minor": amount,
        "currency": str(event["currency"]),
        "event_timestamp": timestamp.isoformat(),
    }


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
