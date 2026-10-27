"""Route valid payment records and quarantine invalid records."""

from datetime import datetime, timezone

import apache_beam as beam
from apache_beam import pvalue

from transform import transform_payment_event


class ValidateAndRoute(beam.DoFn):
    """Emit validated records or tagged quarantine records."""

    def process(self, line):
        try:
            result = transform_payment_event(line)
        except (ValueError, TypeError, KeyError, AttributeError) as exc:
            yield pvalue.TaggedOutput(
                "invalid",
                {
                    "raw_payload": line,
                    "error_reason": (
                        f"{type(exc).__name__}: {str(exc)[:1000]}"
                    ),
                    "processing_timestamp": datetime.now(
                        timezone.utc
                    ).isoformat(),
                },
            )
        else:
            yield result
