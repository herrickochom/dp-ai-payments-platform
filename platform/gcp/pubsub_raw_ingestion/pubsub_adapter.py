"""GCP-KP-001W callback adapter. Importing this module performs no cloud operation.

Call `handle_message(message, store)` from a separately reviewed subscriber lifecycle.
The Pub/Sub message is acknowledged only by the verified worker core, after
immutable GCS creation or identical-object readback. Exceptions propagate; no ack.
"""
from .worker import process


class _MessageAcker:
    def __init__(self, message):
        self.message = message

    def ack(self, _ack_id):
        self.message.ack()


def handle_message(message, store):
    """Process a received streaming-pull message; never nack/ack on failures."""
    if not isinstance(message.data, bytes):
        raise TypeError('Pub/Sub message.data must be bytes')
    return process(message.data, 'streaming-message', store, _MessageAcker(message))
