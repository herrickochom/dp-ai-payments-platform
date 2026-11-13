"""Offline-testable Pub/Sub synchronous Pull lease-extension adapter.

No client creation, credentials, network access on import, or acknowledgement.
The caller supplies a SubscriberClient-compatible object and the ack ID
returned by Pull. The existing lease guard owns acknowledgement.
"""

from __future__ import annotations


class LeaseExtensionError(RuntimeError):
    """Invalid lease-extension configuration."""


class SourceLeaseExtender:
    def __init__(
        self,
        subscriber,
        subscription: str,
        ack_id: str,
        *,
        extension_seconds: int = 30,
        rpc_timeout_seconds: float = 5.0,
    ):
        if subscriber is None or not callable(
            getattr(subscriber, "modify_ack_deadline", None)
        ):
            raise LeaseExtensionError(
                "subscriber must support modify_ack_deadline"
            )

        if not isinstance(subscription, str) or not subscription:
            raise LeaseExtensionError("subscription is required")

        if not isinstance(ack_id, str) or not ack_id:
            raise LeaseExtensionError("acknowledgement ID is required")

        if type(extension_seconds) is not int or not (
            1 <= extension_seconds <= 600
        ):
            raise LeaseExtensionError(
                "extension_seconds must be an integer from 1 to 600"
            )

        if not isinstance(rpc_timeout_seconds, (int, float)) or (
            isinstance(rpc_timeout_seconds, bool)
        ) or not 0 < rpc_timeout_seconds < float("inf"):
            raise LeaseExtensionError(
                "RPC timeout must be finite and positive"
            )

        self.subscriber = subscriber
        self.subscription = subscription
        self.ack_id = ack_id
        self.extension_seconds = extension_seconds
        self.rpc_timeout_seconds = rpc_timeout_seconds

    def extend(self, message):
        """Extend the supplied Pull acknowledgement ID.

        The message argument is accepted for compatibility with
        lease_guard.process_with_renewal. It is not used to derive
        or replace the acknowledgement ID.
        """
        return self.subscriber.modify_ack_deadline(
            request={
                "subscription": self.subscription,
                "ack_ids": [self.ack_id],
                "ack_deadline_seconds": self.extension_seconds,
            },
            timeout=self.rpc_timeout_seconds,
            retry=None,
        )
