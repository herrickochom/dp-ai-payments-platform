"""Bounded acknowledgement adapter for governed Pub/Sub messages.

The caller supplies a SubscriberClient-compatible object.
No client creation, network access on import or publishing.
"""

from __future__ import annotations

from math import isfinite


class AcknowledgementConfigurationError(ValueError):
    """Invalid acknowledgement configuration."""


class SourceAcknowledger:
    def __init__(
        self,
        subscriber,
        subscription: str,
        *,
        rpc_timeout_seconds: float = 5.0,
    ):
        if not callable(
            getattr(subscriber, "acknowledge", None)
        ):
            raise AcknowledgementConfigurationError(
                "subscriber must support acknowledge"
            )

        if not isinstance(subscription, str) or not subscription:
            raise AcknowledgementConfigurationError(
                "subscription is required"
            )

        if (
            isinstance(rpc_timeout_seconds, bool)
            or not isinstance(rpc_timeout_seconds, (int, float))
            or not isfinite(rpc_timeout_seconds)
            or rpc_timeout_seconds <= 0
        ):
            raise AcknowledgementConfigurationError(
                "RPC timeout must be finite and positive"
            )

        self.subscriber = subscriber
        self.subscription = subscription
        self.rpc_timeout_seconds = rpc_timeout_seconds

    def ack(self, ack_id: str):
        if not isinstance(ack_id, str) or not ack_id:
            raise AcknowledgementConfigurationError(
                "acknowledgement ID is required"
            )

        return self.subscriber.acknowledge(
            request={
                "subscription": self.subscription,
                "ack_ids": [ack_id],
            },
            timeout=self.rpc_timeout_seconds,
            retry=None,
        )
