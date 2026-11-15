"""Offline governed Pub/Sub subscriber routing.

Separates governed source lease processing from existing legacy dispatch.
No cloud clients, credentials, publishing or network operations on import.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


class RoutingError(RuntimeError):
    """Governed subscriber routing failure."""


@dataclass(frozen=True)
class RoutingResult:
    route: str
    acknowledged: bool
    reason: str


def route_received(
    *,
    data,
    ack_id,
    bucket,
    acker,
    legacy_process,
    legacy_store,
    legacy_decoder,
    dispatch,
    process_source_with_lease,
    extend_lease,
    process_legacy_with_lease=None,
    renewal_interval_seconds=5.0,
    max_processing_seconds=None,
):
    """Route validated SOURCE and LEGACY messages under lease control."""
    if not isinstance(data, bytes):
        raise RoutingError("message data must be bytes")
    if not isinstance(ack_id, str) or not ack_id:
        raise RoutingError("acknowledgement ID required")
    if not callable(dispatch):
        raise RoutingError("dispatch callback required")
    if not callable(process_source_with_lease):
        raise RoutingError("source lease processor required")
    if not callable(process_legacy_with_lease):
        raise RoutingError("legacy lease processor required")
    if not callable(extend_lease):
        raise RoutingError("lease extension callback required")

    class Received:
        def __init__(self):
            self.data = data

        def ack(self):
            acker.ack(ack_id)

    message = Received()
    source_results = []
    legacy_results = []

    def source_store(received, supplied_bucket):
        result = process_source_with_lease(
            message,
            supplied_bucket,
            extend_lease,
            renewal_interval_seconds=renewal_interval_seconds,
            max_processing_seconds=max_processing_seconds,
        )
        source_results.append(result)
        return result

    class SourceEntry:
        @staticmethod
        def store_one(received, supplied_bucket):
            return source_store(received, supplied_bucket)

    def guarded_legacy_process(
        supplied_data, supplied_ack_id, store, ignored_acker, decoder
    ):
        if supplied_data != data or supplied_ack_id != ack_id:
            raise RoutingError("legacy transport coordinates mismatch")

        result = process_legacy_with_lease(
            message,
            supplied_data,
            supplied_ack_id,
            store,
            decoder,
            legacy_process,
            extend_lease,
            renewal_interval_seconds=renewal_interval_seconds,
            max_processing_seconds=max_processing_seconds,
        )
        legacy_results.append(result)
        return result

    route, result = dispatch(
        data,
        ack_id,
        bucket,
        acker,
        guarded_legacy_process,
        legacy_store,
        legacy_decoder,
        source_store=SourceEntry.store_one,
    )

    if route == "SOURCE":
        if len(source_results) != 1 or legacy_results:
            raise RoutingError("source processing count mismatch")
        lease_result = source_results[0]
    elif route == "LEGACY":
        if len(legacy_results) != 1 or source_results:
            raise RoutingError("legacy processing count mismatch")
        lease_result = legacy_results[0]
    else:
        raise RoutingError("unknown dispatch route")

    if type(getattr(lease_result, "acknowledged", None)) is not bool:
        raise RoutingError("invalid lease result")

    reason = getattr(lease_result, "reason", None)
    if not isinstance(reason, str) or not reason:
        raise RoutingError("missing lease outcome reason")

    return RoutingResult(route, lease_result.acknowledged, reason)
