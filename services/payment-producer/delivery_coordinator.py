"""Durable Kafka delivery coordination; offline integration component."""

from delivery_journal import RecoveryBlocked


class DeliveryFailure(RecoveryBlocked):
    pass


class DeliveryCoordinator:
    def __init__(self, journal, producer):
        self.journal = journal
        self.producer = producer
        self.delivered = []
        self.failures = []
        self.inflight = set()
        self.attempted = 0

        # Reject unresolved records inherited from earlier runs.
        self.journal.assert_recovery_safe()

    def _check_failures(self):
        if self.failures:
            raise DeliveryFailure(
                f"{len(self.failures)} delivery failures: "
                f"{self.failures[0]}"
            )

    def publish(self, topic, event_id, content, key, wire):
        self._check_failures()

        if not isinstance(content, bytes):
            raise TypeError("content must be bytes")
        if not event_id or not topic:
            raise ValueError("event identity and topic required")
        if event_id in self.inflight:
            raise RecoveryBlocked(
                f"Duplicate in-flight event: {event_id}"
            )

        digest = self.journal.fingerprint(content)

        # Durable admission precedes any Kafka produce call.
        self.journal.begin(event_id, topic, digest)
        self.inflight.add(event_id)
        self.attempted += 1

        def callback(error, message):
            if error is not None:
                try:
                    self.journal.mark_uncertain(event_id)
                except Exception as exc:
                    self.failures.append(
                        f"{event_id}: journal error: {exc}"
                    )
                    return

                self.failures.append(
                    f"{event_id}: delivery error: {error}"
                )
                return

            try:
                actual_topic = message.topic()
                if actual_topic != topic:
                    raise DeliveryFailure(
                        f"Topic mismatch: {event_id}"
                    )

                partition = message.partition()
                offset = message.offset()

                # Confirm durably before reporting delivery.
                self.journal.confirm(
                    event_id, topic, partition, offset
                )

                self.delivered.append({
                    "event_id": event_id,
                    "topic": actual_topic,
                    "partition": str(partition),
                    "offset": str(offset),
                    "business_key": (
                        message.key() or b""
                    ).decode("utf-8"),
                })
                self.inflight.discard(event_id)

            except Exception as exc:
                self.failures.append(
                    f"{event_id}: acknowledgement error: {exc}"
                )

        # Exceptions leave PENDING durable for reconciliation.
        self.producer.produce(
            topic=topic,
            key=key,
            value=wire,
            on_delivery=callback,
        )

        self._check_failures()
        self.producer.poll(0)
        self._check_failures()

    def finish(self, timeout=60):
        remaining = self.producer.flush(timeout=timeout)

        if remaining != 0:
            raise DeliveryFailure(
                f"Incomplete Kafka flush: {remaining} outstanding"
            )

        self._check_failures()

        if self.inflight:
            raise DeliveryFailure(
                f"{len(self.inflight)} deliveries unresolved"
            )

        if len(self.delivered) != self.attempted:
            raise DeliveryFailure(
                "Acknowledgement count mismatch"
            )

        self.journal.assert_recovery_safe()
        return list(self.delivered)
