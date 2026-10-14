# STR-002B/B2 — Source Parse Verification

- Status: Recorded
- Date: 2026-10-14
- Ticket: STR-002B/B2 item 3
- Related: j1_lifecycle_state_skeleton.py, STR-002A-DEV §4

## Finding

The J1 skeleton declares a single Table API Kafka source over both
`icmn.pmn.pain001` and `cpo.plm.pain002`. The two topics carry different
registry subjects with different Avro record names (`TechnicalPaymentEvent`,
registry id 1; `PaymentEvent`, registry id 6).

It was previously suspected that this mismatch caused one of the two topics to
be misparsed. Runtime evidence contradicts that suspicion.

The TaskManager log records successful parse of PLM records with all required
inner fields present:

    {"correlation_id": "4a09e40e-...", "current": {"last_seen_at":
    "2026-08-29T12:00:00.000+03:00", "last_stage": "mobile-money-credit",
    "last_status": "PENDING", ...}, "event_type": "PLM", "previous": null}

Both topics populate `parsed_event_data` with a JSON body carrying
`correlation_id`, `technical_stage`, `technical_status`, and `event_timestamp`.
The current `avro-confluent` deserialization resolves the differing envelopes
via column-name overlap; the fields the job projects exist in both registered
schemas under the same names.

## Consequence

Phase 1 of the sink implementation plan is reduced. The source declaration is
not split into two tables. Only Kafka metadata columns (`topic`, `partition`,
`offset`) are added, because the `LifecycleTransition` contract requires them.

## Evidence

- TaskManager log lines from 2026-10-14 13:21:08 (PLM records).
- Counter baseline from the same run: 1428 / 476 / 238 / 952.
