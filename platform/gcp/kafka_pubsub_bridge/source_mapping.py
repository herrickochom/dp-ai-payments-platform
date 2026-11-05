"""GCP-KP-001F: source contract evidence and fail-closed mapping eligibility.

This module deliberately does NOT fabricate payment_id, amount_minor or currency.
It has no cloud, Kafka or external dependencies. Never used as a live publisher.
"""
from dataclasses import dataclass
from typing import Any, Mapping

BUSINESS_SOURCE = 'com.payments.platform.PaymentEvent'
TECHNICAL_SOURCE = 'com.payments.platform.TechnicalPaymentEvent'
TARGET_FIELDS = ('event_id', 'payment_id', 'event_type', 'amount_minor', 'currency', 'event_timestamp')

@dataclass(frozen=True)
class MappingAssessment:
    source_schema: str
    eligible: bool
    blockers: tuple[str, ...]
    candidate_fields: tuple[tuple[str, str], ...]


def assess(source_schema: str, record: Mapping[str, Any]) -> MappingAssessment:
    """Report only grounded candidate relationships; never approve an inferred mapping."""
    if not isinstance(record, Mapping):
        raise TypeError('decoded Avro record must be a mapping')
    if source_schema == BUSINESS_SOURCE:
        blockers = [
            'payment_id: no governed source field mapping approved',
            'amount_minor: instructed_amount is nullable double; currency scale and precision policy not approved',
            'event_timestamp: timestamp equivalence not approved',
        ]
        if not isinstance(record.get('event_id'), str) or not record['event_id'].strip():
            blockers.append('event_id: missing or invalid')
        if not isinstance(record.get('event_type'), str) or not record['event_type'].strip():
            blockers.append('event_type: missing or invalid')
        if not isinstance(record.get('currency'), str) or not record['currency'].strip():
            blockers.append('currency: nullable source value absent or invalid')
        return MappingAssessment(source_schema, False, tuple(blockers),
            (('event_id','event_id'), ('event_type','event_type'), ('currency','currency (nullable)'),
             ('timestamp','event_timestamp (candidate only)'),
             ('instructed_amount','amount_minor (not safe without policy)')))
    if source_schema == TECHNICAL_SOURCE:
        return MappingAssessment(source_schema, False, (
            'payment_id: no governed source field mapping approved',
            'amount_minor: absent from technical contract',
            'currency: absent from technical contract',
        ), (('event_id','event_id'), ('event_type','event_type'),
            ('event_timestamp','event_timestamp')))
    return MappingAssessment(source_schema, False, ('unapproved Schema Registry subject or Avro record type',), ())


def require_approved_mapping(source_schema: str, record: Mapping[str, Any]) -> dict:
    assessment = assess(source_schema, record)
    raise ValueError('GCP-KP-001F mapping blocked: ' + '; '.join(assessment.blockers))
