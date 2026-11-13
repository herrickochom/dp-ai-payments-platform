"""Offline-only staging/merge acceptance contract. No cloud clients or side effects.

This is a simulation and verification boundary, not a BigQuery execution engine.
An actual live executor requires separate implementation and explicit approval.
"""
from dataclasses import dataclass
import re
from typing import Iterable, Mapping

_RUN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")


class ContractError(ValueError):
    pass


def _count(name: str, value: int) -> int:
    if type(value) is not int or value < 0:
        raise ContractError(f"{name} must be a nonnegative integer")
    return value


def identity(row: Mapping) -> tuple[str, int, int]:
    try:
        topic = row['_kafka_topic']
        partition = row['_kafka_partition']
        offset = row['_kafka_offset']
    except (KeyError, TypeError) as exc:
        raise ContractError('missing Kafka identity') from exc
    if not isinstance(topic, str) or not topic.strip():
        raise ContractError('invalid Kafka topic')
    if type(partition) is not int or partition < 0 or type(offset) is not int or offset < 0:
        raise ContractError('invalid Kafka partition/offset')
    return (topic, partition, offset)


@dataclass(frozen=True)
class StagePlan:
    run_id: str
    input_count: int
    accepted_count: int
    quarantine_count: int
    duplicate_count: int
    staging_count: int
    existing_bronze_count: int
    inserted_count: int
    final_bronze_count: int


def verify_plan(*, run_id: str, input_count: int, quarantine_count: int,
                accepted_rows: Iterable[Mapping], staging_rows: Iterable[Mapping],
                existing_identities: Iterable[tuple[str, int, int]],
                inserted_count: int, final_bronze_count: int) -> StagePlan:
    """Validate a run's simulated source, staging, merge and sink evidence.

    The input counts represent records after Avro parsing; each record is either
    quarantined or accepted. Accepted duplicates are not silently discarded:
    same-identity conflicting content is rejected. Existing Bronze identities
    are used to calculate expected inserts, without connecting to BigQuery.
    """
    if not isinstance(run_id, str) or not _RUN.fullmatch(run_id):
        raise ContractError('invalid run_id')
    input_count = _count('input_count', input_count)
    quarantine_count = _count('quarantine_count', quarantine_count)
    inserted_count = _count('inserted_count', inserted_count)
    final_bronze_count = _count('final_bronze_count', final_bronze_count)
    accepted = list(accepted_rows)
    staged = list(staging_rows)
    if input_count != len(accepted) + quarantine_count:
        raise ContractError('source reconciliation mismatch')
    unique = {}
    for row in accepted:
        key = identity(row)
        if key in unique and row != unique[key]:
            raise ContractError('conflicting duplicate Kafka identity')
        unique[key] = row
    staged_keys = [identity(row) for row in staged]
    if len(staged_keys) != len(set(staged_keys)):
        raise ContractError('staging contains duplicate Kafka identities')
    if set(staged_keys) != set(unique):
        raise ContractError('staging identities do not match accepted unique records')
    for row in staged:
        if row != unique[identity(row)]:
            raise ContractError('staging content mismatch')
    existing = list(existing_identities)
    if len(existing) != len(set(existing)):
        raise ContractError('preexisting Bronze identities not unique')
    for key in existing:
        if not isinstance(key, tuple) or len(key) != 3:
            raise ContractError('invalid existing identity')
        identity(dict(zip(('_kafka_topic', '_kafka_partition', '_kafka_offset'), key)))
    expected_inserts = len(set(unique) - set(existing))
    if inserted_count != expected_inserts:
        raise ContractError('merge insert count mismatch')
    if final_bronze_count != len(existing) + expected_inserts:
        raise ContractError('final Bronze count mismatch')
    return StagePlan(run_id, input_count, len(accepted), quarantine_count,
                     len(accepted) - len(unique), len(staged), len(existing),
                     inserted_count, final_bronze_count)


def verify_distinct_run_ids(run_ids: Iterable[str]) -> None:
    seen = set()
    for run_id in run_ids:
        if not isinstance(run_id, str) or not _RUN.fullmatch(run_id) or run_id in seen:
            raise ContractError('invalid or reused run_id')
        seen.add(run_id)
