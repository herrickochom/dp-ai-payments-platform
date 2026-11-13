"""Stage 14F-C: real Avro OCF -> existing validation -> offline durable sink.

Injected objects and sink only. No SDK clients, network, GCP writes or live CLI.
This is NOT a live GCS/BigQuery runner. No durable remote job reconciliation.
"""
from __future__ import annotations
import io
from collections import Counter
from fastavro import reader as avro_reader
from .pipeline import Validate
from .offline_integration import execute_offline
from .durable_execution_adapter import ContractError, fingerprint_records

def prepare_objects(objects):
    """Accept pre-fetched (path, bytes) pairs, never fetch cloud objects here."""
    if not isinstance(objects, list) or not objects:
        raise ContractError("nonempty materialized object list required")
    rows, quarantine = [], []
    input_records = 0
    for path, content in objects:
        if not isinstance(path,str) or not path or not isinstance(content,bytes):
            raise ContractError("invalid pre-fetched object")
        try:
            decoded = list(avro_reader(io.BytesIO(content)))
            if not decoded:
                raise ValueError("empty Avro object")
        except Exception as exc:
            input_records += 1
            quarantine.append({"source_path":path,"reason":f"parse: {type(exc).__name__}"})
            continue
        for rec in decoded:
            input_records += 1
            if not isinstance(rec,dict):
                quarantine.append({"source_path":path,"reason":"non-dict record"})
                continue
            rec = dict(rec, _raw_object_path=path)
            for item in Validate().process(rec):
                # Beam TaggedOutput used by the reviewed Validate contract.
                if getattr(item, "tag", None) == "quarantine":
                    quarantine.append(item.value)
                else:
                    rows.append(item)
    unique = {}
    duplicates = 0
    for row in rows:
        identity=(row["_kafka_topic"],row["_kafka_partition"],row["_kafka_offset"])
        if identity in unique:
            # Do not silently pick a winner when the same Kafka identity has
            # divergent event payloads or source lineage.
            if row != unique[identity]:
                raise ContractError("conflicting duplicate Kafka identity")
            duplicates += 1
        else:
            unique[identity]=row
    accepted=list(unique.values())
    fingerprint_records(accepted)
    if input_records != len(accepted)+duplicates+len(quarantine):
        raise ContractError("source accounting mismatch")
    return {"input_records":input_records,"staging_rows":accepted,
            "quarantine":quarantine,"duplicate_records":duplicates}

def execute_prefetched_offline(*, objects, ledger, fake_client, run_id,
                               staging_table, bronze_table):
    """No cloud client construction. Reuse the existing ledger and MERGE contract."""
    prepared=prepare_objects(objects)
    result=execute_offline(
        ledger=ledger, fake_client=fake_client, run_id=run_id,
        staging_rows=prepared["staging_rows"],
        staging_table=staging_table, bronze_table=bronze_table,
        input_records=prepared["input_records"],
        quarantined_records=len(prepared["quarantine"]),
        duplicate_records=prepared["duplicate_records"])
    return {"status":result["status"],"reconciliation":result["reconciliation"],
            "quarantine":prepared["quarantine"],
            "merge_sql":result["merge_sql"]}
