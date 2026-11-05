"""Offline payment lifecycle simulation.

This is a synthetic, deterministic state-machine test harness.
It performs no GCP calls, Dataflow execution or BigQuery writes.
"""

import hashlib
import json
import sqlite3
from pathlib import Path

from bounded_controller import validate_manifest, execution_id


STATES = (
    "ADMITTED",
    "RUNNING",
    "FAILED",
    "ACCEPTED",
)

SIMULATION_VERSION = "local-simulation-v1"


def expected_evidence(manifest):
    canonical = validate_manifest(manifest)
    payload = json.dumps(
        canonical, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")

    return {
        "object_count": len(canonical["objects"]),
        "manifest_sha256": hashlib.sha256(payload).hexdigest(),
        "simulator": SIMULATION_VERSION,
    }


class LocalSimulator:
    def __init__(self, database):
        self.database = Path(database)

    def connect(self):
        self.database.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(str(self.database), timeout=10)
        con.execute("PRAGMA busy_timeout=10000")
        con.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                execution_id TEXT PRIMARY KEY,
                manifest_json TEXT NOT NULL,
                state TEXT NOT NULL
                    CHECK (state IN (
                        'ADMITTED', 'RUNNING',
                        'FAILED', 'ACCEPTED'
                    )),
                attempts INTEGER NOT NULL DEFAULT 0,
                evidence_json TEXT,
                updated_at TEXT NOT NULL
                    DEFAULT CURRENT_TIMESTAMP
            )
        """)
        return con

    def _transaction(self, con):
        con.execute("BEGIN IMMEDIATE")

    def _read(self, con, identifier):
        return con.execute(
            "SELECT manifest_json, state, attempts, evidence_json "
            "FROM runs WHERE execution_id=?",
            (identifier,),
        ).fetchone()

    def inspect(self, identifier):
        with self.connect() as con:
            row = self._read(con, identifier)

        if row is None:
            return None

        return {
            "execution_id": identifier,
            "state": row[1],
            "attempts": row[2],
            "evidence": json.loads(row[3]) if row[3] else None,
        }

    def admit(self, manifest):
        canonical = validate_manifest(manifest)
        identifier = execution_id(canonical)
        payload = json.dumps(
            canonical, sort_keys=True, separators=(",", ":")
        )

        with self.connect() as con:
            self._transaction(con)
            row = self._read(con, identifier)

            if row is None:
                con.execute(
                    "INSERT INTO runs "
                    "(execution_id, manifest_json, state) "
                    "VALUES (?, ?, 'ADMITTED')",
                    (identifier, payload),
                )
                decision = "NEW"
            else:
                if row[0] != payload:
                    raise ValueError("Execution identity collision")
                decision = "DUPLICATE"

        return {
            "execution_id": identifier,
            "decision": decision,
        }

    def execute(self, manifest, fail_before_acceptance=False):
        admitted = self.admit(manifest)
        identifier = admitted["execution_id"]
        evidence = expected_evidence(manifest)

        with self.connect() as con:
            self._transaction(con)
            row = self._read(con, identifier)

            if row[1] == "ACCEPTED":
                return {
                    "decision": "ALREADY_ACCEPTED",
                    **self.inspect(identifier),
                }

            if row[1] == "RUNNING":
                return {
                    "decision": "IN_PROGRESS",
                    "execution_id": identifier,
                    "state": "RUNNING",
                }

            if row[1] not in ("ADMITTED", "FAILED"):
                raise ValueError("Unexpected execution state")

            con.execute(
                "UPDATE runs SET state='RUNNING', "
                "attempts=attempts+1, "
                "updated_at=CURRENT_TIMESTAMP "
                "WHERE execution_id=?",
                (identifier,),
            )

        # Simulated execution only. No external side effects.
        if fail_before_acceptance:
            with self.connect() as con:
                self._transaction(con)
                con.execute(
                    "UPDATE runs SET state='FAILED', "
                    "updated_at=CURRENT_TIMESTAMP "
                    "WHERE execution_id=? AND state='RUNNING'",
                    (identifier,),
                )
            return {
                "decision": "SIMULATED_FAILURE",
                **self.inspect(identifier),
            }

        with self.connect() as con:
            self._transaction(con)
            row = self._read(con, identifier)

            if row[1] != "RUNNING":
                raise ValueError("Unexpected state before acceptance")

            con.execute(
                "UPDATE runs SET state='ACCEPTED', "
                "evidence_json=?, "
                "updated_at=CURRENT_TIMESTAMP "
                "WHERE execution_id=? AND state='RUNNING'",
                (json.dumps(evidence, sort_keys=True), identifier),
            )

        return {
            "decision": "SIMULATED_ACCEPTANCE",
            **self.inspect(identifier),
        }

    def recover_failed(self, identifier):
        with self.connect() as con:
            row = self._read(con, identifier)
            if row is None or row[1] != "FAILED":
                raise ValueError("Only failed runs may be recovered")
            manifest = json.loads(row[0])

        return self.execute(manifest)


def example_manifest():
    return {
        "schema_version": 1,
        "source": "payments",
        "objects": [
            {
                "uri": "gs://example-bucket/payments/batch-001.json",
                "generation": "12345",
                "sha256": "a" * 64,
            },
            {
                "uri": "gs://example-bucket/payments/batch-002.json",
                "generation": "12346",
                "sha256": "b" * 64,
            },
        ],
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True)
    args = parser.parse_args()

    simulator = LocalSimulator(args.database)
    manifest = example_manifest()

    first = simulator.execute(
        manifest, fail_before_acceptance=True
    )
    recovered = simulator.recover_failed(first["execution_id"])
    replay = simulator.execute(manifest)

    print(json.dumps({
        "initial": first,
        "recovered": recovered,
        "replay": replay,
    }, indent=2, sort_keys=True))
