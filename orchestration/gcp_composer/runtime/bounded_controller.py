"""Offline bounded payment execution controller.

No GCP SDK, network access or cloud execution.
SQLite provides a local durable idempotency ledger.
"""

import hashlib
import json
import sqlite3
from pathlib import Path


class ContractError(ValueError):
    pass


def validate_manifest(manifest):
    if not isinstance(manifest, dict):
        raise ContractError("Manifest must be an object.")

    if set(manifest) != {"schema_version", "source", "objects"}:
        raise ContractError("Unexpected or missing manifest fields.")

    if manifest["schema_version"] != 1:
        raise ContractError("Unsupported manifest schema.")

    if manifest["source"] != "payments":
        raise ContractError("Unsupported source.")

    objects = manifest["objects"]

    if not isinstance(objects, list) or not 1 <= len(objects) <= 1000:
        raise ContractError("Manifest requires 1 to 1000 objects.")

    identities = set()

    for obj in objects:
        if not isinstance(obj, dict):
            raise ContractError("Invalid object.")

        if set(obj) != {"uri", "generation", "sha256"}:
            raise ContractError("Invalid object fields.")

        uri = obj["uri"]
        generation = obj["generation"]
        digest = obj["sha256"]

        if (
            not isinstance(uri, str)
            or not uri.startswith("gs://")
            or "*" in uri
            or "?" in uri
            or len(uri) > 1024
            or len(uri.split("/", 3)) < 4
            or not uri.split("/", 3)[2]
            or not uri.split("/", 3)[3]
        ):
            raise ContractError("Object URI must be explicit and bounded.")

        if (
            not isinstance(generation, str)
            or not generation.isdecimal()
            or len(generation) > 30
            or int(generation) <= 0
        ):
            raise ContractError("Invalid object generation.")

        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(c not in "0123456789abcdef" for c in digest)
        ):
            raise ContractError("Invalid SHA-256.")

        identity = (uri, generation)
        if identity in identities:
            raise ContractError("Duplicate object identity.")

        identities.add(identity)

    return {
        "schema_version": 1,
        "source": "payments",
        "objects": sorted(
            objects,
            key=lambda x: (x["uri"], int(x["generation"]))
        ),
    }


def execution_id(manifest):
    canonical = validate_manifest(manifest)
    payload = json.dumps(
        canonical, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")

    return hashlib.sha256(payload).hexdigest()


class ExecutionLedger:
    def __init__(self, path):
        self.path = Path(path)

    def _connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(str(self.path), timeout=10)
        con.execute("PRAGMA busy_timeout = 10000")
        con.execute("""
            CREATE TABLE IF NOT EXISTS executions (
                execution_id TEXT PRIMARY KEY,
                manifest_json TEXT NOT NULL,
                status TEXT NOT NULL
                    CHECK (status IN ('ADMITTED', 'BLOCKED')),
                created_at TEXT NOT NULL
                    DEFAULT CURRENT_TIMESTAMP
            )
        """)
        return con

    def admit(self, manifest):
        canonical = validate_manifest(manifest)
        identifier = execution_id(canonical)
        payload = json.dumps(
            canonical, sort_keys=True, separators=(",", ":")
        )

        with self._connect() as con:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT manifest_json, status FROM executions "
                "WHERE execution_id = ?",
                (identifier,),
            ).fetchone()

            if row is not None:
                if row[0] != payload:
                    raise ContractError("Execution identity collision.")

                return {
                    "execution_id": identifier,
                    "decision": "DUPLICATE",
                    "status": row[1],
                }

            con.execute(
                "INSERT INTO executions "
                "(execution_id, manifest_json, status) "
                "VALUES (?, ?, ?)",
                (identifier, payload, "BLOCKED"),
            )

        return {
            "execution_id": identifier,
            "decision": "NEW",
            "status": "BLOCKED",
        }


def inspect_manifest_file(path, ledger_path):
    with open(path, encoding="utf-8") as handle:
        manifest = json.load(handle)

    return ExecutionLedger(ledger_path).admit(manifest)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--ledger", required=True)
    args = parser.parse_args()

    result = inspect_manifest_file(args.manifest, args.ledger)
    print(json.dumps(result, indent=2, sort_keys=True))
