"""Safety controls for controlled Raw v1 remediation."""

import fcntl
import hashlib
import json
import os

from contextlib import contextmanager
from pathlib import Path

import raw_v1_metadata_remediation as executor


def require(condition, message):
    if not condition:
        raise ValueError(message)


def validate_scope(manifest, approved):
    require(manifest == approved, "Manifest differs from approved scope")

    objects = manifest["objects"]

    require(len(objects) == 10, "Pilot must contain ten objects")

    names = [item["object"] for item in objects]

    require(len(set(names)) == 10, "Duplicate objects")

    require(
        manifest["bulk_execution_authorised"] is False,
        "Bulk execution prohibited"
    )

    require(
        manifest["execution_authorised"] is False,
        "Unexpected embedded authorisation"
    )

    require(
        all(item["execution_authorised"] is False for item in objects),
        "Unexpected object authorisation"
    )

    conflicts = [
        item for item in objects
        if item["remediation_type"] == "EXPLICIT_CONFLICT_MIGRATION"
    ]

    require(len(conflicts) == 1, "Unexpected conflict count")

    conflict = conflicts[0]

    require(
        conflict["object"] == executor.CONFLICT_OBJECT,
        "Unexpected conflict object"
    )

    require(
        conflict["before"]["metadata"]["data-domain"] == "payments",
        "Unexpected original domain"
    )

    require(
        conflict["expected_metadata"]["data-domain"]
        == "agent_management",
        "Unexpected target domain"
    )

    require(
        all(
            item["remediation_type"] in {
                "STANDARD_METADATA_REMEDIATION",
                "EXPLICIT_CONFLICT_MIGRATION"
            }
            for item in objects
        ),
        "Unexpected remediation type"
    )

    return objects


@contextmanager
def execution_lock(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)

    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(
                "Another controlled executor holds the lock"
            ) from exc

        yield

    finally:
        os.close(fd)


def reconcile_journal(path, approved_names):
    path = Path(path)

    pending = set()
    completed = set()
    quarantined = set()

    if not path.exists():
        return {
            "pending": [],
            "completed": [],
            "quarantined": []
        }

    for line in path.read_text().splitlines():
        require(bool(line.strip()), "Empty journal entry")

        event = json.loads(line)

        name = event["object"]
        status = event["status"]

        require(name in approved_names, "Unapproved journal object")

        if status == "PATCH_INTENT":
            require(name not in pending, "Duplicate unresolved intent")
            require(name not in completed, "Patch after completion")
            require(name not in quarantined, "Patch after quarantine")
            pending.add(name)

        elif status == "UPDATED_VERIFIED":
            require(name in pending, "Completion without patch intent")
            require(
                event.get("payload_verified") is True,
                "Unverified completion"
            )
            pending.remove(name)
            completed.add(name)

        elif status == "ALREADY_COMPLIANT":
            require(
                name not in pending,
                "Unresolved patch requires independent recovery"
            )
            require(
                event.get("payload_verified") is True,
                "Unverified compliant state"
            )
            completed.add(name)

        elif status == "QUARANTINED_REVIEW_REQUIRED":
            quarantined.add(name)

        elif status == "DRY_RUN_PASS":
            require(
                event.get("payload_verified") is True,
                "Unverified dry-run"
            )

        else:
            raise ValueError("Unknown journal status")

    return {
        "pending": sorted(pending),
        "completed": sorted(completed),
        "quarantined": sorted(quarantined)
    }


def require_reconciled(state):
    require(not state["pending"], "Unresolved patch intent")
    require(not state["quarantined"], "Quarantined objects require review")


class ReadOnlyBlob:
    """Expose only the object operations required for validation."""

    def __init__(self, blob):
        self._blob = blob

    def reload(self, *args, **kwargs):
        return self._blob.reload(*args, **kwargs)

    def download_as_bytes(self, *args, **kwargs):
        return self._blob.download_as_bytes(*args, **kwargs)

    def __getattr__(self, name):
        allowed = {
            "name", "generation", "metageneration", "crc32c",
            "md5_hash", "size", "content_type", "cache_control",
            "content_encoding", "content_disposition",
            "content_language", "custom_time", "metadata"
        }

        if name not in allowed:
            raise AttributeError(f"Operation prohibited: {name}")

        return getattr(self._blob, name)

    def patch(self, *args, **kwargs):
        raise RuntimeError("Cloud writes prohibited")


class ReadOnlyBucket:
    def __init__(self, bucket):
        self._bucket = bucket

    def blob(self, name):
        return ReadOnlyBlob(self._bucket.blob(name))


def run_read_only(bucket, manifest_path, decoder, journal, lock_path):
    approved = executor.load_manifest(manifest_path)

    objects = validate_scope(approved, approved)

    names = {item["object"] for item in objects}

    with execution_lock(lock_path):
        state = reconcile_journal(journal, names)
        require_reconciled(state)

        require(not Path(journal).exists(), "Use a fresh dry-run journal")

        readonly = ReadOnlyBucket(bucket)

        results = executor.run_pilot(
            readonly,
            approved,
            decoder,
            journal,
            apply=False,
            allow_conflict=True
        )

        require(len(results) == 10, "Incomplete pilot")

        require(
            all(
                item["status"] in {
                    "DRY_RUN_PASS", "ALREADY_COMPLIANT"
                }
                and item["payload_verified"] is True
                for item in results
            ),
            "Unexpected dry-run result"
        )

        require_reconciled(reconcile_journal(journal, names))

        return results


# The approval record is deliberately not created by implementation.
APPROVAL_PATH = (
    executor.ROOT / "evidence/metadata"
    / "raw-v1-controlled-live-remediation"
    / "execution-approval.json"
)

EXECUTION_DIRECTORY = APPROVAL_PATH.parent

LOCK_PATH = (
    executor.ROOT / "evidence/metadata"
    / ".locks/raw-v1-remediation.lock"
)


def validate_approval(path):
    path = Path(path)

    require(path.is_file(), "Persisted live approval required")

    approval = json.loads(path.read_text())

    required = {
        "capability": "raw_metadata_controlled_live_remediation",
        "approval_authority": "user",
        "approval_status": "APPROVED",
        "project": executor.PROJECT,
        "bucket": executor.BUCKET,
        "metadata_contract_version": "1.0.0",
        "pilot_manifest_sha256": executor.PILOT_SHA,
        "pilot_objects": 10,
        "live_write_authorised": True,
        "conflict_migration_authorised": True,
        "bulk_execution_authorised": False
    }

    for key, value in required.items():
        require(
            key in approval and
            type(approval[key]) is type(value) and
            approval[key] == value,
            f"Invalid approval field: {key}"
        )

    return approval


def durable_event(path, event):
    executor.append_event(path, event)

    # Persist the directory entry as well as the journal content.
    fd = os.open(Path(path).parent, os.O_RDONLY)

    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def bind_journal(directory, manifest_path, approval_path):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)

    identity = {
        "project": executor.PROJECT,
        "bucket": executor.BUCKET,
        "pilot_manifest_sha256": hashlib.sha256(
            Path(manifest_path).read_bytes()
        ).hexdigest(),
        "approval_sha256": hashlib.sha256(
            Path(approval_path).read_bytes()
        ).hexdigest(),
        "pilot_objects": 10
    }

    path = directory / "execution-identity.json"
    journal = directory / "execution-journal.jsonl"

    if path.exists():
        require(
            json.loads(path.read_text()) == identity,
            "Execution journal identity mismatch"
        )

    else:
        require(
            not journal.exists(),
            "Unbound execution journal"
        )

        with path.open("x") as f:
            json.dump(identity, f, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())

        fd = os.open(directory, os.O_RDONLY)

        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    return journal


def validate_resume_state(bucket, objects, decoder, state):
    require_reconciled(state)

    completed = set(state["completed"])

    for item in objects:
        _, _, _, _, compliant = executor.assess(
            bucket, item, decoder, allow_conflict=True
        )

        if item["object"] in completed:
            require(
                compliant,
                "Previously completed object is no longer compliant"
            )
        else:
            require(
                not compliant,
                "Compliant object without verified execution evidence"
            )


def run_controlled_apply(bucket, manifest_path, decoder):
    approved = executor.load_manifest(manifest_path)
    objects = validate_scope(approved, approved)
    names = {item["object"] for item in objects}

    with execution_lock(LOCK_PATH):
        validate_approval(APPROVAL_PATH)

        journal = bind_journal(
            EXECUTION_DIRECTORY,
            manifest_path,
            APPROVAL_PATH
        )

        state = reconcile_journal(journal, names)

        validate_resume_state(
            bucket, objects, decoder, state
        )

        completed = set(state["completed"])
        results = []

        for item in objects:
            name = item["object"]

            if name in completed:
                results.append({
                    "object": name,
                    "status": "PREVIOUSLY_VERIFIED",
                    "payload_verified": True
                })
                continue

            try:
                result = executor.remediate(
                    bucket,
                    item,
                    decoder,
                    apply=True,
                    allow_conflict=True,
                    record=lambda event: durable_event(
                        journal, event
                    )
                )

                require(
                    result["status"] == "UPDATED_VERIFIED",
                    "Unexpected live execution outcome"
                )

                require(
                    result["payload_verified"] is True,
                    "Payload verification failed"
                )

                durable_event(journal, result)
                results.append(result)

            except Exception as exc:
                durable_event(journal, {
                    "object": name,
                    "status": "QUARANTINED_REVIEW_REQUIRED",
                    "error_type": type(exc).__name__
                })
                raise

        final = reconcile_journal(journal, names)
        require_reconciled(final)

        require(
            set(final["completed"]) == names,
            "Incomplete verified execution"
        )

        validate_resume_state(
            bucket, objects, decoder, final
        )

        return results
