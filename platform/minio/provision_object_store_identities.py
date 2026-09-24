"""Explicit MinIO IAM provisioning; validate and dry-run never invoke mc."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit

# This tool also runs as a standalone administrative script from any working
# directory, so the repository root is resolved from the module file itself.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from services.shared.security.secret_provider import resolve_secret  # noqa: E402


POLICY_DIR = Path(__file__).resolve().parent / "policies"
ALIAS = "c10_object_store_provision"
BUCKET = "arn:aws:s3:::dp-ai-payment"
RAW = f"{BUCKET}/raw/v2/*"
QUARANTINE = f"{BUCKET}/restricted/cdc-quarantine/v1/*"
WAREHOUSE = f"{BUCKET}/warehouse/*"
READ = frozenset({"s3:GetObject"})
WRITE = frozenset({"s3:GetObject", "s3:PutObject"})
ICEBERG = frozenset({"s3:GetObject", "s3:PutObject", "s3:DeleteObject"})

# Identity label, named policy, access-key variable, secret-key variable.
IDENTITIES = (
    ("raw_ingest", "raw-ingest", "RAW_INGEST_S3_ACCESS_KEY_ID", "RAW_INGEST_S3_SECRET_ACCESS_KEY"),
    ("cdc_quarantine", "cdc-quarantine", "CDC_QUARANTINE_S3_ACCESS_KEY_ID", "CDC_QUARANTINE_S3_SECRET_ACCESS_KEY"),
    ("platform_raw_read", "platform-raw-read", "PLATFORM_RAW_READ_ACCESS_KEY", "PLATFORM_RAW_READ_SECRET_KEY"),
    ("nessie_catalog", "nessie-catalog", "NESSIE_S3_ACCESS_KEY", "NESSIE_S3_SECRET_KEY"),
    ("trino_iceberg", "trino-iceberg-read", "TRINO_S3_ACCESS_KEY_ID", "TRINO_S3_SECRET_ACCESS_KEY"),
    ("ordinary_transform", "ordinary-transform", "ORDINARY_TRANSFORM_S3_ACCESS_KEY_ID", "ORDINARY_TRANSFORM_S3_SECRET_ACCESS_KEY"),
    ("restricted_identity_transform", "restricted-transform", "RESTRICTED_TRANSFORM_S3_ACCESS_KEY_ID", "RESTRICTED_TRANSFORM_S3_SECRET_ACCESS_KEY"),
    ("ml_prediction_transform", "ml-transform", "ML_TRANSFORM_S3_ACCESS_KEY_ID", "ML_TRANSFORM_S3_SECRET_ACCESS_KEY"),
)
POLICY_SCOPE = {
    "raw-ingest": ({"raw/v2", "raw/v2/*"}, {RAW: WRITE}),
    "cdc-quarantine": ({"restricted/cdc-quarantine/v1", "restricted/cdc-quarantine/v1/*"}, {QUARANTINE: WRITE}),
    "platform-raw-read": ({"raw/v2", "raw/v2/*"}, {RAW: READ}),
    "nessie-catalog": ({"warehouse", "warehouse/*"}, {WAREHOUSE: ICEBERG}),
    "trino-iceberg-read": ({"warehouse", "warehouse/*"}, {WAREHOUSE: READ}),
    "ordinary-transform": ({"raw/v2", "raw/v2/*", "warehouse", "warehouse/*"}, {RAW: READ, WAREHOUSE: ICEBERG}),
    "restricted-transform": ({"raw/v2", "raw/v2/*", "warehouse", "warehouse/*"}, {RAW: READ, WAREHOUSE: ICEBERG}),
    "ml-transform": ({"raw/v2", "raw/v2/*", "warehouse", "warehouse/*"}, {RAW: READ, WAREHOUSE: ICEBERG}),
}


class ProvisionError(Exception):
    pass


def validate_all() -> None:
    names = [row[0] for row in IDENTITIES]
    policies = [row[1] for row in IDENTITIES]
    if len(IDENTITIES) != 8 or len(set(names)) != 8 or set(policies) != set(POLICY_SCOPE):
        raise ProvisionError("identity/policy mapping is incomplete")
    if {path.name for path in POLICY_DIR.glob("*.json")} != {f"{name}.json" for name in policies}:
        raise ProvisionError("policy inventory differs from the eight approved policies")
    for name in policies:
        try:
            document = json.loads((POLICY_DIR / f"{name}.json").read_text())
        except (OSError, ValueError) as exc:
            raise ProvisionError(f"invalid policy file: {name}") from exc
        if document.get("Version") != "2012-10-17" or not isinstance(document.get("Statement"), list):
            raise ProvisionError(f"invalid policy structure: {name}")
        expected_prefixes, expected_objects = POLICY_SCOPE[name]
        prefixes: set[str] = set()
        objects: dict[str, set[str]] = {}
        for statement in document["Statement"]:
            if not isinstance(statement, dict) or statement.get("Effect") != "Allow":
                raise ProvisionError(f"invalid policy statement: {name}")
            if "NotAction" in statement or "NotResource" in statement:
                raise ProvisionError(f"negated policy authority: {name}")
            actions = statement.get("Action")
            resources = statement.get("Resource")
            if not isinstance(actions, list) or not isinstance(resources, list):
                raise ProvisionError(f"invalid policy action/resource: {name}")
            action_set, resource_set = set(actions), set(resources)
            if not action_set or not resource_set or "*" in resource_set or "s3:*" in action_set:
                raise ProvisionError(f"wildcard policy authority: {name}")
            if not action_set <= {"s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject"}:
                raise ProvisionError(f"administrative or unexpected action: {name}")
            if "s3:ListBucket" in action_set:
                if action_set != {"s3:ListBucket"} or resource_set != {BUCKET}:
                    raise ProvisionError(f"unscoped bucket listing: {name}")
                condition = statement.get("Condition")
                if condition is None and name == "nessie-catalog":
                    # Nessie 0.108.4 performs HeadBucket during object-store
                    # readiness. HeadBucket has no s3:prefix, so this identity
                    # requires bucket-level ListBucket on the canonical bucket.
                    # Object authority remains restricted to warehouse/*.
                    prefixes.update(expected_prefixes)
                else:
                    if not isinstance(condition, dict) or set(condition) != {"StringLike"}:
                        raise ProvisionError(f"unscoped bucket listing: {name}")
                    like = condition["StringLike"]
                    if not isinstance(like, dict) or set(like) != {"s3:prefix"} or not isinstance(like["s3:prefix"], list):
                        raise ProvisionError(f"unscoped bucket listing: {name}")
                    prefixes.update(like["s3:prefix"])
            else:
                if "Condition" in statement or len(resource_set) != 1:
                    raise ProvisionError(f"unexpected object scope: {name}")
                resource = next(iter(resource_set))
                if resource not in expected_objects:
                    raise ProvisionError(f"unexpected object resource: {name}")
                objects.setdefault(resource, set()).update(action_set)
        if prefixes != expected_prefixes or objects != expected_objects:
            raise ProvisionError(f"policy scope differs from approved contract: {name}")


def require_apply_environment(source: dict[str, str]) -> None:
    required = {"MINIO_ENDPOINT", "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD"}
    required.update(variable for row in IDENTITIES for variable in row[2:])
    missing = sorted(name for name in required if not source.get(name, "").strip())
    if missing:
        raise ProvisionError("missing required environment variables: " + ", ".join(missing))
    keys = [source[row[2]] for row in IDENTITIES]
    if len(set(keys)) != len(keys) or source["MINIO_ROOT_USER"] in keys:
        raise ProvisionError("service access keys must be distinct from each other and from root")
    if source["MINIO_ROOT_PASSWORD"] in [source[row[3]] for row in IDENTITIES]:
        raise ProvisionError("service secrets must differ from root")
    parsed = urlsplit(source["MINIO_ENDPOINT"])
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ProvisionError("MINIO_ENDPOINT must be an http(s) URL without credentials")


def admin_environment(source: dict[str, str]) -> dict[str, str]:
    endpoint = urlsplit(source["MINIO_ENDPOINT"])
    credentials = f'{quote(source["MINIO_ROOT_USER"], safe="")}:{quote(source["MINIO_ROOT_PASSWORD"], safe="")}@'
    host = urlunsplit((endpoint.scheme, credentials + endpoint.netloc, endpoint.path, "", ""))
    environment = dict(source)
    environment[f"MC_HOST_{ALIAS}"] = host
    return environment


def mc(arguments: list[str], environment: dict[str, str]) -> str:
    # Never relay mc output: it can contain access keys or command arguments.
    result = subprocess.run(["mc", *arguments], env=environment, text=True, capture_output=True, check=False)
    if result.returncode:
        raise ProvisionError("MinIO administrative command failed; inspect state manually")
    return result.stdout


def policy_commands() -> list[list[str]]:
    return [
        ["admin", "policy", "create", ALIAS, policy, str(POLICY_DIR / f"{policy}.json")]
        for _, policy, _, _ in IDENTITIES
    ]


def attachment_command(policy: str, access_key: str) -> list[str]:
    return ["admin", "policy", "attach", ALIAS, policy, "--user", access_key]


def existing_users(output: str) -> dict[str, set[str]]:
    users = {}
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 2 or parts[0] not in {"enabled", "disabled"}:
            raise ProvisionError("cannot safely parse existing MinIO users")
        if parts[1] in users:
            raise ProvisionError("duplicate MinIO user in administrative listing")
        users[parts[1]] = set(",".join(parts[2:]).split(",")) - {""}
    return users


def apply(source: dict[str, str]) -> None:
    require_apply_environment(source)
    environment = admin_environment(source)
    # Inspect all associations before mutation. User secrets cannot be read
    # back, so an existing desired key is preserved without credential claims.
    existing = existing_users(mc(["admin", "user", "ls", ALIAS], environment))
    intended_policy_by_key = {source[access]: policy for _, policy, access, _ in IDENTITIES}
    managed_policies = set(POLICY_SCOPE)
    for access_key, policies in existing.items():
        assigned_managed = managed_policies.intersection(policies)
        if assigned_managed - {intended_policy_by_key.get(access_key)}:
            raise ProvisionError("unexpected managed policy assignment; manual validation required")

    for command in policy_commands():
        mc(command, environment)
    preserved = 0
    for _, policy, access_name, secret_name in IDENTITIES:
        access_key = source[access_name]
        if access_key in existing:
            preserved += 1
        else:
            # Build secret-bearing arguments only at the point of use.
            mc(["admin", "user", "add", ALIAS, access_key, source[secret_name]], environment)
        if policy not in existing.get(access_key, set()):
            mc(attachment_command(policy, access_key), environment)
    print(f"Reconciled eight service identities; {preserved} existing user secret(s) were not verified or rotated")


def administrative_source() -> dict[str, str]:
    """Process environment with secret material resolved via the provider.

    Access-key identifiers are identity/configuration and remain ordinary
    environment reads. Secret-key material (root and service) resolves
    through the shared secret-provider contract: environment-backed locally,
    mounted-file-backed when DP_SECRET_DIR is configured.
    """
    source = dict(os.environ)
    for name in ("MINIO_ROOT_PASSWORD", *(row[3] for row in IDENTITIES)):
        value = resolve_secret(name)
        if value is not None:
            source[name] = value
    return source


def main(argv: list[str] | None = None, source: dict[str, str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    source = administrative_source() if source is None else source
    if argv == ["--help"]:
        print("--validate  Local policy and mapping validation; no MinIO contact")
        print("--dry-run   Local desired-state plan; no MinIO mutation")
        print("--apply     LIVE MinIO IAM administration; invoke deliberately with administrative credentials")
        return 0
    if argv not in (["--validate"], ["--dry-run"], ["--apply"]):
        print("Use --help; live provisioning requires explicit --apply", file=sys.stderr)
        return 2
    try:
        validate_all()
        if argv == ["--validate"]:
            print("VALID: eight scoped policies and identity mappings")
        elif argv == ["--dry-run"]:
            print("PLAN (offline; no mutation):")
            for identity, policy, _, _ in IDENTITIES:
                print(f"  {identity} -> {policy} ({POLICY_DIR / (policy + '.json')}): ensure/update policy; ensure user; attach policy")
        else:
            apply(source)
        return 0
    except ProvisionError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
