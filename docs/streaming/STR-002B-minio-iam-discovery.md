# STR-002B MinIO IAM discovery

The repository owns local MinIO IAM through a Python desired-state reconciler baked into the one-shot `minio-init-iam` image. It validates eight separately stored custom policies, creates missing users from environment-named credentials, and attaches one policy per identity; no existing policy covers `flink/`, and no lifecycle, versioning, or quota declaration for that prefix was found. Production service identity and IAM are planned Terraform concerns, while local Compose remains the topology authority (`infra/terraform/README.md:3-16`; `infra/terraform/contracts/local_ownership.json:5-31`). The dedicated Flink MinIO identity can be provisioned by the repository-owned `minio-init-iam` reconciler following the pattern of `raw_ingest` and its custom `raw-ingest` policy.

## T1 — Provisioning-script location

- The canonical provisioner is `platform/minio/provision_object_store_identities.py`; it constructs `mc admin policy create`, `mc admin user ls/add`, and `mc admin policy attach` argument lists (`platform/minio/provision_object_store_identities.py:142-158,197-221`).
- The shell wrapper delegates directly to that Python file (`platform/minio/provision-object-store-identities.sh:1-5`).
- The Dockerfile copies the Python script, policies, and shared secret provider from the repository; nothing is downloaded at build time (`platform/docker/dockerfiles/Dockerfile.minio-init-iam:9-22`).
- Searches covered `platform/`, `scripts/`, the Dockerfile inputs, and repository-wide admin-command references. Other matches are tests or documentation, not another applying provisioner (`tests/security/test_object_store_provisioning.py:228-229`; `tests/security/test_remaining_secret_callers.py:347`).

**Confidence: High.** The complete image entrypoint and all repository admin-command construction paths were traced.

## T2 — Canonical provisioning script

No literal credential is embedded; secret values are resolved by variable name through the shared provider (`platform/minio/provision_object_store_identities.py:261-274`).

```python
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
    """Parse ``mc admin user list --json`` output.

    The tabular listing truncates long access keys with an ellipsis, which
    makes the tabular form unusable for exact matching against configured
    access keys. The JSON form emits the full access key and the attached
    policy name for each user. The parser therefore requires JSON output
    and refuses any line that does not decode to a record with the fields
    it depends on.
    """
    users: dict[str, set[str]] = {}
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except ValueError as exc:
            raise ProvisionError("cannot safely parse existing MinIO users") from exc
        if not isinstance(record, dict):
            raise ProvisionError("cannot safely parse existing MinIO users")
        access_key = record.get("accessKey")
        status = record.get("userStatus", record.get("status"))
        policy = record.get("policyName", record.get("policy", ""))
        if not isinstance(access_key, str) or not access_key:
            raise ProvisionError("cannot safely parse existing MinIO users")
        if status not in {"enabled", "disabled"}:
            raise ProvisionError("cannot safely parse existing MinIO users")
        if not isinstance(policy, str):
            raise ProvisionError("cannot safely parse existing MinIO users")
        if access_key in users:
            raise ProvisionError("duplicate MinIO user in administrative listing")
        users[access_key] = {policy} if policy else set()
    return users


def apply(source: dict[str, str]) -> None:
    require_apply_environment(source)
    environment = admin_environment(source)
    # Inspect all associations before mutation. User secrets cannot be read
    # back, so an existing desired key is preserved without credential claims.
    existing = existing_users(mc(["admin", "user", "ls", "--json", ALIAS], environment))
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


def rotate(source: dict[str, str], identity: str) -> None:
    """Re-key exactly one approved identity, in place.

    ``apply`` deliberately never rewrites an existing user's secret: MinIO
    cannot read a stored secret back, so reconciliation cannot tell a matching
    secret from a stale one. That is correct for a convergence loop, but it
    means a credential that has leaked through, for example, version control
    cannot be revoked through ``--apply`` at all.

    This mode is the deliberate exception. It is limited to the eight approved
    identities, requires the user to already exist, and uses the same
    ``mc admin user add`` call that ``apply`` uses to create a user, which
    replaces the stored secret. The access key, the policy attachment and every
    other identity are left untouched, so the blast radius of a rotation is a
    single principal.
    """
    require_apply_environment(source)
    rows = {row[0]: row for row in IDENTITIES}
    if identity not in rows:
        raise ProvisionError("unknown identity; rotation is limited to the eight approved identities")
    _, policy, access_name, secret_name = rows[identity]

    environment = admin_environment(source)
    access_key = source[access_name]
    existing = existing_users(mc(["admin", "user", "ls", "--json", ALIAS], environment))
    if access_key not in existing:
        raise ProvisionError("refusing to rotate an identity that does not already exist")
    if policy not in existing[access_key]:
        raise ProvisionError("refusing to rotate an identity whose policy is not attached")

    # Re-add with the current secret. Built only at the point of use and
    # never echoed: the command line is not logged or printed.
    mc(["admin", "user", "add", ALIAS, access_key, source[secret_name]], environment)
    print(f"Rotated secret for identity '{identity}'; access key, policy and all other identities unchanged")


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
        print("--rotate <identity>  LIVE re-key of one approved identity, for a credential that has been exposed")
        return 0
    if len(argv) == 2 and argv[0] == "--rotate":
        rotate_argv = argv
    elif argv in (["--validate"], ["--dry-run"], ["--apply"]):
        rotate_argv = None
    else:
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
        elif rotate_argv is not None:
            rotate(source, rotate_argv[1])
        else:
            apply(source)
        return 0
    except ProvisionError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
```

Source: `platform/minio/provision_object_store_identities.py:1-312`.

**Confidence: High.** The fenced block is the full repository file, including comments and blank lines.

## T3 — IAM Dockerfile

```dockerfile
ARG MINIO_MC_IMAGE=reg.mini.dev/minio-client@sha256:6ffae97b957902e23118625bb84b3d4a32f2d7ee6be2dba5ea4efae2a8452d37

FROM ${MINIO_MC_IMAGE} AS minio_mc

FROM python:3.13-slim

COPY --from=minio_mc /usr/bin/mc /usr/local/bin/mc

WORKDIR /app

COPY platform/minio/provision_object_store_identities.py /app/platform/minio/provision_object_store_identities.py
COPY platform/minio/policies /app/platform/minio/policies
COPY services/shared/security /app/services/shared/security

RUN touch /app/platform/__init__.py \
    && touch /app/platform/minio/__init__.py \
    && touch /app/services/__init__.py \
    && touch /app/services/shared/__init__.py

ENV PYTHONPATH=/app

ENTRYPOINT ["python3", "/app/platform/minio/provision_object_store_identities.py"]
CMD ["--apply"]
```

- The final base is `python:3.13-slim`; `mc` is copied from a digest-pinned MinIO client build stage (`platform/docker/dockerfiles/Dockerfile.minio-init-iam:1-7`).
- The provisioner and policy directory are baked into the image, not mounted at runtime (`platform/docker/dockerfiles/Dockerfile.minio-init-iam:9-13`; `docker-compose.yaml:1576-1613`).
- The entrypoint invokes the Python script and the default command supplies `--apply` (`platform/docker/dockerfiles/Dockerfile.minio-init-iam:20-23`).

**Confidence: High.** The Dockerfile and corresponding Compose service were read in full.

## T4 — Existing identities and policies

| Identity label | Custom policy | Access/secret variables | Scope and actions |
|---|---|---|---|
| `raw_ingest` | `raw-ingest` | `RAW_INGEST_S3_ACCESS_KEY_ID` / `RAW_INGEST_S3_SECRET_ACCESS_KEY` | `raw/v2/*`: Get, Put |
| `cdc_quarantine` | `cdc-quarantine` | `CDC_QUARANTINE_S3_ACCESS_KEY_ID` / `CDC_QUARANTINE_S3_SECRET_ACCESS_KEY` | `restricted/cdc-quarantine/v1/*`: Get, Put |
| `platform_raw_read` | `platform-raw-read` | `PLATFORM_RAW_READ_ACCESS_KEY` / `PLATFORM_RAW_READ_SECRET_KEY` | `raw/v2/*`: Get |
| `nessie_catalog` | `nessie-catalog` | `NESSIE_S3_ACCESS_KEY` / `NESSIE_S3_SECRET_KEY` | `warehouse/*`: Get, Put, Delete; bucket listing is unconditioned for health checks |
| `trino_iceberg` | `trino-iceberg-read` | `TRINO_S3_ACCESS_KEY_ID` / `TRINO_S3_SECRET_ACCESS_KEY` | `warehouse/*`: Get |
| `ordinary_transform` | `ordinary-transform` | `ORDINARY_TRANSFORM_S3_ACCESS_KEY_ID` / `ORDINARY_TRANSFORM_S3_SECRET_ACCESS_KEY` | raw: Get; warehouse: Get, Put, Delete |
| `restricted_identity_transform` | `restricted-transform` | `RESTRICTED_TRANSFORM_S3_ACCESS_KEY_ID` / `RESTRICTED_TRANSFORM_S3_SECRET_ACCESS_KEY` | raw: Get; warehouse: Get, Put, Delete |
| `ml_prediction_transform` | `ml-transform` | `ML_TRANSFORM_S3_ACCESS_KEY_ID` / `ML_TRANSFORM_S3_SECRET_ACCESS_KEY` | raw: Get; warehouse: Get, Put, Delete |

Mappings and approved scopes: `platform/minio/provision_object_store_identities.py:23-51`. Policy documents: `platform/minio/policies/raw-ingest.json:1-34`, `cdc-quarantine.json:1-25`, `platform-raw-read.json:1-33`, `nessie-catalog.json:1-27`, `trino-iceberg-read.json:1-32`, `ordinary-transform.json:1-47`, `restricted-transform.json:1-47`, and `ml-transform.json:1-47`.

- All eight are custom policies; no canned `readwrite`, `readonly`, `writeonly`, or `diagnostics` policy is attached (`platform/minio/provision_object_store_identities.py:31-51,150-159`).
- Apart from Nessie's documented health-check exception, listing is bucket-level with an `s3:prefix` condition and object authority is prefix-scoped with ARNs such as `arn:aws:s3:::dp-ai-payment/raw/v2/*` (`platform/minio/provision_object_store_identities.py:89-114`; `platform/minio/policies/raw-ingest.json:5-31`).

**Confidence: High.** The validated mapping and every companion JSON policy were inspected.

## T5 — Provisioner conventions

- Identity labels use `snake_case`; policy names use lower-case hyphenated names; access keys themselves come from named environment variables (`platform/minio/provision_object_store_identities.py:31-41,213-221`).
- Policies are separate JSON files with version `2012-10-17`, Allow statements, scoped `ListBucket`, and object actions on one exact ARN (`platform/minio/provision_object_store_identities.py:63-114`).
- The provisioner runs `policy create`, preserves an existing user, otherwise runs `user add`, then attaches any missing intended policy (`platform/minio/provision_object_store_identities.py:150-159,197-222`).
- Re-runs are reconciliation-oriented: current users are inspected first, existing secrets are deliberately not rewritten, policy association drift fails closed, and all policy-create commands are repeated (`platform/minio/provision_object_store_identities.py:197-222`). The tool does not use `--force` or suppress command failures (`platform/minio/provision_object_store_identities.py:142-147`).
- Required runtime names comprise MinIO endpoint/root credentials plus every access/secret pair in `IDENTITIES` (`platform/minio/provision_object_store_identities.py:117-130`).
- The exact object form is `arn:aws:s3:::dp-ai-payment/<prefix>/*`; the bucket ARN is `arn:aws:s3:::dp-ai-payment` (`platform/minio/provision_object_store_identities.py:23-26`).

**Confidence: High.** These conventions are enforced by validation code rather than inferred only from examples.

## T6 — Invocation

- The Compose service is `docker-compose.yaml:1576-1613`. It passes root and all eight service credential variable names, depends on healthy MinIO plus completed bucket and PostgreSQL health initialisers, joins `data-platform-network`, has `restart: 'no'`, and has no profile (`docker-compose.yaml:1576-1613`).
- Compose supplies no command override, so the image runs its default `--apply` (`platform/docker/dockerfiles/Dockerfile.minio-init-iam:22-23`; `docker-compose.yaml:1576-1613`).
- A maintainer can deliberately re-run the one-shot service with `docker compose run --rm minio-init-iam`; this command is reported only and was not executed.
- It is designed as a guarded reconciliation loop, although existing user secrets cannot be verified and are not rotated by `--apply` (`platform/minio/provision_object_store_identities.py:197-222,225-258`).

**Confidence: High.** Invocation follows directly from Compose plus the image entrypoint/CMD.

## T7 — Repository state for `flink/`

- No repository declaration sets a MinIO lifecycle, versioning, or quota rule for `flink/`. The existing policy inventory covers only `raw/v2`, CDC quarantine, and `warehouse` (`platform/minio/provision_object_store_identities.py:23-51`).
- Outside the two Compose service blocks, the same checkpoint/savepoint prefixes occur in the source configuration at `platform/flink/conf/flink-conf.yaml:72-73`; Compose references them at `docker-compose.yaml:1627-1628,1675-1676`.
- No attempt was made to list or create the runtime prefix.

**Confidence: High.** Exact-prefix and MinIO lifecycle/versioning/quota searches were performed repository-wide; runtime existence is deliberately outside scope.

## T8 — What is not in the repository

- Local IAM is not external: Compose is the local topology authority and the repository-owned service applies it (`infra/terraform/contracts/local_ownership.json:23-31`; `docker-compose.yaml:1576-1613`).
- Production Terraform is declared as the control plane and its planned scope includes service identities, IAM, storage, secrets and retention, but the checked-in Terraform currently contains no MinIO IAM resource (`infra/terraform/README.md:3-16,36-37`).
- The local target is explicitly a non-production reference integration environment (`infra/terraform/contracts/local_ownership.json:5-9,42-45`). No implemented production MinIO identity naming convention was found, so equivalence with local names is unknown.

**Confidence: Medium.** Repository ownership is clear, but an external production module or operator process cannot be disproved from repository contents alone.

## Conventions to follow

A future `flinkchkpt` identity must fit the enforced local pattern: a `snake_case` logical label mapped to a lower-case hyphenated custom-policy name and an access/secret environment-variable pair; a separate policy JSON using version `2012-10-17`; bucket listing constrained with `StringLike` / `s3:prefix`; object permissions bound to `arn:aws:s3:::dp-ai-payment/flink/*`; and user creation followed by policy attachment through the reconciliation loop (`platform/minio/provision_object_store_identities.py:31-51,63-114,150-159,197-222`). This describes verified conventions, not an implementation specification.

## Where the change will be made

The central later edit point is `platform/minio/provision_object_store_identities.py:31-51`, which contains the complete identity-to-policy-to-variable mapping and approved scope contract. A matching custom policy would reside alongside `platform/minio/policies/raw-ingest.json:1-34`, and its variable names would be passed in the `minio-init-iam` environment block at `docker-compose.yaml:1583-1603`. No change was made.

## Known unknowns

- Whether production MinIO IAM is supplied by an external Terraform module or operator workflow not present in this repository.
- The production identity and policy naming convention.
- Runtime policy/user state and whether any unmanaged `flink/` grant already exists.
- Runtime lifecycle, versioning, and quota state not represented in repository configuration.

## Non-goals

This discovery did not mutate or query MinIO runtime state, run Compose services, inspect or reproduce secret values, design the new grant, edit IAM code or policies, or investigate an external production Terraform implementation.

