# Secret provider contract

`services/shared/security/secret_provider.py` is the single provider-neutral entry point for reading secret values. It answers one question: given a secret name, what is its value, or is it absent. `tests/security/test_secret_provider_contract.py` proves the contract offline.

This is the **C10.3D.1 core contract**. No caller has been migrated to it yet; existing read sites keep their current behaviour until C10.3D.2 moves them in bounded groups. Nothing in this contract changes Compose wiring, PostgreSQL versions, CDC, Debezium, Kafka activation, Schema Registry configuration, TLS, object-store IAM, or C8/C9 semantics.

## Resolution

| Situation | Behaviour |
| --- | --- |
| Local / non-production, nothing declared | Environment variables only. Existing local development is unchanged. |
| `DP_SECRET_DIR` explicitly configured | The mounted file `$DP_SECRET_DIR/<NAME>` is tried first; the environment is consulted only when the file cannot supply a value. |
| `DP_SECRET_SOURCE` undeclared in production | Fails closed. See below. |

Only trailing line-ending characters (`\n`, `\r\n`, `\r`) are removed from file contents. Surrounding whitespace is part of the value. Values are never cached: every resolution reads afresh, and no global secret store exists.

## Source selection

`DP_SECRET_SOURCE` declares how a deployment supplies secrets. Allowed values are `environment` and `mounted-files`.

- **Local / non-production:** the source defaults to `environment`, so nothing must be declared.
- **`DP_SECURITY_MODE=production`:** `DP_SECRET_SOURCE` must be declared explicitly. An undeclared source fails closed rather than being silently assumed.
- **`environment` in production:** permitted for now as a deployment mechanism, and explicitly declared rather than assumed. This is **not** claimed to be the final production architecture; the seam exists so later deployment hardening can move to mounted files without changing callers.
- **`mounted-files`:** `DP_SECRET_DIR` must also be set, absolute, an existing directory, and readable.
- **Any other value:** fails closed.

`SECRET_MANAGER` and `KMS_KEY_ID` remain governance seams in `platform/config/governance/gate2_production_seams.yaml` and are deliberately **not** required by this contract: a deployment may legitimately supply secrets as mounted files without any application-level secret-manager client.

## Name safety

Secret names must match `[A-Z][A-Z0-9_]*`. This rejects path traversal (`../`), absolute paths, path separators, relative segments, dots, dashes, spaces, lowercase and any provider-specific path expression. File resolution is additionally verified to stay inside the real `DP_SECRET_DIR`, so a name that resolves outside it — including through a symbolic link — is rejected. `resolve_secret()` cannot be used for arbitrary filesystem reads.

## Failure behaviour

| Condition | Result |
| --- | --- |
| Name is not in the allowed form | `SecretNameError`. The supplied text is never echoed, because an invalid name has not been proved to be a name. |
| Source undeclared in production, unsupported, or an invalid secret directory | `SecretConfigurationError`. |
| `require_secret()` finds the secret absent or empty | `SecretUnavailable`. The message names the secret and never its value. |
| A configured secret file is unreadable | `SecretConfigurationError`, rather than silently falling back. |

An empty value counts as unavailable. No exception, `repr`, `str` or `args` carries a secret value, and the module has no output path: it never logs, never prints, and imports no cloud SDK, secret-manager client, or container-runtime dependency.

## Split authorities

The contract is name-based only and preserves every existing credential boundary: the eight object-store identities, the Nessie transform/publication/read tokens, the PostgreSQL runtime and migration identities, Kafka and Schema Registry credentials, and UI credentials. `tests/security/test_secret_provider_contract.py` pins the transform authority sets and the object-store identity mapping so this contract cannot widen them.

## Deferred

Caller migration (C10.3D.2); transport/TLS material and certificate mounts; cloud secret-manager or external secret-manager implementations; key management and rotation; Compose `secrets:` adoption. None of these are decided here.
