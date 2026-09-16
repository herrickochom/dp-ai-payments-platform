# 03 - Tokenisation Tests

## Canonical token contract

Canonical input:

`beneficiary_id`

Algorithm:

HMAC-SHA256

Domain separation:

`gate2-beneficiary||` + beneficiary_id

Runtime configuration:

- `DP_TOKEN_KEY`
- `DP_TOKEN_KEY_VERSION`

Output form:

`<version>:<64 lowercase hexadecimal characters>`

Properties:

- deterministic for a given key/version and beneficiary;
- keyed rather than plain SHA256;
- domain separated;
- versioned for rotation;
- fail closed when required configuration is absent;
- secret material is not persisted or logged.

## Legacy token treatment

Legacy Bronze values of the form:

`TOKEN-<16hex>`

are truncated, unkeyed SHA256-derived provenance values and are not the
canonical Gate 2 beneficiary token.

They may be retained only as restricted provenance, for example:

`legacy_source_beneficiary_token`

They must not be promoted as the canonical analytical token.

## Tokeniser tests

Final observed result:

`tests/test_tokeniser.py`: 9 PASS / 0 FAIL

## Token-link materialisation

The bounded materialisation implementation is:

`services/shared/materialise_token_link.py`

The restricted token-link schema contains:

- beneficiary_id
- beneficiary_key_internal
- token_version
- beneficiary_token
- is_active
- generated_at

Final authorised materialisation:

- rows: 250
- unique canonical beneficiary IDs: 250
- unique canonical tokens: 250
- active rows: 250
- null canonical token fields: 0
- return code: 0

Independent Trino aggregate validation also passed.

The materialisation job must not be rerun merely for evidence generation.
