# 08 - RBAC Access Control

## Trino access-control objective

Ordinary service and BI users must not have access to restricted identity
data.

The activated candidate policy contains no wildcard users.

Roles/users validated:

- `hochom`: owner/full authorised Iceberg access
- `trino`: read-only Consumption
- `metabase`: read-only Consumption
- `agent-api`: SELECT only on the explicitly authorised executive
  Consumption surface

Ordinary users have no access to:

- Staging
- Bronze
- Silver
- Silver Vault
- Gold
- restricted identity service

except where explicitly authorised for the owner/admin identity.

## Fresh access checks

Observed:

- hochom -> executive overview: ALLOW
- hochom -> restricted token link: ALLOW, 250 rows
- trino -> AI risk Consumption: ALLOW, 151 rows
- trino -> restricted token link: DENIED
- metabase -> executive overview: ALLOW
- metabase -> restricted token link: DENIED
- agent-api -> executive overview: ALLOW
- agent-api -> AI risk: DENIED
- agent-api -> restricted token link: DENIED

Result:

PASS - least privilege demonstrated for the tested POC identities.

No passwords, password hashes, beneficiary identifiers or canonical tokens
are included in this evidence file.

## POC limitation

Local Trino permits the development configuration required by the POC.
This must not be described as equivalent to production transport security,
enterprise IAM or KMS controls.
