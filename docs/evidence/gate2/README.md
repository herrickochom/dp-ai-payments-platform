# Gate 2 Data Protection and Governance Evidence

Status: FINAL ACCEPTANCE PREPARATION
Evidence date: 2026-09-16

This directory records the implementation, validation, incidents,
remediation and final acceptance evidence for Gate 2.

The evidence deliberately records failed and superseded checks as well as
successful retests. Historical observations are not rewritten to match the
current regenerated data state.

## Final baseline before freeze

- Git branch: `main`
- Git base before final freeze commit:
  `91ade6c820d2c645469d35ce755ae2147957097f`
- Nessie branch: `main`
- Nessie current hash:
  `ddd3b9b6bfd9db5b00ac6304d74124b249176ec0ddab1464adba8b8e69ee96ba`
- Pre-Gate2 Nessie recovery hash:
  `f85893b23c441db5abd0d2400da134cbb5fc4d0cb52a1e3c24e3ae02d85b35df`
- Pre-Gate2 recovery tag: `pre-gate2-20260915T155541Z`

## Evidence index

1. Scope and architecture
2. Data protection tests
3. Tokenisation tests
4. Transformation builds
5. Bronze lineage audit
6. Phase 6 event correlation
7. AI default-risk validation
8. RBAC access control
9. Geography validation
10. Incidents and recovery
11. Final Gate 2 acceptance

## Evidence principle

A PASS means the stated control was demonstrated for the stated scope.
Historical results remain historical. Current regenerated data is not
manipulated to recreate an earlier row count, risk distribution or event
matching state.
