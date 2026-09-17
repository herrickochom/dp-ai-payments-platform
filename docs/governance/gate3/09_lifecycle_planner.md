# Gate 3.5C lifecycle eligibility planner

Date: 2026-09-17. Status: IMPLEMENTED_AND_TESTED for the offline decision contract below. This is not complete Gate 3 acceptance or authorisation to mutate data.

## Purpose and scope

`services/shared/lifecycle_planner.py` provides `plan_lifecycle(LifecycleAsset) -> LifecycleDecision`. It evaluates one explicitly supplied metadata record, deterministically and without infrastructure connections, filesystem IO, clock reads or external dependencies. The input is a frozen dataclass; the function returns an enum and does not modify its input. No CLI, scanner, scheduler or batch executor is implemented.

ELIGIBLE_FOR_DISPOSAL means only that supplied metadata passed dry-run consideration checks. It is not deletion authority. Deletion remains disabled in the existing governance declaration. ELIGIBLE_FOR_ARCHIVE does not perform or authorise archival. RETAIN means the approved retention period has not elapsed, not permission for indefinite retention.

## Typed input contract

| Fields | Interpretation |
|---|---|
| asset_id, asset_type, sensitivity | Nonempty metadata strings; the planner does not infer a retention schedule from sensitivity or asset type. |
| age_days, retention_days | Supplied whole nonnegative days from a known clock and approved asset schedule; None, negative, boolean, fractional, string or other invalid values mean POLICY_UNRESOLVED. Age equal to retention is elapsed. Zero is accepted only as an explicitly resolved supplied period; no zero-day policy is inferred. |
| policy_resolved, policy_conflict | Policy/scope/precedence approval assertions. Conflict wins over unresolved policy. Both require explicit known states before eligibility. |
| hold_status | HoldStatus.CLEAR, ACTIVE or UNKNOWN. None, unknown and unsupported values block. Raw strings must be converted/validated by any future caller; they are not treated as approved clear hold status. |
| owner_approved, privacy_legal_approved, recovery_approved | All must be exactly True for elapsed archive/disposal consideration; false/unknown/truthy strings or integers do not suffice. |
| dependencies_resolved | Must be exactly True; unknown or unresolved blocks. Caller must cover downstream copies, retained-reference reachability and recovery dependencies. |
| protected_baseline, recovery_reference, recovery_backup | Must each be explicitly False before eligibility. True or unknown blocks under the corresponding protection decision. |
| archive_requested, archive_approved | Archive is explicitly selected by archive_requested=True and requires archive_approved=True plus all three ordinary approvals. It uses the same elapsed approved retention threshold; a separate earlier archive clock is not implemented. No approval falls back to disposal. Unknown archive intent blocks elapsed consideration. |

Unknown defaults are conservative: minimally populated assets block at protected-baseline status. Boolean identity checks prevent truthy non-booleans from authorising eligibility. Asset identity/classification is validated at the unresolved-policy stage after higher-priority safety blockers.

## Exact decision vocabulary and precedence

| Order | Condition | Decision |
|---|---|---|
| 1 | Protected baseline or unknown protection | BLOCKED_PROTECTED_BASELINE |
| 2 | Active/unknown/invalid hold | BLOCKED_LEGAL_HOLD |
| 3 | Recovery reference or unknown reference dependency | BLOCKED_RECOVERY_REFERENCE |
| 4 | Recovery backup or unknown backup dependency | BLOCKED_RECOVERY_BACKUP |
| 5 | Unresolved/unknown dependencies | BLOCKED_DEPENDENCIES |
| 6 | Explicit policy conflict | POLICY_CONFLICT |
| 7 | Unresolved/unknown policy, conflict status, clock/period or identity/classification | POLICY_UNRESOLVED |
| 8 | Age below approved retention | RETAIN |
| 9 | Elapsed period with any missing/unknown required approval | BLOCKED_APPROVAL |
| 10 | Explicit archive request with archive approval | ELIGIBLE_FOR_ARCHIVE |
| 10 | Explicit archive request without archive approval or unknown archive intent | BLOCKED_APPROVAL |
| 11 | Elapsed period, all approvals and explicit non-archive intent | ELIGIBLE_FOR_DISPOSAL |

The first matching condition wins; a single decision does not claim other blockers absent. All eleven supported decision values appear above. Approval checks follow below-retention RETAIN, so missing disposal approval does not prevent continued retention within the approved period.

## Policy conflict and legal hold handling

Neither `platform/config/minio/minio-security-config.yaml`'s 365-day maximum nor sensitivity-based periods in `platform/config/governance/governance_policies.yaml` are read or selected by the module. Those declarations have unresolved scope/precedence and are not proven regulatory requirements. A caller must mark an applicable conflict with policy_conflict=True, or unresolved scope/clock with policy_resolved=False/None. A changed numeric retention_days cannot bypass those flags. Do not invent legal/regulatory periods or claim supplied approval booleans establish regulatory compliance.

Hold must be explicitly CLEAR. ACTIVE/UNKNOWN/None block, including elapsed periods with all approvals. Protected baseline remains the higher-priority blocker, consistent with the governance contract; it never becomes eligible simply through ageing.

## Recovery protection

Caller metadata must identify Nessie recovery references, physical backups, protected prefix subtrees and every snapshot/file dependency. Tests cover the current Gate 3 tag `gate3-recovery-20260916T200450Z`, backup concept `dp-ai-payment-gate3-backup/gate3-current-state/20260916T201500Z/`, and frozen baselines using explicit protection flags. Their names are test fixtures, not infrastructure queries or independently verified identities. The same protections apply to historical Gate 2 exclusions irrespective of current availability.

The planner does not maintain a trusted protected-reference registry or infer protection from a name/path. A caller incorrectly asserting False can misclassify an asset; upstream evidence and approved inventory remain mandatory. Unknown protection/recovery defaults fail closed. See [protected baseline](01_protected_baseline.md), [asset matrix](02_asset_retention_matrix.md), [hold policy](03_legal_hold_and_disposal_policy.md), [replay policy](04_replay_and_recovery_policy.md) and the current-state record in [document 08](08_current_recovery_baseline_and_logging_activation.md).

## Verification and limitations

Only these two requested test selections were run, using the existing `.venv` interpreter via PATH and disabling bytecode/pytest cache writes:

- `python3 -m pytest tests/test_lifecycle_planner.py -q`: **69 passed**.
- `python3 -m pytest tests/test_data_protection.py -q`: **13 passed**.

Tests are offline and synthetic. They cover required decisions, exact threshold, unknown/invalid clocks and approvals, recovery concepts, combined-blocker precedence, archive approval and unresolved/conflicting 365-day declarations. The no-side-effects case guards filesystem writes/deletion/moves, sockets and process execution while repeatedly evaluating eligibility; module import inspection constrains dependencies to dataclasses/enum. No destructive operation is invoked by testing.

The module trusts supplied evidence. It does not validate signatures/approver identities, hold registry freshness, policy/inventory versions, live reachability, copy lineage, disposal suppression or stale-plan changes. It produces neither a complete persisted candidate manifest nor execution receipts. Reference-aware maintenance fixtures, restore demonstration and live inventory integrations remain separate acceptance work. These limitations prevent treating all G3-11/G3-12 requirements as complete.

**No lifecycle executor exists in this implementation. No deletion, archival, expiry, file movement or garbage collection was performed.** No dbt, producer, Kafka replay/reset/configuration, MinIO lifecycle change, Nessie mutation, recovery-tag/backup modification, ML/token job or Docker Compose operation was performed during Gate 3.5C. Consumer/privacy/token/transformation contracts remain unchanged. No commit was made.
