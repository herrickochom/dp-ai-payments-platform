# Gate 3.5D — offline lifecycle dry-run simulation

The demonstration in `services/shared/lifecycle_planner_demo.py` constructs
exactly 12 fictional records and calls the existing
`services/shared/lifecycle_planner.py` for every decision. It does not duplicate
lifecycle decision logic. All identifiers, asset types and sensitivity labels
are marked SYNTHETIC. Ages and the illustrative 90-day retention period are
synthetic inputs, not approved retention policy for any real asset.

| Scenario | Expected decision |
| --- | --- |
| Protected baseline | BLOCKED_PROTECTED_BASELINE |
| Active legal hold | BLOCKED_LEGAL_HOLD |
| Unknown legal hold | BLOCKED_LEGAL_HOLD |
| Recovery reference | BLOCKED_RECOVERY_REFERENCE |
| Recovery backup | BLOCKED_RECOVERY_BACKUP |
| Unresolved dependencies | BLOCKED_DEPENDENCIES |
| Conflicting retention policies | POLICY_CONFLICT |
| Unresolved policy/clock | POLICY_UNRESOLVED |
| Below approved synthetic retention | RETAIN |
| Elapsed retention, incomplete approval | BLOCKED_APPROVAL |
| Fully approved elapsed synthetic retention | ELIGIBLE_FOR_DISPOSAL |
| Explicitly archive-eligible synthetic asset | ELIGIBLE_FOR_ARCHIVE |

The CLI prints scenario, asset type, age, retention, expected decision, actual
decision and PASS/FAIL. A mismatch returns exit status 1; all matches return 0.
The final counters report total scenarios, passes and failures, followed by
`DESTRUCTIVE_ACTIONS=0` and `LIVE_INFRASTRUCTURE_CONNECTIONS=0`.

Eligibility is dry-run consideration only. ELIGIBLE_FOR_DISPOSAL is not
execution authority; ELIGIBLE_FOR_ARCHIVE does not perform archival. There is
no lifecycle executor, deletion, archive execution, object movement, snapshot
expiry, orphan removal or garbage collection. The demo has no infrastructure
clients, network access, subprocess execution, Docker/dbt invocation, database
connection or ML execution. It constructs metadata in memory and prints to
stdout. Frozen Gate 2 evidence and real recovery references/backups are not
read or changed.

## Validation

```sh
.venv/bin/python -m pytest tests/test_lifecycle_planner.py tests/test_lifecycle_planner_demo.py -q
.venv/bin/python services/shared/lifecycle_planner_demo.py
.venv/bin/python -m pytest tests/test_data_protection.py -q
git diff --check
```

Demo tests check the exact 12 expected decisions, synthetic metadata, delegation
to the existing planner, successful and failing return values, offline execution
with prohibited IO patched to fail, and a source-level import/call allowlist
that prevents adding client or executor capabilities unnoticed.

## Limitations

Synthetic assertions do not verify real asset age, policy approval, legal holds,
dependencies, recovery protection, retention enforcement or restore capability.
The demonstration does not discover assets or connect to infrastructure. It
provides no approval workflow or execution mechanism. Existing planner tests
cover additional precedence and invalid-input cases; these 12 examples are
illustrative coverage, not production lifecycle acceptance or permission to
run maintenance.
