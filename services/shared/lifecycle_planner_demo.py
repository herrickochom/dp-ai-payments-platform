"""Offline synthetic eligibility demonstration; no lifecycle executor."""
from dataclasses import dataclass, replace

if __package__:
    from . import lifecycle_planner as planner
else:
    import lifecycle_planner as planner


@dataclass(frozen=True)
class Scenario:
    name: str
    asset: planner.LifecycleAsset
    expected: planner.LifecycleDecision


def build_scenarios() -> tuple[Scenario, ...]:
    """Construct fictional metadata; 90 days is illustrative, not real policy."""
    baseline = planner.LifecycleAsset(
        asset_id='SYNTHETIC_DEMO_BASE', asset_type='SYNTHETIC_ASSET',
        sensitivity='SYNTHETIC_ONLY', age_days=100, retention_days=90,
        policy_resolved=True, policy_conflict=False,
        hold_status=planner.HoldStatus.CLEAR, owner_approved=True,
        privacy_legal_approved=True, recovery_approved=True,
        dependencies_resolved=True, protected_baseline=False,
        recovery_reference=False, recovery_backup=False,
    )
    d = planner.LifecycleDecision
    specifications = (
        ('protected baseline', d.BLOCKED_PROTECTED_BASELINE,
         {'protected_baseline': True}),
        ('active legal hold', d.BLOCKED_LEGAL_HOLD,
         {'hold_status': planner.HoldStatus.ACTIVE}),
        ('unknown legal hold', d.BLOCKED_LEGAL_HOLD,
         {'hold_status': planner.HoldStatus.UNKNOWN}),
        ('recovery reference', d.BLOCKED_RECOVERY_REFERENCE,
         {'asset_type': 'SYNTHETIC_REFERENCE', 'recovery_reference': True}),
        ('recovery backup', d.BLOCKED_RECOVERY_BACKUP,
         {'asset_type': 'SYNTHETIC_BACKUP', 'recovery_backup': True}),
        ('unresolved dependencies', d.BLOCKED_DEPENDENCIES,
         {'dependencies_resolved': False}),
        ('conflicting retention policies', d.POLICY_CONFLICT,
         {'policy_conflict': True}),
        ('unresolved policy/clock', d.POLICY_UNRESOLVED,
         {'policy_resolved': False, 'age_days': None}),
        ('below approved retention', d.RETAIN, {'age_days': 89}),
        ('elapsed retention, incomplete approval', d.BLOCKED_APPROVAL,
         {'owner_approved': None}),
        ('fully approved elapsed retention', d.ELIGIBLE_FOR_DISPOSAL, {}),
        ('explicitly archive-eligible synthetic asset', d.ELIGIBLE_FOR_ARCHIVE,
         {'archive_requested': True, 'archive_approved': True}),
    )
    return tuple(
        Scenario(name, replace(baseline, asset_id=f'SYNTHETIC_DEMO_{index:02d}',
                               **changes), expected)
        for index, (name, expected, changes) in enumerate(specifications, start=1)
    )


def main() -> int:
    scenarios = build_scenarios()
    passed = 0
    print('SCENARIO | ASSET_TYPE | AGE_DAYS | RETENTION_DAYS | EXPECTED | ACTUAL | RESULT')
    for scenario in scenarios:
        actual = planner.plan_lifecycle(scenario.asset)
        matches = actual == scenario.expected
        passed += int(matches)
        print(f'{scenario.name} | {scenario.asset.asset_type} | '
              f'{scenario.asset.age_days} | {scenario.asset.retention_days} | '
              f'{scenario.expected.value} | {actual.value} | '
              f'{"PASS" if matches else "FAIL"}')
    failed = len(scenarios) - passed
    print(f'TOTAL_SCENARIOS={len(scenarios)}')
    print(f'PASSED={passed}')
    print(f'FAILED={failed}')
    print('DESTRUCTIVE_ACTIONS=0')
    print('LIVE_INFRASTRUCTURE_CONNECTIONS=0')
    return 1 if failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
