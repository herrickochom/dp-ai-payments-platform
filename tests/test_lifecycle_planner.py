"""Offline synthetic lifecycle eligibility and fail-closed precedence tests."""
import ast
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pytest

from services.shared.lifecycle_planner import (
    HoldStatus, LifecycleAsset, LifecycleDecision as D, plan_lifecycle,
)


def approved(**overrides):
    asset = LifecycleAsset(
        asset_id='synthetic-asset', asset_type='RAW', sensitivity='RESTRICTED',
        age_days=1095, retention_days=1095, policy_resolved=True,
        policy_conflict=False, hold_status=HoldStatus.CLEAR,
        owner_approved=True, privacy_legal_approved=True, recovery_approved=True,
        dependencies_resolved=True, protected_baseline=False,
        recovery_reference=False, recovery_backup=False,
    )
    return replace(asset, **overrides)


@pytest.mark.parametrize('changes,expected', [
    ({'protected_baseline': True}, D.BLOCKED_PROTECTED_BASELINE),
    ({'protected_baseline': None}, D.BLOCKED_PROTECTED_BASELINE),
    ({'hold_status': HoldStatus.ACTIVE}, D.BLOCKED_LEGAL_HOLD),
    ({'hold_status': HoldStatus.UNKNOWN}, D.BLOCKED_LEGAL_HOLD),
    ({'hold_status': None}, D.BLOCKED_LEGAL_HOLD),
    ({'hold_status': 'CLEAR'}, D.BLOCKED_LEGAL_HOLD),
    ({'recovery_reference': True}, D.BLOCKED_RECOVERY_REFERENCE),
    ({'recovery_reference': None}, D.BLOCKED_RECOVERY_REFERENCE),
    ({'recovery_backup': True}, D.BLOCKED_RECOVERY_BACKUP),
    ({'recovery_backup': None}, D.BLOCKED_RECOVERY_BACKUP),
    ({'dependencies_resolved': False}, D.BLOCKED_DEPENDENCIES),
    ({'dependencies_resolved': None}, D.BLOCKED_DEPENDENCIES),
    ({'policy_conflict': True}, D.POLICY_CONFLICT),
    ({'policy_conflict': None}, D.POLICY_UNRESOLVED),
    ({'policy_resolved': False}, D.POLICY_UNRESOLVED),
    ({'policy_resolved': None}, D.POLICY_UNRESOLVED),
    ({'age_days': 1094, 'owner_approved': None}, D.RETAIN),
    ({'age_days': 1095}, D.ELIGIBLE_FOR_DISPOSAL),
    ({'age_days': 1096}, D.ELIGIBLE_FOR_DISPOSAL),
    ({'archive_requested': True, 'archive_approved': True}, D.ELIGIBLE_FOR_ARCHIVE),
    ({'archive_requested': True, 'archive_approved': None}, D.BLOCKED_APPROVAL),
    ({'archive_requested': True, 'archive_approved': False}, D.BLOCKED_APPROVAL),
    ({'archive_requested': None}, D.BLOCKED_APPROVAL),
    ({'archive_requested': True, 'archive_approved': True, 'age_days': 10}, D.RETAIN),
])
def test_decisions(changes, expected):
    assert plan_lifecycle(approved(**changes)) is expected


@pytest.mark.parametrize('field', ['owner_approved', 'privacy_legal_approved', 'recovery_approved'])
@pytest.mark.parametrize('value', [False, None, 1, 'true'])
@pytest.mark.parametrize('archive', [False, True])
def test_missing_or_non_boolean_approval_blocks(field, value, archive):
    asset = approved(**{field: value}, archive_requested=archive, archive_approved=True)
    assert plan_lifecycle(asset) is D.BLOCKED_APPROVAL


@pytest.mark.parametrize('field', ['age_days', 'retention_days'])
@pytest.mark.parametrize('value', [None, -1, True, 1.5, float('nan'), '365'])
def test_unknown_or_invalid_clock_blocks(field, value):
    assert plan_lifecycle(approved(**{field: value})) is D.POLICY_UNRESOLVED


@pytest.mark.parametrize('field', ['asset_id', 'asset_type', 'sensitivity'])
def test_unknown_asset_metadata_blocks(field):
    assert plan_lifecycle(approved(**{field: ''})) is D.POLICY_UNRESOLVED


def test_blocker_precedence():
    asset = approved(protected_baseline=True, hold_status=HoldStatus.ACTIVE,
                     recovery_reference=True, recovery_backup=True,
                     dependencies_resolved=False, policy_conflict=True,
                     policy_resolved=False, owner_approved=False)
    steps = [('protected_baseline', False, D.BLOCKED_PROTECTED_BASELINE),
             ('hold_status', HoldStatus.CLEAR, D.BLOCKED_LEGAL_HOLD),
             ('recovery_reference', False, D.BLOCKED_RECOVERY_REFERENCE),
             ('recovery_backup', False, D.BLOCKED_RECOVERY_BACKUP),
             ('dependencies_resolved', True, D.BLOCKED_DEPENDENCIES),
             ('policy_conflict', False, D.POLICY_CONFLICT),
             ('policy_resolved', True, D.POLICY_UNRESOLVED),
             ('owner_approved', True, D.BLOCKED_APPROVAL)]
    for field, cleared, expected in steps:
        assert plan_lifecycle(asset) is expected
        asset = replace(asset, **{field: cleared})
    assert plan_lifecycle(asset) is D.ELIGIBLE_FOR_DISPOSAL


@pytest.mark.parametrize('conflict,expected', [(True, D.POLICY_CONFLICT),
                                             (False, D.POLICY_UNRESOLVED)])
def test_minio_365_cannot_override_unresolved_longer_declaration(conflict, expected):
    asset = approved(age_days=400, retention_days=365, policy_resolved=False,
                     policy_conflict=conflict)
    assert plan_lifecycle(asset) is expected
    assert plan_lifecycle(replace(asset, retention_days=3650)) is expected


def test_current_recovery_concepts_cannot_expire():
    for asset in [
        approved(asset_id='gate3-recovery-20260916T200450Z', asset_type='NESSIE_TAG',
                 recovery_reference=True, age_days=10000),
        approved(asset_id='dp-ai-payment-gate3-backup/gate3-current-state/20260916T201500Z/',
                 asset_type='BACKUP', recovery_backup=True, age_days=10000),
        approved(asset_type='FROZEN_BASELINE', protected_baseline=True, age_days=10000),
    ]:
        assert plan_lifecycle(asset) in {D.BLOCKED_RECOVERY_REFERENCE,
                                       D.BLOCKED_RECOVERY_BACKUP, D.BLOCKED_PROTECTED_BASELINE}


def test_disposal_consideration_has_no_io_or_mutation():
    asset = approved()
    with patch('builtins.open', side_effect=AssertionError('No filesystem IO')), \
         patch('os.remove', side_effect=AssertionError('No deletion')), \
         patch('os.unlink', side_effect=AssertionError('No deletion')), \
         patch('os.rename', side_effect=AssertionError('No move')), \
         patch('socket.socket', side_effect=AssertionError('No network')), \
         patch('subprocess.Popen', side_effect=AssertionError('No executor')):
        assert [plan_lifecycle(asset) for _ in range(3)] == [D.ELIGIBLE_FOR_DISPOSAL] * 3
    assert asset == approved()
    # Constrain the module itself to standard-library metadata types, not clients.
    source = Path(__file__).resolve().parents[1] / 'services/shared/lifecycle_planner.py'
    tree = ast.parse(source.read_text())
    imports = [n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)]
    assert set(imports) == {'dataclasses', 'enum'}
    assert not any(isinstance(n, ast.Import) for n in ast.walk(tree))


def test_default_unknown_metadata_fails_closed():
    assert plan_lifecycle(LifecycleAsset('synthetic', 'RAW', 'RESTRICTED')) is D.BLOCKED_PROTECTED_BASELINE
