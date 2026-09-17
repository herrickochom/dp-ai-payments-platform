"""Verify synthetic coverage, delegation, offline execution and exit results."""
import ast
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

from services.shared import lifecycle_planner as planner
from services.shared import lifecycle_planner_demo as demo


EXPECTED = (
    'BLOCKED_PROTECTED_BASELINE', 'BLOCKED_LEGAL_HOLD', 'BLOCKED_LEGAL_HOLD',
    'BLOCKED_RECOVERY_REFERENCE', 'BLOCKED_RECOVERY_BACKUP',
    'BLOCKED_DEPENDENCIES', 'POLICY_CONFLICT', 'POLICY_UNRESOLVED', 'RETAIN',
    'BLOCKED_APPROVAL', 'ELIGIBLE_FOR_DISPOSAL', 'ELIGIBLE_FOR_ARCHIVE',
)


def test_exact_synthetic_scenarios_and_expected_decisions():
    scenarios = demo.build_scenarios()
    assert len(scenarios) == 12
    assert tuple(s.expected.value for s in scenarios) == EXPECTED
    assert tuple(planner.plan_lifecycle(s.asset).value for s in scenarios) == EXPECTED
    assert len({s.asset.asset_id for s in scenarios}) == 12
    for scenario in scenarios:
        assert scenario.asset.asset_id.startswith('SYNTHETIC_DEMO_')
        assert scenario.asset.asset_type.startswith('SYNTHETIC_')
        assert scenario.asset.sensitivity == 'SYNTHETIC_ONLY'
    assert scenarios[1].asset.hold_status is planner.HoldStatus.ACTIVE
    assert scenarios[2].asset.hold_status is planner.HoldStatus.UNKNOWN
    assert scenarios[7].asset.age_days is None
    assert scenarios[8].asset.age_days < scenarios[8].asset.retention_days
    assert scenarios[11].asset.archive_requested is True
    assert scenarios[11].asset.archive_approved is True


def test_existing_planner_is_used_and_demo_succeeds(capsys):
    assert demo.planner is planner
    with patch.object(planner, 'plan_lifecycle', wraps=planner.plan_lifecycle) as spy:
        assert demo.main() == 0
    assert spy.call_count == 12
    assert [call.args[0] for call in spy.call_args_list] == [
        s.asset for s in demo.build_scenarios()]
    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == ('SCENARIO | ASSET_TYPE | AGE_DAYS | RETENTION_DAYS | '
                        'EXPECTED | ACTUAL | RESULT')
    assert len(lines[1:13]) == 12
    assert all(line.endswith(' | PASS') for line in lines[1:13])
    assert lines[-5:] == ['TOTAL_SCENARIOS=12', 'PASSED=12', 'FAILED=0',
                         'DESTRUCTIVE_ACTIONS=0', 'LIVE_INFRASTRUCTURE_CONNECTIONS=0']


def test_mismatch_returns_failure(capsys):
    original = planner.plan_lifecycle
    def wrong_first(asset):
        if asset.asset_id == 'SYNTHETIC_DEMO_01':
            return planner.LifecycleDecision.RETAIN
        return original(asset)
    with patch.object(planner, 'plan_lifecycle', side_effect=wrong_first):
        assert demo.main() == 1
    output = capsys.readouterr().out
    assert ' | FAIL\n' in output
    assert 'PASSED=11\nFAILED=1\n' in output


def test_offline_without_execution_or_mutation(capsys):
    forbidden = ('builtins.open', 'socket.socket', 'socket.create_connection',
                 'subprocess.Popen', 'os.system', 'os.remove', 'os.unlink',
                 'os.rename', 'os.replace', 'shutil.move', 'shutil.rmtree',
                 'pathlib.Path.open', 'pathlib.Path.unlink', 'pathlib.Path.rename')
    with ExitStack() as stack:
        for target in forbidden:
            stack.enter_context(patch(target, side_effect=AssertionError(target)))
        assert demo.main() == 0
    assert 'DESTRUCTIVE_ACTIONS=0' in capsys.readouterr().out


def test_no_client_or_executor_capability_in_demo_source():
    tree = ast.parse(Path(demo.__file__).read_text())
    imports = [node for node in ast.walk(tree)
               if isinstance(node, (ast.Import, ast.ImportFrom))]
    assert len(imports) == 3
    for node in imports:
        if isinstance(node, ast.Import):
            assert [alias.name for alias in node.names] == ['lifecycle_planner']
        elif node.module == 'dataclasses':
            assert {alias.name for alias in node.names} == {'dataclass', 'replace'}
        else:
            assert node.level == 1 and node.module is None
            assert [alias.name for alias in node.names] == ['lifecycle_planner']
    # Restrict all callable expressions to metadata construction, printing,
    # the existing planner, and normal exit handling. No dynamic executor.
    allowed_names = {'dataclass', 'replace', 'Scenario', 'tuple', 'enumerate',
                     'build_scenarios', 'print', 'int', 'len', 'SystemExit', 'main'}
    allowed_attributes = {('planner', 'LifecycleAsset'), ('planner', 'plan_lifecycle')}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                assert node.func.id in allowed_names
            else:
                assert isinstance(node.func, ast.Attribute)
                assert isinstance(node.func.value, ast.Name)
                assert (node.func.value.id, node.func.attr) in allowed_attributes
