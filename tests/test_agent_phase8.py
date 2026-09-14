import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services/agent-api"))

from governance_models import IdentityContext
from multi_agent import DeterministicPlanner, ExecutionLimitError, MultiAgentOrchestrator, PlanValidationError
from orchestration_models import (
    AgentEvidence, AgentPlan, AgentResult, AgentStep, AgentTask, ExecutionContext,
    OrchestrationStatus, StepStatus,
)


def task(objective, permissions=()):
    return AgentTask(objective=objective,
                     identity=IdentityContext(subject_id="user", permissions=list(permissions)))


def context(**values):
    return ExecutionContext(request_id="request-1", identity=IdentityContext(subject_id="user"), **values)


def allow(_step, _task, _phase): return "ALLOW"


def success(step, _task, dependencies):
    return AgentResult(output={"agent": step.agent, "dependencies": sorted(dependencies)}, tool_calls=1,
                       evidence=[AgentEvidence(agent=step.agent, kind="tool", reference=f"{step.agent}-result",
                                               tool=f"{step.agent}.run")])


def test_deterministic_routing_scenarios():
    planner = DeterministicPlanner()
    assert [s.agent for s in planner.plan(task("Show district repayment performance")).steps] == ["analytics"]
    assert [s.agent for s in planner.plan(task("Create a dashboard of districts at repayment risk")).steps] == [
        "data_discovery", "analytics", "visualization", "dashboard"]
    assert [s.agent for s in planner.plan(task(
        "What is the policy definition of delayed payment and which districts currently exceed it?"
    )).steps] == ["knowledge", "analytics"]
    assert [s.agent for s in planner.plan(task("Why did beneficiary X not receive payment?")).steps] == [
        "governance", "analytics"]


def test_dag_execution_builder_and_evidence_aggregation():
    orchestrator = MultiAgentOrchestrator({name: success for name in (
        "data_discovery", "analytics", "visualization", "dashboard")}, allow)
    result = orchestrator.execute(task("Create a dashboard of repayment risk"), context())
    assert result.status == OrchestrationStatus.COMPLETED
    assert result.outputs["step-4"]["dependencies"] == ["step-3"]
    assert [e.agent for e in result.evidence] == ["data_discovery", "analytics", "visualization", "dashboard"]


def test_hybrid_branches_preserve_partial_evidence():
    def knowledge_failure(*_):
        raise RuntimeError("retrieval unavailable")
    orchestrator = MultiAgentOrchestrator({"knowledge": knowledge_failure, "analytics": success}, allow)
    result = orchestrator.execute(task("What policy applies and how many payments currently fail?"), context())
    assert result.status == OrchestrationStatus.PARTIAL
    assert "step-2" in result.outputs
    assert result.failures[0].category == "RuntimeError"


def test_policy_denial_and_approval_cannot_be_overridden():
    def guard(_step, requested, phase):
        if "restricted" in requested.objective.lower(): return "DENY"
        if "publish" in requested.objective.lower() and phase == "before_plan": return "REQUIRE_APPROVAL"
        return "ALLOW"
    orchestrator = MultiAgentOrchestrator({"governance": success}, guard)
    assert orchestrator.execute(task("publish restricted beneficiary data"), context()).status == OrchestrationStatus.DENIED
    assert orchestrator.execute(task("publish dashboard"), context()).status == OrchestrationStatus.REQUIRES_APPROVAL


def test_dependency_failure_skips_downstream_steps():
    plan = AgentPlan(plan_id="p", steps=[
        AgentStep(step_id="a", agent="data_discovery", objective="discover"),
        AgentStep(step_id="b", agent="analytics", objective="analyse", dependencies=["a"]),
    ])
    orchestrator = MultiAgentOrchestrator({"data_discovery": lambda *_: AgentResult(status=StepStatus.FAILED),
                                            "analytics": success}, allow)
    result = orchestrator.execute(task("analyse data"), context(), plan)
    assert result.status == OrchestrationStatus.FAILED
    assert result.plan.steps[1].status == StepStatus.SKIPPED


def test_loop_unknown_agent_and_step_limits_are_rejected():
    orchestrator = MultiAgentOrchestrator({}, allow)
    circular = AgentPlan(plan_id="cycle", steps=[
        AgentStep(step_id="a", agent="analytics", objective="a", dependencies=["b"]),
        AgentStep(step_id="b", agent="analytics", objective="b", dependencies=["a"]),
    ])
    with pytest.raises(PlanValidationError):
        orchestrator.execute(task("valid objective"), context(), circular)
    too_many = AgentPlan(plan_id="large", steps=[
        AgentStep(step_id=str(i), agent="analytics", objective="analyse") for i in range(3)
    ])
    with pytest.raises(ExecutionLimitError):
        orchestrator.execute(task("valid objective"), context(max_steps=2), too_many)


def test_tool_limit_is_enforced():
    orchestrator = MultiAgentOrchestrator({"analytics": lambda *_: AgentResult(tool_calls=2)}, allow)
    with pytest.raises(ExecutionLimitError):
        orchestrator.execute(task("repayment performance"), context(max_tool_calls=1))
