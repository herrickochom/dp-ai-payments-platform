from __future__ import annotations

import hashlib
import time
from collections.abc import Callable

from observability import brief, emit
from orchestration_models import (
    AgentFailure, AgentPlan, AgentResult, AgentStep, AgentTask, ExecutionContext,
    OrchestrationResult, OrchestrationStatus, StepStatus,
)


class PlanValidationError(ValueError):
    pass


class ExecutionLimitError(RuntimeError):
    pass


Handler = Callable[[AgentStep, AgentTask, dict[str, AgentResult]], AgentResult]
PolicyGuard = Callable[[AgentStep, AgentTask, str], str]


KNOWN_AGENTS = {
    "data_discovery", "analytics", "visualization", "dashboard",
    "data_quality", "insight", "governance", "knowledge",
}


class DeterministicPlanner:
    def __init__(self, max_steps: int = 8):
        self.max_steps = max_steps

    def plan(self, task: AgentTask) -> AgentPlan:
        text = task.objective.lower()
        if task.requested_agent:
            agents = [task.requested_agent]
        elif "dashboard" in text or "scorecard" in text:
            agents = ["data_discovery", "analytics", "visualization", "dashboard"]
        elif any(word in text for word in ("policy", "definition", "guidance")) and any(
            word in text for word in ("current", "how many", "which district", "currently")
        ):
            agents = ["knowledge", "analytics"]
        elif any(word in text for word in ("policy", "definition", "guidance", "document")):
            agents = ["knowledge"]
        elif "beneficiary" in text:
            agents = ["governance", "analytics"]
        elif any(word in text for word in ("permission", "access", "authoris", "publish restricted")):
            agents = ["governance"]
        elif any(word in text for word in ("data quality", "missing", "duplicate", "freshness")):
            agents = ["data_quality"]
        elif any(word in text for word in ("anomaly", "monitor", "unusual", "what changed")):
            agents = ["insight"]
        elif any(word in text for word in ("chart", "visualize", "visualise", "graph")):
            agents = ["data_discovery", "analytics", "visualization"]
        elif any(word in text for word in ("schema", "table", "column", "dataset")):
            agents = ["data_discovery"]
        else:
            agents = ["analytics"]
        if any(agent not in KNOWN_AGENTS for agent in agents):
            raise PlanValidationError("unknown requested agent")
        if len(agents) > self.max_steps:
            raise ExecutionLimitError("maximum plan step count exceeded")
        steps = []
        for index, agent in enumerate(agents):
            # Knowledge and analytics are independent hybrid branches.
            dependencies = [] if agents == ["knowledge", "analytics"] else (
                [f"step-{index}"] if index else []
            )
            steps.append(AgentStep(step_id=f"step-{index + 1}", agent=agent,
                                   objective=task.objective, dependencies=dependencies))
        digest = hashlib.sha256((task.objective + "|" + ",".join(agents)).encode()).hexdigest()[:20]
        return AgentPlan(plan_id=f"plan-{digest}", steps=steps)


class MultiAgentOrchestrator:
    def __init__(self, handlers: dict[str, Handler], policy_guard: PolicyGuard,
                 planner: DeterministicPlanner | None = None):
        self.handlers = handlers
        self.policy_guard = policy_guard
        self.planner = planner or DeterministicPlanner()

    @staticmethod
    def validate_plan(plan: AgentPlan, context: ExecutionContext) -> list[AgentStep]:
        if len(plan.steps) > context.max_steps:
            raise ExecutionLimitError("maximum plan step count exceeded")
        ids = [step.step_id for step in plan.steps]
        if len(ids) != len(set(ids)):
            raise PlanValidationError("duplicate step_id")
        by_id = {step.step_id: step for step in plan.steps}
        for step in plan.steps:
            if step.agent not in KNOWN_AGENTS:
                raise PlanValidationError("unknown agent")
            if any(dep not in by_id for dep in step.dependencies):
                raise PlanValidationError("unknown dependency")
        ordered, visiting, visited = [], set(), set()
        def visit(step_id: str, depth: int):
            if depth > context.max_depth:
                raise ExecutionLimitError("maximum plan depth exceeded")
            if step_id in visiting:
                raise PlanValidationError("circular plan")
            if step_id in visited:
                return
            visiting.add(step_id)
            for dependency in by_id[step_id].dependencies:
                visit(dependency, depth + 1)
            visiting.remove(step_id); visited.add(step_id); ordered.append(by_id[step_id])
        for step_id in ids:
            visit(step_id, 1)
        return ordered

    def execute(self, task: AgentTask, context: ExecutionContext,
                plan: AgentPlan | None = None) -> OrchestrationResult:
        started = time.monotonic()
        emit("orchestration_started", request_id=context.request_id,
             subject_id=context.identity.subject_id,
             objective=brief(task.objective))
        precheck = self.policy_guard(
            AgentStep(step_id="precheck", agent="governance", objective=task.objective),
            task, "before_plan",
        )
        if precheck in {"DENY", "REQUIRE_APPROVAL"}:
            status = (OrchestrationStatus.DENIED if precheck == "DENY"
                      else OrchestrationStatus.REQUIRES_APPROVAL)
            empty_plan = plan or AgentPlan(plan_id="policy-precheck", steps=[])
            emit("policy_decision", request_id=context.request_id, phase="before_plan",
                 decision=precheck, status=status.value)
            emit("orchestration_completed", request_id=context.request_id, status=status.value)
            return OrchestrationResult(request_id=context.request_id, plan=empty_plan, status=status,
                                       failures=[AgentFailure(category="PolicyDecision", message=precheck)])
        plan = plan or self.planner.plan(task)
        ordered = self.validate_plan(plan, context)
        results: dict[str, AgentResult] = {}
        total_tools = 0
        for step in ordered:
            emit("agent_selected", request_id=context.request_id, step_id=step.step_id,
                 agent=step.agent)
            if time.monotonic() - started > context.max_elapsed_seconds:
                raise ExecutionLimitError("elapsed execution budget exceeded")
            if any(results[dep].status != StepStatus.COMPLETED for dep in step.dependencies):
                step.status = StepStatus.SKIPPED
                step.failure = AgentFailure(category="DependencyFailure", message="required dependency did not complete")
                results[step.step_id] = AgentResult(status=StepStatus.SKIPPED, failure=step.failure)
                continue
            decision = self.policy_guard(step, task, "before_tool")
            emit("policy_decision", request_id=context.request_id, step_id=step.step_id,
                 phase="before_tool", decision=decision)
            if decision in {"DENY", "REQUIRE_APPROVAL"}:
                step.status = StepStatus.DENIED if decision == "DENY" else StepStatus.REQUIRES_APPROVAL
                step.failure = AgentFailure(category="PolicyDecision", message=decision)
                results[step.step_id] = AgentResult(status=step.status, failure=step.failure)
                continue
            handler = self.handlers.get(step.agent)
            if handler is None:
                result = AgentResult(status=StepStatus.FAILED,
                                     failure=AgentFailure(category="HandlerUnavailable", message=step.agent))
            else:
                try:
                    result = handler(
                        step, task,
                        {dependency: results[dependency] for dependency in step.dependencies},
                    )
                except Exception as exc:
                    result = AgentResult(status=StepStatus.FAILED,
                                         failure=AgentFailure(category=type(exc).__name__, message=str(exc)))
            total_tools += result.tool_calls
            if total_tools > context.max_tool_calls:
                raise ExecutionLimitError("maximum tool call count exceeded")
            step.status, step.evidence, step.failure = result.status, result.evidence, result.failure
            results[step.step_id] = result
        final_decision = self.policy_guard(AgentStep(step_id="final", agent="governance",
                                                     objective=task.objective), task, "before_return")
        emit("policy_decision", request_id=context.request_id, phase="before_return",
             decision=final_decision)
        statuses = [result.status for result in results.values()]
        if final_decision == "DENY" or StepStatus.DENIED in statuses:
            status = OrchestrationStatus.DENIED
        elif final_decision == "REQUIRE_APPROVAL" or StepStatus.REQUIRES_APPROVAL in statuses:
            status = OrchestrationStatus.REQUIRES_APPROVAL
        elif statuses and all(value == StepStatus.COMPLETED for value in statuses):
            status = OrchestrationStatus.COMPLETED
        elif StepStatus.COMPLETED in statuses:
            status = OrchestrationStatus.PARTIAL
        else:
            status = OrchestrationStatus.FAILED
        emit("orchestration_completed", request_id=context.request_id, status=status.value,
             steps=len(ordered), tool_calls=total_tools,
             elapsed_ms=round((time.monotonic() - started) * 1000, 2))
        return OrchestrationResult(
            request_id=context.request_id, plan=plan, status=status,
            outputs={key: value.output for key, value in results.items() if value.output},
            evidence=[e for value in results.values() for e in value.evidence],
            failures=[value.failure for value in results.values() if value.failure],
        )
