from __future__ import annotations

from governance import GovernancePolicyEngine
from governance_models import (
    GovernanceRequest,
    PolicyDecision,
    PolicyDecisionType,
)


class GovernanceAgent:
    """
    Governance Agent.

    The agent does not grant or deny access itself.

    It invokes deterministic governance policy evaluation and
    explains the resulting decision.

    The deterministic GovernancePolicyEngine remains the
    authoritative enforcement component.
    """

    name = "governance"

    def __init__(
        self,
        policy_engine: GovernancePolicyEngine | None = None,
    ):
        self.policy_engine = (
            policy_engine
            if policy_engine is not None
            else GovernancePolicyEngine()
        )

    def evaluate(
        self,
        request: GovernanceRequest,
    ) -> dict:
        """
        Evaluate a governance request using the deterministic
        policy engine and return an explanatory agent response.

        The agent cannot override the policy decision.
        """

        decision = self.policy_engine.evaluate(
            request
        )

        return {
            "agent": self.name,
            "decision": decision,
            "explanation": self._explain(
                decision
            ),
        }

    @staticmethod
    def _explain(
        decision: PolicyDecision,
    ) -> str:
        """
        Produce a human-readable explanation of the deterministic
        policy decision.

        This method explains the result only. It does not alter,
        reinterpret, or override the policy decision.
        """

        if (
            decision.decision
            == PolicyDecisionType.ALLOW
        ):
            return (
                "Access is allowed by the deterministic "
                "governance policy evaluation."
            )

        if (
            decision.decision
            == PolicyDecisionType.MASK
        ):
            fields = ", ".join(
                decision.masked_fields
            )

            if fields:
                return (
                    "Access is permitted, but sensitive fields "
                    "must be masked before the result reaches "
                    "the requesting agent or user. "
                    f"Masked fields: {fields}."
                )

            return (
                "Access is permitted, but governance policy "
                "requires sensitive values to be masked before "
                "the result reaches the requesting agent or user."
            )

        if (
            decision.decision
            == PolicyDecisionType.REQUIRE_APPROVAL
        ):
            return (
                "The request involves sensitive data and "
                "requires approved access before execution."
            )

        reasons = " ".join(
            decision.reasons
        ).strip()

        if reasons:
            return (
                "Access is denied by the deterministic "
                "governance policy evaluation. "
                + reasons
            )

        return (
            "Access is denied by the deterministic "
            "governance policy evaluation."
        )