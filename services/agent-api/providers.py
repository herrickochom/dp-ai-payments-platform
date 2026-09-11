from typing import Protocol

from config import Settings


class ModelProvider(Protocol):
    def complete(self, prompt: str) -> str: ...


class DisabledModelProvider:
    def complete(self, prompt: str) -> str:
        raise RuntimeError("No model provider is configured")


def configured_provider(settings: Settings) -> ModelProvider:
    # Phase 1 deliberately defines the provider boundary without requiring a
    # provider SDK or secret. Deterministic discovery/analytics remain usable.
    if settings.model_provider == "disabled":
        return DisabledModelProvider()
    raise ValueError(f"Unsupported AGENT_MODEL_PROVIDER: {settings.model_provider}")

