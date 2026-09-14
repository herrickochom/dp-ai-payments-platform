from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from governance_models import DataClassification, IdentityContext


class KnowledgeMetadata(BaseModel):
    title: str = Field(min_length=1, max_length=300)
    source_type: str = Field(min_length=1, max_length=80)
    source_uri: str = Field(min_length=1, max_length=1000)
    version: str = Field(default="1", min_length=1, max_length=80)
    effective_date: date | None = None
    classification: DataClassification = DataClassification.INTERNAL
    allowed_roles: list[str] = Field(default_factory=list)
    geography_scope: str | None = None
    content_hash: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class KnowledgeDocument(BaseModel):
    document_id: str | None = None
    content: str = Field(min_length=1, max_length=2_000_000)
    metadata: KnowledgeMetadata

    @model_validator(mode="after")
    def source_is_known(self):
        if self.metadata.source_type not in {
            "policy", "guidance", "data_dictionary", "technical_documentation",
            "iso20022_reference", "runbook", "governance", "architecture",
            "procedure",
        }:
            raise ValueError("unknown knowledge source_type")
        return self


class KnowledgeChunk(BaseModel):
    document_id: str
    chunk_id: str
    ordinal: int = Field(ge=0)
    content: str
    metadata: KnowledgeMetadata


class KnowledgeQuery(BaseModel):
    query: str = Field(min_length=2, max_length=2000)
    user_context: IdentityContext
    limit: int = Field(default=5, ge=1, le=20)


class KnowledgeCitation(BaseModel):
    document_id: str
    chunk_id: str
    title: str
    source_uri: str


class KnowledgeEvidence(BaseModel):
    citation: KnowledgeCitation
    classification: DataClassification
    retrieval_score: float = Field(ge=0.0, le=1.0)
    content_hash: str


class KnowledgeHit(BaseModel):
    chunk: KnowledgeChunk
    score: float = Field(ge=0.0, le=1.0)
    evidence: KnowledgeEvidence
    content_is_untrusted: bool = True
    instructions_allowed: Literal[False] = False
    prompt_injection_redacted: bool = False


class RetrievalResult(BaseModel):
    query: str
    hits: list[KnowledgeHit] = Field(default_factory=list)
    citations: list[KnowledgeCitation] = Field(default_factory=list)
    backend: str = "local_deterministic"


class HybridEvidence(BaseModel):
    knowledge: list[KnowledgeEvidence] = Field(default_factory=list)
    live_data: list[dict[str, Any]] = Field(default_factory=list)

