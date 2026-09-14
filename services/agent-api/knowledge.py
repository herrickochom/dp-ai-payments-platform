from __future__ import annotations

import hashlib
import re
from abc import ABC, abstractmethod
from copy import deepcopy

from governance_models import DataClassification, IdentityContext
from knowledge_models import (
    KnowledgeChunk, KnowledgeCitation, KnowledgeDocument, KnowledgeEvidence,
    KnowledgeHit, KnowledgeQuery, RetrievalResult,
)
from observability import emit


TOKEN = re.compile(r"[a-z0-9]+")
INJECTION = re.compile(
    r"(?im)^.*(?:ignore (?:all )?(?:previous|system) instructions|system prompt|"
    r"grant (?:me )?permissions?|execute unrestricted sql|override governance).*$"
)


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _tokens(value: str) -> set[str]:
    return set(TOKEN.findall(value.lower()))


class KnowledgeRetriever(ABC):
    @abstractmethod
    def retrieve(self, query: KnowledgeQuery, governance_context=None) -> RetrievalResult:
        """Return only evidence authorised before retrieval content is exposed."""


class LocalKnowledgeRetriever(KnowledgeRetriever):
    def __init__(self, chunk_size: int = 800):
        if chunk_size < 100 or chunk_size > 4000:
            raise ValueError("chunk_size must be between 100 and 4000 characters")
        self.chunk_size = chunk_size
        self._documents: dict[str, KnowledgeDocument] = {}
        self._chunks: dict[str, KnowledgeChunk] = {}

    def ingest(self, document: KnowledgeDocument) -> list[KnowledgeChunk]:
        normalized = "\n".join(line.rstrip() for line in document.content.strip().splitlines())
        content_hash = _hash(normalized)
        identity = f"{document.metadata.source_type}|{document.metadata.source_uri}|{document.metadata.version}|{content_hash}"
        document_id = document.document_id or f"kdoc-{_hash(identity)[:24]}"
        metadata = document.metadata.model_copy(update={"content_hash": content_hash})
        stored = document.model_copy(update={"document_id": document_id, "content": normalized, "metadata": metadata})

        old_ids = [key for key, chunk in self._chunks.items() if chunk.document_id == document_id]
        for key in old_ids:
            del self._chunks[key]
        chunks = []
        for ordinal, content in enumerate(self._split(normalized)):
            chunk_id = f"kchunk-{_hash(f'{document_id}|{ordinal}|{content}')[:24]}"
            chunk = KnowledgeChunk(document_id=document_id, chunk_id=chunk_id,
                                   ordinal=ordinal, content=content, metadata=metadata)
            self._chunks[chunk_id] = chunk
            chunks.append(chunk)
        self._documents[document_id] = stored
        return chunks

    def _split(self, content: str) -> list[str]:
        paragraphs = [part.strip() for part in re.split(r"\n\s*\n", content) if part.strip()]
        chunks: list[str] = []
        current = ""
        for paragraph in paragraphs:
            pieces = [paragraph[i:i + self.chunk_size] for i in range(0, len(paragraph), self.chunk_size)]
            for piece in pieces:
                candidate = f"{current}\n\n{piece}".strip()
                if current and len(candidate) > self.chunk_size:
                    chunks.append(current)
                    current = piece
                else:
                    current = candidate
        if current:
            chunks.append(current)
        return chunks

    @staticmethod
    def _allowed(identity: IdentityContext, chunk: KnowledgeChunk) -> bool:
        metadata = chunk.metadata
        if metadata.allowed_roles and not set(identity.roles).intersection(metadata.allowed_roles):
            return False
        required = {
            DataClassification.PUBLIC: None,
            DataClassification.INTERNAL: "can_view_internal_data",
            DataClassification.CONFIDENTIAL: "can_view_confidential_data",
            DataClassification.RESTRICTED: "can_view_restricted_data",
        }[metadata.classification]
        return required is None or required in identity.permissions

    def retrieve(self, query: KnowledgeQuery, governance_context=None) -> RetrievalResult:
        query_tokens = _tokens(query.query)
        ranked = []
        for chunk in self._chunks.values():
            if not self._allowed(query.user_context, chunk):
                continue
            haystack = _tokens(f"{chunk.metadata.title} {chunk.content}")
            overlap = len(query_tokens.intersection(haystack))
            if not overlap:
                continue
            score = min(1.0, overlap / max(1, len(query_tokens)))
            ranked.append((-score, chunk.document_id, chunk.ordinal, chunk))
        hits = []
        for negative_score, _, _, stored_chunk in sorted(ranked)[:query.limit]:
            chunk = deepcopy(stored_chunk)
            redacted = bool(INJECTION.search(chunk.content))
            if redacted:
                chunk.content = INJECTION.sub("[untrusted instruction redacted]", chunk.content)
            citation = KnowledgeCitation(document_id=chunk.document_id, chunk_id=chunk.chunk_id,
                                         title=chunk.metadata.title, source_uri=chunk.metadata.source_uri)
            evidence = KnowledgeEvidence(citation=citation,
                                         classification=chunk.metadata.classification,
                                         retrieval_score=-negative_score,
                                         content_hash=chunk.metadata.content_hash or "")
            hits.append(KnowledgeHit(chunk=chunk, score=-negative_score, evidence=evidence,
                                     prompt_injection_redacted=redacted))
        emit("rag_retrieval",
             subject_id=query.user_context.subject_id,
             query_length=len(query.query), hits=len(hits),
             classifications=sorted({hit.evidence.classification.value for hit in hits}))
        return RetrievalResult(query=query.query, hits=hits,
                               citations=[hit.evidence.citation for hit in hits])
