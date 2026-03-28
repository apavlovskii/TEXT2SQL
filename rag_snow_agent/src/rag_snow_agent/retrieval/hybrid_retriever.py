"""Hybrid retriever: dense (ChromaDB) + lexical boost, fused with RRF."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import chromadb

log = logging.getLogger(__name__)

# ── token splitting ──────────────────────────────────────────────────────────

_SPLIT_RE = re.compile(r"[_.\s]+|(?<=[a-z])(?=[A-Z])")


def tokenize_identifier(name: str) -> set[str]:
    """Split a qualified name or NL query into lowercase tokens."""
    return {t.lower() for t in _SPLIT_RE.split(name) if t}


# ── RRF ──────────────────────────────────────────────────────────────────────


def reciprocal_rank_fusion(
    ranked_lists: list[list[str]],
    k: int = 60,
) -> list[tuple[str, float]]:
    """Fuse multiple ranked ID lists with RRF.

    Returns [(id, rrf_score)] sorted descending by score.
    """
    scores: dict[str, float] = {}
    for ranked in ranked_lists:
        for rank_0, item_id in enumerate(ranked):
            scores[item_id] = scores.get(item_id, 0.0) + 1.0 / (k + rank_0 + 1)
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)


# ── scored item ──────────────────────────────────────────────────────────────


@dataclass
class ScoredItem:
    chroma_id: str
    object_type: str  # "table" or "column"
    qualified_name: str
    metadata: dict = field(default_factory=dict)
    dense_rank: int = 0  # 1-based
    lexical_rank: int = 0
    fused_rank: int = 0
    rrf_score: float = 0.0


# ── retriever ────────────────────────────────────────────────────────────────


class HybridRetriever:
    """Query ChromaDB with dense + lexical, fuse with RRF."""

    def __init__(self, collection: chromadb.Collection, rrf_k: int = 60) -> None:
        self.collection = collection
        self.rrf_k = rrf_k

    def retrieve(
        self,
        query: str,
        db_id: str,
        object_type: str,
        top_k: int = 50,
    ) -> list[ScoredItem]:
        """Return up to *top_k* items of *object_type* for *db_id*, ranked by RRF."""
        # 1) Dense retrieval from Chroma
        dense_results = self.collection.query(
            query_texts=[query],
            n_results=min(top_k * 2, 200),  # over-fetch to allow re-ranking
            where={"$and": [{"db_id": db_id}, {"object_type": object_type}]},
            include=["metadatas", "distances"],
        )
        ids = dense_results["ids"][0] if dense_results["ids"] else []
        metadatas = dense_results["metadatas"][0] if dense_results["metadatas"] else []

        if not ids:
            return []

        # Build lookup
        meta_by_id: dict[str, dict] = {}
        for cid, meta in zip(ids, metadatas):
            meta_by_id[cid] = meta

        # Dense ranked list (Chroma returns nearest first)
        dense_ranked = list(ids)

        # 2) Lexical ranking: overlap between query tokens and qualified_name tokens
        query_tokens = tokenize_identifier(query)
        scored_lex: list[tuple[str, float]] = []
        for cid, meta in meta_by_id.items():
            qname = meta.get("qualified_name", "")
            name_tokens = tokenize_identifier(qname)
            overlap = len(query_tokens & name_tokens)
            scored_lex.append((cid, overlap))
        # Sort desc by overlap, stable
        scored_lex.sort(key=lambda x: x[1], reverse=True)
        lexical_ranked = [cid for cid, _ in scored_lex]

        # 3) RRF fusion
        fused = reciprocal_rank_fusion([dense_ranked, lexical_ranked], k=self.rrf_k)

        # Build dense rank lookup
        dense_rank_map = {cid: r + 1 for r, cid in enumerate(dense_ranked)}
        lex_rank_map = {cid: r + 1 for r, cid in enumerate(lexical_ranked)}

        items: list[ScoredItem] = []
        for fused_rank_0, (cid, rrf_score) in enumerate(fused[:top_k]):
            meta = meta_by_id.get(cid, {})
            items.append(
                ScoredItem(
                    chroma_id=cid,
                    object_type=object_type,
                    qualified_name=meta.get("qualified_name", cid),
                    metadata=meta,
                    dense_rank=dense_rank_map.get(cid, 0),
                    lexical_rank=lex_rank_map.get(cid, 0),
                    fused_rank=fused_rank_0 + 1,
                    rrf_score=rrf_score,
                )
            )
        return items

    def retrieve_tables(
        self, query: str, db_id: str, top_k: int = 50
    ) -> list[ScoredItem]:
        return self.retrieve(query, db_id, "table", top_k)

    def retrieve_columns(
        self, query: str, db_id: str, top_k: int = 100
    ) -> list[ScoredItem]:
        return self.retrieve(query, db_id, "column", top_k)
