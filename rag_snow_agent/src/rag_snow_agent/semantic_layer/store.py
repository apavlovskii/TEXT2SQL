"""Semantic layer Chroma store for persisting and querying semantic cards."""

from __future__ import annotations

import logging

import chromadb

from ..chroma.chroma_store import ChromaStore
from .models import SemanticCard, SemanticProfile

log = logging.getLogger(__name__)

SEMANTIC_COLLECTION = "semantic_cards"


class SemanticLayerStore:
    """Store and query semantic cards in ChromaDB."""

    def __init__(self, chroma_store: ChromaStore) -> None:
        self._chroma_store = chroma_store

    def collection(self) -> chromadb.Collection:
        """Return the semantic_cards collection."""
        kwargs: dict = {
            "name": SEMANTIC_COLLECTION,
            "metadata": {"hnsw:space": "cosine"},
        }
        if self._chroma_store._embedding_fn is not None:
            kwargs["embedding_function"] = self._chroma_store._embedding_fn
        return self._chroma_store.client.get_or_create_collection(**kwargs)

    def upsert_semantic_profile(self, profile: SemanticProfile) -> int:
        """Persist all facts from a SemanticProfile as SemanticCards.

        Returns the number of cards upserted.
        """
        cards: list[SemanticCard] = []
        for fact in profile.all_facts():
            cards.append(
                SemanticCard(
                    db_id=profile.db_id,
                    fact_type=fact.fact_type,
                    subject=fact.subject,
                    confidence=fact.confidence,
                    source_types=fact.source,
                )
            )

        if not cards:
            return 0

        col = self.collection()
        batch_size = 500
        for i in range(0, len(cards), batch_size):
            chunk = cards[i : i + batch_size]
            col.upsert(
                ids=[c.chroma_id() for c in chunk],
                documents=[c.document for c in chunk],
                metadatas=[c.chroma_metadata() for c in chunk],
            )

        log.info("Upserted %d SemanticCards for %s", len(cards), profile.db_id)
        return len(cards)

    def query_semantic_cards(
        self, db_id: str, instruction: str, top_k: int = 10
    ) -> list[dict]:
        """Query semantic cards for a given db_id and instruction.

        Returns list of dicts with card metadata and documents.
        """
        col = self.collection()
        try:
            results = col.query(
                query_texts=[instruction],
                n_results=top_k,
                where={"db_id": db_id},
                include=["metadatas", "documents", "distances"],
            )
        except Exception:
            log.debug(
                "Semantic card query failed for db_id=%s", db_id, exc_info=True
            )
            return []

        cards: list[dict] = []
        ids = results.get("ids", [[]])[0]
        docs = results.get("documents", [[]])[0]
        metas = results.get("metadatas", [[]])[0]
        dists = results.get("distances", [[]])[0]

        for i, cid in enumerate(ids):
            cards.append(
                {
                    "id": cid,
                    "document": docs[i] if i < len(docs) else "",
                    "metadata": metas[i] if i < len(metas) else {},
                    "distance": dists[i] if i < len(dists) else 1.0,
                }
            )
        return cards
