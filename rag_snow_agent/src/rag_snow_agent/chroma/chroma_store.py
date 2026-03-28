"""Thin wrapper around a persistent ChromaDB client."""

from __future__ import annotations

import logging
from pathlib import Path

import chromadb

from .schema_cards import ColumnCard, JoinCard, TableCard

log = logging.getLogger(__name__)

DEFAULT_CHROMA_DIR = Path(__file__).resolve().parents[3] / ".chroma"
SCHEMA_COLLECTION = "schema_cards"


class ChromaStore:
    """Manages the persistent ChromaDB instance and the schema_cards collection."""

    def __init__(self, persist_dir: str | Path | None = None) -> None:
        self.persist_dir = Path(persist_dir) if persist_dir else DEFAULT_CHROMA_DIR
        self.persist_dir.mkdir(parents=True, exist_ok=True)
        log.info("ChromaDB persist dir: %s", self.persist_dir)
        self.client = chromadb.PersistentClient(path=str(self.persist_dir))

    def schema_collection(self) -> chromadb.Collection:
        return self.client.get_or_create_collection(
            name=SCHEMA_COLLECTION,
            metadata={"hnsw:space": "cosine"},
        )

    # ------------------------------------------------------------------
    # Bulk upsert helpers
    # ------------------------------------------------------------------

    def upsert_table_cards(self, cards: list[TableCard]) -> int:
        if not cards:
            return 0
        col = self.schema_collection()
        col.upsert(
            ids=[c.chroma_id() for c in cards],
            documents=[c.document for c in cards],
            metadatas=[c.chroma_metadata() for c in cards],
        )
        log.info("Upserted %d TableCards", len(cards))
        return len(cards)

    def upsert_column_cards(self, cards: list[ColumnCard]) -> int:
        if not cards:
            return 0
        col = self.schema_collection()
        # Chroma upsert batch limit is ~40k; chunk if needed
        batch = 5000
        for i in range(0, len(cards), batch):
            chunk = cards[i : i + batch]
            col.upsert(
                ids=[c.chroma_id() for c in chunk],
                documents=[c.document for c in chunk],
                metadatas=[c.chroma_metadata() for c in chunk],
            )
        log.info("Upserted %d ColumnCards", len(cards))
        return len(cards)

    def upsert_join_cards(self, cards: list[JoinCard]) -> int:
        if not cards:
            return 0
        col = self.schema_collection()
        batch = 5000
        for i in range(0, len(cards), batch):
            chunk = cards[i : i + batch]
            col.upsert(
                ids=[c.chroma_id() for c in chunk],
                documents=[c.document for c in chunk],
                metadatas=[c.chroma_metadata() for c in chunk],
            )
        log.info("Upserted %d JoinCards", len(cards))
        return len(cards)

    def count_by_type(self, db_id: str) -> dict[str, int]:
        """Return {object_type: count} for a given db_id."""
        col = self.schema_collection()
        results = col.get(
            where={"db_id": db_id},
            include=["metadatas"],
        )
        counts: dict[str, int] = {}
        for meta in results["metadatas"] or []:
            otype = meta.get("object_type", "unknown")
            counts[otype] = counts.get(otype, 0) + 1
        return counts
