"""Thin wrapper around a persistent ChromaDB client."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import chromadb
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction

from .schema_cards import ColumnCard, JoinCard, TableCard

log = logging.getLogger(__name__)

DEFAULT_CHROMA_DIR = Path(__file__).resolve().parents[3] / ".chroma"
SCHEMA_COLLECTION = "schema_cards"
DEFAULT_EMBEDDING_MODEL = "text-embedding-3-large"


def _get_embedding_function() -> OpenAIEmbeddingFunction | None:
    """Return OpenAI embedding function if API key is available, else None (Chroma default)."""
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        # Try loading from .env / .env.example
        try:
            from dotenv import load_dotenv

            for env_file in [".env", ".env.example"]:
                p = Path(env_file)
                if p.exists():
                    load_dotenv(p, override=False)
                    api_key = os.environ.get("OPENAI_API_KEY")
                    if api_key:
                        break
        except ImportError:
            pass

    if not api_key:
        log.warning(
            "OPENAI_API_KEY not set; falling back to Chroma default embeddings "
            "(all-MiniLM-L6-v2). Set OPENAI_API_KEY for stronger retrieval."
        )
        return None

    log.info("Using OpenAI embedding model: %s", DEFAULT_EMBEDDING_MODEL)
    return OpenAIEmbeddingFunction(
        api_key=api_key,
        model_name=DEFAULT_EMBEDDING_MODEL,
    )


class ChromaStore:
    """Manages the persistent ChromaDB instance and the schema_cards collection."""

    def __init__(self, persist_dir: str | Path | None = None) -> None:
        self.persist_dir = Path(persist_dir) if persist_dir else DEFAULT_CHROMA_DIR
        self.persist_dir.mkdir(parents=True, exist_ok=True)
        log.info("ChromaDB persist dir: %s", self.persist_dir)
        self.client = chromadb.PersistentClient(path=str(self.persist_dir))
        self._embedding_fn = _get_embedding_function()

    def schema_collection(self) -> chromadb.Collection:
        kwargs: dict = {
            "name": SCHEMA_COLLECTION,
            "metadata": {"hnsw:space": "cosine"},
        }
        if self._embedding_fn is not None:
            kwargs["embedding_function"] = self._embedding_fn
        return self.client.get_or_create_collection(**kwargs)

    # ------------------------------------------------------------------
    # Bulk upsert helpers
    # ------------------------------------------------------------------

    # OpenAI embedding API allows max 2048 inputs per request
    _UPSERT_BATCH = 500

    def upsert_table_cards(self, cards: list[TableCard]) -> int:
        if not cards:
            return 0
        col = self.schema_collection()
        for i in range(0, len(cards), self._UPSERT_BATCH):
            chunk = cards[i : i + self._UPSERT_BATCH]
            col.upsert(
                ids=[c.chroma_id() for c in chunk],
                documents=[c.document for c in chunk],
                metadatas=[c.chroma_metadata() for c in chunk],
            )
        log.info("Upserted %d TableCards", len(cards))
        return len(cards)

    def upsert_column_cards(self, cards: list[ColumnCard]) -> int:
        if not cards:
            return 0
        col = self.schema_collection()
        batch = self._UPSERT_BATCH
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
        batch = self._UPSERT_BATCH
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
