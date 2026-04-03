"""ChromaDB collection for table sample records."""

from __future__ import annotations

import json
import logging
from typing import Any

import chromadb
import tiktoken
from pydantic import BaseModel, computed_field

from .chroma_store import ChromaStore

log = logging.getLogger(__name__)

SAMPLE_COLLECTION = "sample_records"

_enc = tiktoken.get_encoding("cl100k_base")


def _token_count(text: str) -> int:
    return len(_enc.encode(text))


def _truncate_value(v: Any, max_len: int = 80) -> Any:
    """Truncate a single value for compact display."""
    if isinstance(v, str) and len(v) > max_len:
        return v[:max_len] + "..."
    if isinstance(v, dict):
        return {k: _truncate_value(val, max_len=40) for k, val in list(v.items())[:6]}
    if isinstance(v, list) and len(v) > 2:
        return [_truncate_value(item, max_len=40) for item in v[:2]] + ["..."]
    if isinstance(v, list):
        return [_truncate_value(item, max_len=40) for item in v]
    return v


def _format_rows(rows: list[dict], max_rows: int = 2) -> str:
    """Format sample rows into compact text."""
    lines = []
    for i, row in enumerate(rows[:max_rows]):
        truncated = {k: _truncate_value(v) for k, v in row.items()}
        lines.append(f"Row {i + 1}: {json.dumps(truncated, default=str, ensure_ascii=False)}")
    return "\n".join(lines)


class SampleRecordCard(BaseModel):
    """One card per table, containing sample rows."""

    db_id: str
    table_fqn: str  # DB.SCHEMA.TABLE
    rows: list[dict]

    @computed_field  # type: ignore[prop-decorator]
    @property
    def document(self) -> str:
        header = f"Sample data from {self.table_fqn}:"
        body = _format_rows(self.rows, max_rows=2)
        return f"{header}\n{body}"

    def chroma_id(self) -> str:
        return f"sample:{self.table_fqn}"

    def chroma_metadata(self) -> dict:
        return {
            "db_id": self.db_id,
            "table_fqn": self.table_fqn,
            "object_type": "sample_records",
            "row_count": len(self.rows),
        }


class SampleRecordStore:
    """Store and retrieve table sample records from ChromaDB."""

    def __init__(self, chroma_store: ChromaStore) -> None:
        self._chroma_store = chroma_store

    def collection(self) -> chromadb.Collection:
        kwargs: dict = {
            "name": SAMPLE_COLLECTION,
            "metadata": {"hnsw:space": "cosine"},
        }
        if self._chroma_store._embedding_fn is not None:
            kwargs["embedding_function"] = self._chroma_store._embedding_fn
        return self._chroma_store.client.get_or_create_collection(**kwargs)

    def upsert_samples(self, cards: list[SampleRecordCard]) -> int:
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
        log.info("Upserted %d SampleRecordCards", len(cards))
        return len(cards)

    def get_samples_for_tables(
        self, db_id: str, table_fqns: list[str]
    ) -> dict[str, list[dict]]:
        """Retrieve sample rows for specific tables by FQN.

        Returns {table_fqn: [row_dicts]} for tables that have sample data.
        """
        if not table_fqns:
            return {}

        col = self.collection()
        # Fetch all samples for this db_id
        try:
            results = col.get(
                where={"db_id": db_id},
                include=["metadatas", "documents"],
            )
        except Exception:
            log.debug("Sample records get failed for db_id=%s", db_id, exc_info=True)
            return {}

        # Build FQN -> document mapping
        fqn_set = set(table_fqns)
        samples: dict[str, list[dict]] = {}
        ids = results.get("ids", [])
        metas = results.get("metadatas", [])

        for i, mid in enumerate(ids):
            meta = metas[i] if i < len(metas) else {}
            fqn = meta.get("table_fqn", "")
            if fqn in fqn_set:
                # Parse the stored rows from the card's chroma_id to find the original
                # We need the raw rows, not the document text — retrieve from JSON
                samples[fqn] = []  # placeholder, filled by _load_raw_rows

        # For actual row data, we need to re-parse from the stored documents
        # or load from the JSON file. Since documents contain formatted text,
        # we store raw rows in a local cache when ingesting.
        # For now, return the document text keyed by FQN for prompt injection.
        docs = results.get("documents", [])
        result: dict[str, list[dict]] = {}
        for i, mid in enumerate(ids):
            meta = metas[i] if i < len(metas) else {}
            fqn = meta.get("table_fqn", "")
            if fqn in fqn_set:
                result[fqn] = docs[i] if i < len(docs) else ""  # type: ignore[assignment]

        return result  # type: ignore[return-value]

    def get_sample_context_for_tables(
        self, db_id: str, table_fqns: list[str]
    ) -> list[tuple[str, str]]:
        """Retrieve (table_fqn, document_text) pairs for matching tables.

        For partitioned tables (e.g. GA_SESSIONS_20170101), matches by
        table-name prefix so that a stored sample from any partition
        satisfies a request for any other partition of the same table.
        """
        if not table_fqns:
            return []

        col = self.collection()
        try:
            results = col.get(
                where={"db_id": db_id},
                include=["metadatas", "documents"],
            )
        except Exception:
            log.debug("Sample records get failed for db_id=%s", db_id, exc_info=True)
            return []

        # Build prefix set: "DB.SCHEMA.TABLE_NAME" → strip trailing digits
        # e.g. "GA4.GA4_OBFUSCATED_SAMPLE_ECOMMERCE.EVENTS_20210125" → "...EVENTS_"
        def _table_prefix(fqn: str) -> str:
            import re
            return re.sub(r"\d+$", "", fqn)

        requested_prefixes: set[str] = set()
        for fqn in table_fqns:
            requested_prefixes.add(fqn)            # exact match
            requested_prefixes.add(_table_prefix(fqn))  # prefix match

        pairs: list[tuple[str, str]] = []
        seen_prefixes: set[str] = set()  # avoid duplicate partition matches
        ids = results.get("ids", [])
        metas = results.get("metadatas", [])
        docs = results.get("documents", [])

        for i in range(len(ids)):
            meta = metas[i] if i < len(metas) else {}
            fqn = meta.get("table_fqn", "")
            prefix = _table_prefix(fqn)
            if (fqn in requested_prefixes or prefix in requested_prefixes) and prefix not in seen_prefixes:
                doc = docs[i] if i < len(docs) else ""
                pairs.append((fqn, doc))
                seen_prefixes.add(prefix)

        return pairs


def build_sample_context(
    table_docs: list[tuple[str, str]],
    max_tokens: int = 800,
) -> str:
    """Format retrieved sample record documents into prompt context text.

    *table_docs* is a list of (table_fqn, document_text) pairs from
    ``SampleRecordStore.get_sample_context_for_tables()``.

    Returns an empty string if no data is available or budget is zero.
    """
    if not table_docs:
        return ""

    header = "Sample data from relevant tables (use to understand value formats):\n"
    budget = max_tokens - _token_count(header) - 5
    if budget <= 0:
        return ""

    parts = [header]
    for _fqn, doc in table_docs:
        entry = doc + "\n"
        entry_tokens = _token_count(entry)
        if entry_tokens > budget:
            # Try with just 1 row (take first line = header + first row)
            lines = doc.split("\n")
            if len(lines) >= 2:
                entry = "\n".join(lines[:2]) + "\n"
                entry_tokens = _token_count(entry)
            if entry_tokens > budget:
                continue
        parts.append(entry)
        budget -= entry_tokens

    if len(parts) <= 1:
        return ""
    return "".join(parts).rstrip("\n")
