"""Retrieve semantic context for prompt injection."""

from __future__ import annotations

import logging

from ..semantic_layer.store import SemanticLayerStore

log = logging.getLogger(__name__)


def retrieve_semantic_context(
    db_id: str,
    instruction: str,
    chroma_store,
    top_k: int = 8,
) -> str:
    """Query semantic_cards collection and return compact text for prompt injection.

    Returns an empty string if no semantic cards are found or on error.
    """
    try:
        sem_store = SemanticLayerStore(chroma_store)
        cards = sem_store.query_semantic_cards(db_id, instruction, top_k=top_k)
    except Exception:
        log.debug(
            "Semantic retrieval failed for db_id=%s", db_id, exc_info=True
        )
        return ""

    if not cards:
        return ""

    lines = ["Semantic context:"]
    for card in cards:
        meta = card.get("metadata", {})
        fact_type = meta.get("fact_type", "")
        subject = meta.get("subject", "")
        confidence = meta.get("confidence", 0)
        line = f"- [{fact_type}] {subject} (conf={confidence})"
        lines.append(line)

    return "\n".join(lines)
