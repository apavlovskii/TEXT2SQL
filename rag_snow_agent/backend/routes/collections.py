"""Vector DB collections info endpoint."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends

from ..dependencies import get_agent
from ..services.agent_adapter import AgentAdapter

router = APIRouter(tags=["collections"])
log = logging.getLogger(__name__)


@router.get("/api/collections")
def list_collections(agent: AgentAdapter = Depends(get_agent)):
    """Return ChromaDB collection names and item counts."""
    if not agent._chroma_store:
        return []

    try:
        import chromadb
        client = agent._chroma_store.client
        result = []
        for col in client.list_collections():
            result.append({
                "name": col.name,
                "count": col.count(),
                "metadata": col.metadata or {},
            })
        return result
    except Exception as exc:
        log.error("Failed to list collections: %s", exc)
        return []
