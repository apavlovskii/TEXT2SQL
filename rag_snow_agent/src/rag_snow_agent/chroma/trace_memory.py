"""Trace memory collection for storing successful solution traces."""

import logging
from pathlib import Path

import chromadb

from .chroma_store import DEFAULT_CHROMA_DIR

log = logging.getLogger(__name__)

TRACE_COLLECTION = "trace_memory"


class TraceMemoryStore:
    def __init__(self, persist_dir=None):
        self.persist_dir = Path(persist_dir) if persist_dir else DEFAULT_CHROMA_DIR
        self.persist_dir.mkdir(parents=True, exist_ok=True)
        self.client = chromadb.PersistentClient(path=str(self.persist_dir))

    def collection(self):
        return self.client.get_or_create_collection(
            name=TRACE_COLLECTION,
            metadata={"hnsw:space": "cosine"},
        )

    def upsert_trace(self, trace_record: dict) -> None:
        col = self.collection()
        doc = (
            trace_record.get("instruction_summary", "")
            + "\n"
            + trace_record.get("plan_summary", "")
        )
        col.upsert(
            ids=[trace_record["trace_id"]],
            documents=[doc],
            metadatas=[
                {
                    "db_id": trace_record["db_id"],
                    "instance_id": trace_record["instance_id"],
                    "tables_used": ",".join(trace_record.get("tables_used", [])),
                    "token_estimate": trace_record.get("token_estimate", 0),
                }
            ],
        )

    def query_traces(
        self, db_id: str, instruction: str, top_k: int = 3
    ) -> list[dict]:
        col = self.collection()
        try:
            results = col.query(
                query_texts=[instruction],
                n_results=top_k,
                where={"db_id": db_id},
                include=["metadatas", "documents", "distances"],
            )
        except Exception:
            log.debug("Trace query failed for db_id=%s", db_id, exc_info=True)
            return []
        traces = []
        ids = results.get("ids", [[]])[0]
        docs = results.get("documents", [[]])[0]
        metas = results.get("metadatas", [[]])[0]
        dists = results.get("distances", [[]])[0]
        for i, tid in enumerate(ids):
            traces.append(
                {
                    "trace_id": tid,
                    "document": docs[i] if i < len(docs) else "",
                    "metadata": metas[i] if i < len(metas) else {},
                    "distance": dists[i] if i < len(dists) else 1.0,
                }
            )
        return traces

    def delete_all_for_db(self, db_id: str) -> None:
        col = self.collection()
        results = col.get(where={"db_id": db_id})
        if results["ids"]:
            col.delete(ids=results["ids"])
