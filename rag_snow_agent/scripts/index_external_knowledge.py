"""Index external knowledge markdown files into ChromaDB.

Reads external knowledge files referenced by Spider2-Snow instances
and stores them as chunks in the schema_cards collection with
object_type='external_doc'.

Usage:
    uv run python scripts/index_external_knowledge.py --chroma_dir .chroma/
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

_SRC_DIR = Path(__file__).resolve().parent.parent / "src"
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

# Known locations for external knowledge docs
_DOC_DIRS = [
    Path(__file__).resolve().parent.parent.parent / "ReFoRCE" / "spider2-snow" / "resource" / "documents",
    Path(__file__).resolve().parent.parent.parent / "Spider2" / "spider2-snow" / "resource" / "documents",
]

# Map external_knowledge filename to the db_ids that use it
_DOC_DB_MAP = {
    "google_analytics_sample.ga_sessions.md": ["GA360"],
    "ga360_hits.eCommerceAction.action_type.md": ["GA360"],
    "ga4_obfuscated_sample_ecommerce.events.md": ["GA4"],
    "patents_info.md": ["PATENTS", "PATENTS_GOOGLE"],
    "sliding_windows_calculation_cpc.md": ["PATENTS"],
    "lang_and_ext.md": ["GITHUB_REPOS"],
    "functions_st_distance.md": ["NOAA_DATA"],
    "functions_st_dwithin.md": ["GEO_OPENSTREETMAP", "NEW_YORK_NOAA"],
    "functions_st_within.md": ["NEW_YORK_CITIBIKE_1", "NOAA_DATA_PLUS", "NOAA_GLOBAL_FORECAST_SYSTEM"],
    "functions_st_intersects.md": ["GEO_OPENSTREETMAP_BOUNDARIES"],
    "functions_st_intersects_polygon_line.md": ["GEO_OPENSTREETMAP"],
    "functions_st_contains.md": ["NEW_YORK_GEO"],
    "forward_backward_citation.md": ["PATENTSVIEW"],
    "avg_vulnerable_weights.md": ["CENSUS_BUREAU_ACS_2"],
    "total_vulnerable_weights.md": ["CENSUS_BUREAU_ACS_2"],
}


def _find_doc(filename: str) -> Path | None:
    """Find the external knowledge file in known locations."""
    for doc_dir in _DOC_DIRS:
        path = doc_dir / filename
        if path.exists():
            return path
    return None


def _chunk_markdown(text: str, max_chunk_chars: int = 2000) -> list[str]:
    """Split markdown into chunks at section boundaries."""
    lines = text.split("\n")
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for line in lines:
        # Split at headers or when chunk gets too large
        if line.startswith("##") and current_len > 200:
            chunks.append("\n".join(current))
            current = []
            current_len = 0

        current.append(line)
        current_len += len(line) + 1

        if current_len >= max_chunk_chars:
            chunks.append("\n".join(current))
            current = []
            current_len = 0

    if current:
        chunks.append("\n".join(current))

    return [c.strip() for c in chunks if c.strip()]


def index_documents(chroma_dir: str = ".chroma/") -> dict[str, int]:
    """Index all external knowledge documents into ChromaDB."""
    from rag_snow_agent.chroma.chroma_store import ChromaStore

    store = ChromaStore(persist_dir=chroma_dir)
    col = store.schema_collection()

    total_chunks = 0
    total_docs = 0

    for filename, db_ids in _DOC_DB_MAP.items():
        path = _find_doc(filename)
        if not path:
            log.warning("Document not found: %s", filename)
            continue

        text = path.read_text()
        chunks = _chunk_markdown(text)
        log.info("Indexing %s: %d chunks for %s", filename, len(chunks), db_ids)

        ids = []
        documents = []
        metadatas = []

        for i, chunk in enumerate(chunks):
            for db_id in db_ids:
                chunk_id = f"doc:{db_id}:{filename}:{i}"
                ids.append(chunk_id)
                documents.append(f"External knowledge: {filename}\n\n{chunk}")
                metadatas.append({
                    "db_id": db_id,
                    "object_type": "external_doc",
                    "qualified_name": filename,
                    "source": "external_knowledge",
                    "token_estimate": len(chunk) // 4,
                    "chunk_index": i,
                    "total_chunks": len(chunks),
                    "filename": filename,
                })

        # Upsert in batches
        batch_size = 50
        for start in range(0, len(ids), batch_size):
            end = start + batch_size
            col.upsert(
                ids=ids[start:end],
                documents=documents[start:end],
                metadatas=metadatas[start:end],
            )

        total_chunks += len(ids)
        total_docs += 1

    log.info("Indexed %d documents, %d total chunks", total_docs, total_chunks)
    return {"documents": total_docs, "chunks": total_chunks}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chroma_dir", default=".chroma/")
    args = parser.parse_args()

    result = index_documents(args.chroma_dir)
    print(f"\nIndexed: {result['documents']} documents, {result['chunks']} chunks")


if __name__ == "__main__":
    main()
