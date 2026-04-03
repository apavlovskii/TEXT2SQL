"""Debug CLI: inspect the semantic profile stored in ChromaDB for a database.

Usage:
    python -m rag_snow_agent.semantic_layer.debug_semantic_profile --db_id GA360
"""

from __future__ import annotations

import argparse
import logging
import sys

from ..chroma.chroma_store import ChromaStore
from .store import SEMANTIC_COLLECTION, SemanticLayerStore


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Debug: inspect semantic profile in ChromaDB"
    )
    parser.add_argument("--db_id", required=True, help="Database identifier")
    parser.add_argument("--chroma_dir", default=None, help="ChromaDB persist directory")
    parser.add_argument(
        "--query", default=None, help="Optional query to test retrieval"
    )
    parser.add_argument("--top_k", type=int, default=10, help="Number of results")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    chroma = ChromaStore(persist_dir=args.chroma_dir)
    sem_store = SemanticLayerStore(chroma)

    # Show collection stats
    try:
        col = sem_store.collection()
        total = col.count()
        print(f"\nSemantic cards collection: {total} total cards")

        # Get all cards for this db_id
        results = col.get(
            where={"db_id": args.db_id},
            include=["metadatas", "documents"],
        )
        db_count = len(results.get("ids", []))
        print(f"Cards for {args.db_id}: {db_count}")

        if db_count > 0:
            # Count by fact_type
            type_counts: dict[str, int] = {}
            for meta in results.get("metadatas", []):
                ft = meta.get("fact_type", "unknown")
                type_counts[ft] = type_counts.get(ft, 0) + 1

            print("\nBy fact_type:")
            for ft, count in sorted(type_counts.items()):
                print(f"  {ft}: {count}")

            # Show first few documents
            print("\nSample cards:")
            docs = results.get("documents", [])
            for doc in docs[:5]:
                print(f"  {doc[:120]}")

    except Exception as e:
        print(f"Error accessing collection: {e}")
        sys.exit(1)

    # Optional query test
    if args.query:
        print(f"\n--- Query: '{args.query}' (top {args.top_k}) ---")
        cards = sem_store.query_semantic_cards(
            args.db_id, args.query, top_k=args.top_k
        )
        for card in cards:
            print(
                f"  [{card['metadata'].get('fact_type', '?')}] "
                f"{card['metadata'].get('subject', '?')} "
                f"(dist={card.get('distance', '?'):.3f})"
            )


if __name__ == "__main__":
    main()
