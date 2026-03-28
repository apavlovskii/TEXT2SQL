"""CLI: python -m rag_snow_agent.chroma.ingest_syntax

Ingest Snowflake SQL syntax reference into ChromaDB for retrieval
during query generation.
"""

from __future__ import annotations

import argparse
import logging

from .chroma_store import ChromaStore
from .snowflake_syntax import SnowflakeSyntaxStore, build_all_syntax_chunks

log = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Ingest Snowflake SQL syntax reference into ChromaDB"
    )
    parser.add_argument("--chroma_dir", default=None)
    parser.add_argument("--max_chunk_tokens", type=int, default=600)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    chunks = build_all_syntax_chunks(max_chunk_tokens=args.max_chunk_tokens)
    log.info("Built %d syntax chunks across %d topics", len(chunks), len(set(c.topic for c in chunks)))

    store = ChromaStore(persist_dir=args.chroma_dir)
    syntax_store = SnowflakeSyntaxStore(store)
    count = syntax_store.upsert_chunks(chunks)

    print(f"\nIngested {count} Snowflake syntax chunks into '{syntax_store.collection().name}' collection")
    print(f"Topics: {', '.join(sorted(set(c.topic for c in chunks)))}")
    print(f"Total items in collection: {syntax_store.count()}")


if __name__ == "__main__":
    main()
