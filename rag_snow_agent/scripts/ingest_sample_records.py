"""Ingest sample_records.json into the ChromaDB sample_records collection.

Usage:
    python -m rag_snow_agent.scripts.ingest_sample_records \
        --data_path rag_snow_agent/data/sample_records.json \
        --chroma_dir rag_snow_agent/.chroma
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data_path",
        default="rag_snow_agent/data/sample_records.json",
        help="Path to sample_records.json",
    )
    parser.add_argument(
        "--chroma_dir",
        default=None,
        help="ChromaDB persistence directory (default: auto-detect)",
    )
    args = parser.parse_args()

    data_path = Path(args.data_path)
    if not data_path.exists():
        print(f"ERROR: data file not found: {data_path}", file=sys.stderr)
        sys.exit(1)

    data = json.loads(data_path.read_text())

    from rag_snow_agent.chroma.chroma_store import ChromaStore
    from rag_snow_agent.chroma.sample_records import SampleRecordCard, SampleRecordStore

    store = ChromaStore(persist_dir=args.chroma_dir)
    sample_store = SampleRecordStore(store)

    cards: list[SampleRecordCard] = []
    for db_id, tables in data.items():
        for table_fqn, rows in tables.items():
            cards.append(
                SampleRecordCard(
                    db_id=db_id,
                    table_fqn=table_fqn,
                    rows=rows,
                )
            )
            log.info("  %s: %d rows", table_fqn, len(rows))

    count = sample_store.upsert_samples(cards)
    log.info("Ingested %d sample record cards into ChromaDB", count)

    # Verify
    col = sample_store.collection()
    total = col.count()
    log.info("Collection '%s' now has %d documents", col.name, total)


if __name__ == "__main__":
    main()
