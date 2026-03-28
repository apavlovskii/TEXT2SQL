"""CLI to ingest gold SQL JOIN conditions into ChromaDB.

Usage:
    python -m rag_snow_agent.chroma.ingest_gold_joins \
        --gold_dir Spider2/spider2-snow/evaluation_suite/gold/sql/ \
        --credentials snowflake_credentials.json
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from ..chroma.chroma_store import ChromaStore
from ..chroma.schema_cards import JoinCard
from .gold_joins import extract_joins_from_gold_sqls

log = logging.getLogger(__name__)


def _infer_db_id(table_name: str) -> str:
    """Infer db_id from a fully qualified table name (DB.SCHEMA.TABLE -> DB)."""
    parts = table_name.split(".")
    if len(parts) >= 3:
        return parts[0]
    return table_name


def ingest_gold_joins(
    gold_sql_dir: str | Path,
    chroma_dir: str | Path | None = None,
) -> int:
    """Extract joins from gold SQLs and upsert into ChromaDB.

    Returns the number of JoinCards upserted.
    """
    raw_joins = extract_joins_from_gold_sqls(gold_sql_dir)
    if not raw_joins:
        log.warning("No joins extracted from %s", gold_sql_dir)
        return 0

    # Deduplicate by (left_table, left_column, right_table, right_column)
    seen: set[tuple[str, str, str, str]] = set()
    cards: list[JoinCard] = []
    for j in raw_joins:
        key = (
            j["left_table"].upper(),
            j["left_column"].upper(),
            j["right_table"].upper(),
            j["right_column"].upper(),
        )
        if key in seen:
            continue
        seen.add(key)

        db_id = _infer_db_id(j["left_table"])
        cards.append(
            JoinCard(
                db_id=db_id,
                left_table=j["left_table"],
                left_column=j["left_column"],
                right_table=j["right_table"],
                right_column=j["right_column"],
                confidence=1.0,
                source="gold_sql",
            )
        )

    store = ChromaStore(persist_dir=chroma_dir)
    count = store.upsert_join_cards(cards)
    log.info("Upserted %d gold join card(s)", count)
    return count


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Ingest gold SQL JOIN conditions into ChromaDB"
    )
    parser.add_argument(
        "--gold_dir",
        required=True,
        help="Directory containing gold .sql files",
    )
    parser.add_argument(
        "--chroma_dir",
        default=None,
        help="ChromaDB persist directory (default: .chroma)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    count = ingest_gold_joins(args.gold_dir, args.chroma_dir)
    print(f"Ingested {count} gold join card(s) from {args.gold_dir}")


if __name__ == "__main__":
    main()
