"""Enrich ChromaDB schema_cards with natural language descriptions.

Reads table_column_descriptions.json and upserts descriptions as comments
on existing TableCard and ColumnCard entries. Also updates the embedded
document text so semantic search benefits from the descriptions.

Usage:
    uv run python -m scripts.enrich_descriptions --chroma_dir .chroma/
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path

log = logging.getLogger(__name__)

# Match partition table names to their collapsed group
_DATE_SUFFIX_RE = re.compile(r"^(.+?)_?\d{6,8}$")

DESCRIPTIONS_PATH = Path(__file__).resolve().parent.parent / "data" / "table_column_descriptions.json"


def _load_descriptions(path: Path | None = None) -> dict:
    p = path or DESCRIPTIONS_PATH
    with open(p) as f:
        return json.load(f)


def _flatten_column_description(col_info: dict) -> str:
    """Build a concise column description string from the JSON entry."""
    desc = col_info.get("description", "")
    variant_kind = col_info.get("variant_kind", "")
    fields = col_info.get("fields", {})
    access = col_info.get("access_pattern", "")

    parts = []
    if desc:
        parts.append(desc)
    if access:
        parts.append(f"Access: {access}")
    if fields and isinstance(fields, dict):
        # Flatten nested field descriptions
        field_parts = []
        for fname, fval in fields.items():
            if fname.startswith("_"):
                continue
            if isinstance(fval, str):
                field_parts.append(f"{fname}: {fval}")
            elif isinstance(fval, dict):
                # Nested object (e.g., hits.product)
                sub_desc = fval.get("_description", "")
                sub_fields = [
                    f"{fname}.{sf}: {sv}"
                    for sf, sv in fval.items()
                    if not sf.startswith("_") and isinstance(sv, str)
                ]
                if sub_desc:
                    field_parts.append(f"{fname}: {sub_desc}")
                field_parts.extend(sub_fields)
        if field_parts:
            parts.append("Fields: " + "; ".join(field_parts[:15]))
    return " ".join(parts)


def _match_table_key(qualified_name: str, table_keys: dict[str, dict]) -> str | None:
    """Match a qualified table name (possibly with date suffix) to a description key."""
    # Direct match
    if qualified_name in table_keys:
        return qualified_name

    # Try stripping the date suffix for partition tables
    parts = qualified_name.rsplit(".", 1)
    if len(parts) == 2:
        schema_part, table_name = parts[0], parts[1]
        m = _DATE_SUFFIX_RE.match(table_name)
        if m:
            base = m.group(1).rstrip("_")
            # Try DB.SCHEMA.BASE
            candidate = f"{schema_part}.{base}"
            if candidate in table_keys:
                return candidate

    return None


def enrich(chroma_dir: str | None = None, descriptions_path: Path | None = None) -> dict[str, int]:
    """Enrich schema_cards with descriptions. Returns counts."""
    # Lazy imports to avoid import errors in environments without chromadb
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
    from rag_snow_agent.chroma.chroma_store import ChromaStore
    from rag_snow_agent.chroma.schema_cards import ColumnCard, TableCard

    data = _load_descriptions(descriptions_path)
    store = ChromaStore(persist_dir=chroma_dir)
    col = store.schema_collection()

    tables_updated = 0
    columns_updated = 0

    for db_id, db_info in data["databases"].items():
        table_descs = db_info.get("tables", {})

        # --- Enrich tables ---
        table_results = col.get(
            where={"$and": [{"db_id": db_id}, {"object_type": "table"}]},
            include=["metadatas", "documents"],
            limit=100,
        )

        table_cards_to_upsert: list[TableCard] = []
        for tid, meta, doc in zip(
            table_results.get("ids") or [],
            table_results.get("metadatas") or [],
            table_results.get("documents") or [],
        ):
            qname = meta.get("qualified_name", "")
            matched_key = _match_table_key(qname, table_descs)
            if not matched_key:
                continue

            tinfo = table_descs[matched_key]
            desc = tinfo.get("description", "")
            partition = tinfo.get("partition_info", "")
            full_comment = desc
            if partition:
                full_comment += f" {partition}"

            # Build a TableCard with the enriched comment
            # We need to reconstruct enough fields for the document + metadata
            card = TableCard(
                db_id=db_id,
                qualified_name=qname,
                table_type="BASE TABLE",
                comment=full_comment,
                column_names=[
                    c_name for c_name in tinfo.get("columns", {}).keys()
                ],
            )
            table_cards_to_upsert.append(card)
            tables_updated += 1

        if table_cards_to_upsert:
            store.upsert_table_cards(table_cards_to_upsert)

        # --- Enrich columns ---
        col_results = col.get(
            where={"$and": [{"db_id": db_id}, {"object_type": "column"}]},
            include=["metadatas"],
            limit=5000,
        )

        column_cards_to_upsert: list[ColumnCard] = []
        for cid, meta in zip(
            col_results.get("ids") or [],
            col_results.get("metadatas") or [],
        ):
            qname = meta.get("qualified_name", "")
            tqn = meta.get("table_qualified_name", "")
            col_name = qname.rsplit(".", 1)[-1] if "." in qname else qname
            dtype = meta.get("data_type", "VARCHAR")

            # Skip VARIANT_FIELD sub-columns — they get descriptions via parent
            if dtype == "VARIANT_FIELD":
                continue

            # Find the matching table description
            matched_key = _match_table_key(tqn, table_descs)
            if not matched_key:
                continue

            col_descs = table_descs[matched_key].get("columns", {})
            # Strip quotes for matching
            clean_name = col_name.strip('"')
            col_info = col_descs.get(clean_name, {})
            if not col_info:
                continue

            if isinstance(col_info, dict):
                comment = _flatten_column_description(col_info)
            else:
                comment = str(col_info)

            if not comment:
                continue

            card = ColumnCard(
                db_id=db_id,
                qualified_name=qname,
                table_qualified_name=tqn,
                data_type=dtype,
                is_nullable="YES",
                comment=comment,
            )
            column_cards_to_upsert.append(card)
            columns_updated += 1

        if column_cards_to_upsert:
            store.upsert_column_cards(column_cards_to_upsert)

    log.info("Enrichment complete: %d tables, %d columns updated", tables_updated, columns_updated)
    return {"tables_updated": tables_updated, "columns_updated": columns_updated}


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Enrich schema_cards with descriptions")
    parser.add_argument("--chroma_dir", default=".chroma/")
    parser.add_argument("--descriptions", default=None, help="Path to descriptions JSON")
    args = parser.parse_args()

    desc_path = Path(args.descriptions) if args.descriptions else None
    counts = enrich(chroma_dir=args.chroma_dir, descriptions_path=desc_path)
    print(f"\nEnriched: {counts['tables_updated']} tables, {counts['columns_updated']} columns")


if __name__ == "__main__":
    main()
