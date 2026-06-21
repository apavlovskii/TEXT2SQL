"""Debug CLI for inspecting retrieval results.

Usage:
    uv run python -m rag_snow_agent.retrieval.debug_retrieve \
        --db_id TESTDB --query "total orders by month" \
        --top_k 50 --max_schema_tokens 800
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from collections import defaultdict
from pathlib import Path

import yaml

from ..chroma.chroma_store import ChromaStore
from .budget import classify_column, trim_to_budget
from .connectivity import expand_connectivity, expand_join_graph_neighbors
from .hybrid_retriever import HybridRetriever, ScoredItem
from .schema_slice import ColumnSlice, SchemaSlice, TableSlice

log = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).resolve().parents[4] / "config" / "defaults.yaml"


def _load_config() -> dict:
    if _CONFIG_PATH.exists():
        return yaml.safe_load(_CONFIG_PATH.read_text()) or {}
    return {}


_VARIANT_FIELD_PARENT_RE = re.compile(r'^"?([^"]+)"?:(.+)$')


_DESC_MAX_FIELDS = 12   # max nested fields (per table) that get an inline description
_DESC_TRUNC = 220       # truncate each description in the slice to keep the token budget


def _lex_tokens(s: str) -> set[str]:
    return {t for t in re.split(r"[^a-z0-9]+", (s or "").lower()) if len(t) > 2}


def _enrich_variant_fields(
    schema_slice: SchemaSlice,
    collection,
    db_id: str,
    query: str = "",
) -> None:
    """Populate ``variant_fields`` / ``variant_kind`` and attach descriptions for
    the most query-relevant nested fields on each VARIANT column.

    Queries ChromaDB for VARIANT_FIELD entries belonging to each table in the
    slice, groups them by parent VARIANT column, and attaches the sub-field
    names.  Columns with known scalar sub-fields are reclassified as OBJECT
    (direct colon access) rather than ARRAY (needs FLATTEN). Descriptions
    (``comment``) for the top fields most relevant to *query* are attached so the
    model knows what each sub-field means; selection is lexical (no embeddings).
    """
    qtok = _lex_tokens(query)
    for ts in schema_slice.tables:
        # Fetch all VARIANT_FIELD entries for this table
        try:
            vf_results = collection.get(
                where={
                    "$and": [
                        {"db_id": db_id},
                        {"object_type": "column"},
                        {"data_type": "VARIANT_FIELD"},
                        {"table_qualified_name": ts.qualified_name},
                    ]
                },
                include=["metadatas"],
            )
        except Exception:
            log.debug("Failed to fetch VARIANT_FIELDs for %s", ts.qualified_name)
            continue

        if not vf_results.get("metadatas"):
            continue

        # Group sub-field names by parent column:  "totals":pageviews → totals → [pageviews]
        # Also track whether the sub-fields come from array elements or direct objects
        fields_by_parent: dict[str, list[str]] = defaultdict(list)
        comment_by_parent_field: dict[tuple[str, str], str] = {}
        is_array_element: dict[str, bool] = {}
        for meta in vf_results["metadatas"]:
            # Derive the nested key by stripping the table prefix, NOT rsplit('.')
            # — nested-of-nested paths (e.g. "hits":product.productRevenue) contain
            # dots that rsplit would truncate.
            qn = meta.get("qualified_name", "")
            tqn = meta.get("table_qualified_name", "")
            if tqn and qn.startswith(tqn + "."):
                col_name = qn[len(tqn) + 1:]
            else:
                col_name = qn.rsplit(".", 1)[-1]
            comment = meta.get("comment", "") or ""
            m = _VARIANT_FIELD_PARENT_RE.match(col_name)
            if m:
                parent = m.group(1)   # e.g. "totals" or "hits"
                field = m.group(2)    # e.g. "pageviews" or "product.productRevenue"
                fields_by_parent[parent].append(field)
                if comment.strip():
                    comment_by_parent_field[(parent, field)] = comment.strip()
                cl = comment.lower()
                if any(k in cl for k in ("array element", "flatten", "repeated array")):
                    is_array_element[parent] = True

        if not fields_by_parent:
            continue

        # Rank described fields by lexical relevance to the query and keep the
        # top-N (per table) to receive an inline description in the slice.
        described = [
            (p, f, c) for (p, f), c in comment_by_parent_field.items()
        ]

        def _score(item):
            p, f, c = item
            cl = c.lower()
            # Demote fields with no real data (demo placeholders / redacted) so the
            # limited description budget goes to fields that carry usable values.
            if "not populated" in cl or "redacted" in cl or "no longer supported" in cl:
                return -1
            return len(qtok & _lex_tokens(f"{p} {f} {c}"))

        described.sort(key=lambda it: (-_score(it), it[0], it[1]))
        selected: dict[str, dict[str, str]] = defaultdict(dict)
        for p, f, c in described[:_DESC_MAX_FIELDS]:
            selected[p][f] = c[:_DESC_TRUNC]

        # Attach to matching ColumnSlice objects
        for cs in ts.columns:
            canon = (cs.original_name or cs.name).strip('"')
            if canon in fields_by_parent:
                # Order names so the most query-relevant appear first (within the
                # [:8] cap shown in the slice), described fields ahead of the rest.
                names = sorted(set(fields_by_parent[canon]))
                desc_for_col = selected.get(canon, {})
                names.sort(key=lambda f: (f not in desc_for_col,
                                          -len(qtok & _lex_tokens(f)), f))
                cs.variant_fields = names
                if desc_for_col:
                    cs.variant_field_descriptions = desc_for_col
                if is_array_element.get(canon):
                    # Sub-fields from array elements → ARRAY that needs FLATTEN
                    cs.variant_kind = "ARRAY"
                    log.debug(
                        "Enriched %s.%s with %d VARIANT sub-fields (kind=ARRAY, array elements)",
                        ts.qualified_name, canon, len(cs.variant_fields),
                    )
                else:
                    # Sub-fields from direct object access → OBJECT
                    cs.variant_kind = "OBJECT"
                    log.debug(
                        "Enriched %s.%s with %d VARIANT sub-fields (kind=OBJECT)",
                        ts.qualified_name, canon, len(cs.variant_fields),
                    )


def build_schema_slice(
    retriever: HybridRetriever,
    query: str,
    db_id: str,
    top_k_tables: int,
    top_k_columns: int,
    max_schema_tokens: int,
    max_tables: int | None = None,
    max_columns_per_table: int | None = None,
    connectivity_rounds: int = 1,
) -> tuple[SchemaSlice, list[ScoredItem], list[ScoredItem]]:
    """Run full retrieval pipeline and return (slice, table_items, column_items)."""
    table_items = retriever.retrieve_tables(query, db_id, top_k=top_k_tables)
    column_items = retriever.retrieve_columns(query, db_id, top_k=top_k_columns)

    # Group columns by table
    cols_by_table: dict[str, list[ScoredItem]] = defaultdict(list)
    for ci in column_items:
        tqn = ci.metadata.get("table_qualified_name", "")
        cols_by_table[tqn].append(ci)

    # Build TableSlices for each retrieved table
    table_slices: list[TableSlice] = []
    for ti in table_items:
        qname = ti.qualified_name
        col_slices = []
        for ci in cols_by_table.get(qname, []):
            col_name = ci.qualified_name.rsplit(".", 1)[-1]
            raw_dtype = ci.metadata.get("data_type", "VARCHAR")
            # Skip VARIANT_FIELD sub-columns — their info is carried by
            # the parent VARIANT column's variant_fields list instead.
            if raw_dtype.upper() == "VARIANT_FIELD":
                continue
            cs = ColumnSlice(
                name=col_name,
                data_type=raw_dtype,
                comment=ci.metadata.get("comment") or None,
                original_name=col_name,  # preserves exact case from qualified_name
                token_estimate=ci.metadata.get("token_estimate", 5),
                fused_rank=ci.fused_rank,
                is_variant=raw_dtype.upper() in ("VARIANT", "OBJECT", "ARRAY"),
            )
            classify_column(cs)
            col_slices.append(cs)

        # If no columns retrieved for this table, add columns from Chroma directly
        if not col_slices:
            all_cols = retriever.collection.get(
                where={
                    "$and": [
                        {"db_id": db_id},
                        {"object_type": "column"},
                        {"table_qualified_name": qname},
                    ]
                },
                include=["metadatas"],
            )
            for meta in all_cols["metadatas"] or []:
                col_name = meta.get("qualified_name", "").rsplit(".", 1)[-1]
                raw_dtype = meta.get("data_type", "VARCHAR")
                if raw_dtype.upper() == "VARIANT_FIELD":
                    continue
                cs = ColumnSlice(
                    name=col_name,
                    data_type=raw_dtype,
                    comment=meta.get("comment") or None,
                    original_name=col_name,
                    token_estimate=meta.get("token_estimate", 5),
                    fused_rank=999,
                    is_variant=raw_dtype.upper() in ("VARIANT", "OBJECT", "ARRAY"),
                )
                classify_column(cs)
                col_slices.append(cs)

        ts = TableSlice(
            qualified_name=qname,
            comment=ti.metadata.get("comment") or None,
            table_token_estimate=ti.metadata.get("token_estimate", 10),
            fused_rank=ti.fused_rank,
            columns=col_slices,
        )
        table_slices.append(ts)

    schema_slice = SchemaSlice(db_id=db_id, tables=table_slices)

    # Enrich VARIANT columns with known sub-field paths from ChromaDB
    _enrich_variant_fields(schema_slice, retriever.collection, db_id, query=query)

    # Connectivity expansion
    if connectivity_rounds > 0:
        expand_connectivity(
            schema_slice, retriever.collection, max_rounds=connectivity_rounds
        )

    # Join-graph neighbor expansion: add geo/location tables when question needs them
    expand_join_graph_neighbors(schema_slice, retriever.collection, query)

    # Budget trimming
    trim_to_budget(
        schema_slice,
        max_schema_tokens=max_schema_tokens,
        max_tables=max_tables,
        max_columns_per_table=max_columns_per_table,
    )

    return schema_slice, table_items, column_items


def main(argv: list[str] | None = None) -> None:
    cfg = _load_config().get("retrieval", {})

    parser = argparse.ArgumentParser(description="Debug schema retrieval")
    parser.add_argument("--db_id", required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--top_k", type=int, default=cfg.get("top_k_tables", 8))
    parser.add_argument("--top_k_columns", type=int, default=cfg.get("top_k_columns", 25))
    parser.add_argument(
        "--max_schema_tokens", type=int, default=cfg.get("max_schema_tokens", 2500)
    )
    parser.add_argument("--max_tables", type=int, default=None)
    parser.add_argument("--max_columns_per_table", type=int, default=None)
    parser.add_argument("--chroma_dir", default=None)
    parser.add_argument("--rrf_k", type=int, default=cfg.get("rrf_k", 60))
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    store = ChromaStore(persist_dir=args.chroma_dir)
    collection = store.schema_collection()
    retriever = HybridRetriever(collection, rrf_k=args.rrf_k)

    schema_slice, table_items, column_items = build_schema_slice(
        retriever=retriever,
        query=args.query,
        db_id=args.db_id,
        top_k_tables=args.top_k,
        top_k_columns=args.top_k_columns,
        max_schema_tokens=args.max_schema_tokens,
        max_tables=args.max_tables,
        max_columns_per_table=args.max_columns_per_table,
    )

    # ── print results ─────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"Query:   {args.query}")
    print(f"DB:      {args.db_id}")
    print(f"Budget:  {args.max_schema_tokens} tokens")
    print(f"{'='*60}")

    print(f"\n--- Top tables (retrieved {len(table_items)}) ---")
    for ti in table_items[:20]:
        print(
            f"  rank={ti.fused_rank:3d}  dense={ti.dense_rank:3d}  "
            f"lex={ti.lexical_rank:3d}  rrf={ti.rrf_score:.4f}  "
            f"{ti.qualified_name}"
        )

    print(f"\n--- SchemaSlice ---")
    print(schema_slice.summary())
    for ts in schema_slice.tables:
        print(f"\n  TABLE {ts.qualified_name}  (rank={ts.fused_rank}, ~{ts.token_estimate} tok)")
        for col in ts.columns:
            flags = []
            if col.is_join_key:
                flags.append("JK")
            if col.is_time_column:
                flags.append("T")
            flag_str = f" [{','.join(flags)}]" if flags else ""
            print(f"    {col.name:30s} {col.data_type:20s} rank={col.fused_rank}{flag_str}")

    print(f"\n--- Formatted prompt text ({schema_slice.token_estimate} tokens) ---")
    print(schema_slice.format_for_prompt())
    print()


if __name__ == "__main__":
    main()
