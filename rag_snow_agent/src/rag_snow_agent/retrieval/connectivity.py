"""Connectivity expansion: join-graph-aware and heuristic fallback."""

from __future__ import annotations

import logging
import re
from collections import defaultdict

import chromadb

from .join_graph import JoinGraph
from .schema_slice import ColumnSlice, SchemaSlice, TableSlice

log = logging.getLogger(__name__)

_JOIN_RE = re.compile(r"(^ID$|_ID$|_KEY$)", re.IGNORECASE)


def _join_key_tokens(columns: list[ColumnSlice]) -> set[str]:
    """Return lowercased join-ish column names for a table."""
    return {c.name.lower() for c in columns if _JOIN_RE.search(c.name)}


def _tables_share_join_key(t1: TableSlice, t2: TableSlice) -> bool:
    keys1 = _join_key_tokens(t1.columns)
    keys2 = _join_key_tokens(t2.columns)
    return bool(keys1 & keys2)


def expand_connectivity(
    schema_slice: SchemaSlice,
    collection: chromadb.Collection,
    max_rounds: int = 1,
) -> SchemaSlice:
    """If selected tables lack shared join keys, try adding a bridge table.

    Heuristic: find a table card in *db_id* whose column_names contain join-ish
    keys overlapping with 2+ selected tables.  Cap: 1 bridge addition per round.
    """
    if len(schema_slice.tables) < 2 or max_rounds < 1:
        return schema_slice

    existing_names = {t.qualified_name for t in schema_slice.tables}

    # Check if any pair is disconnected
    connected_pairs = set()
    tables = schema_slice.tables
    for i in range(len(tables)):
        for j in range(i + 1, len(tables)):
            if _tables_share_join_key(tables[i], tables[j]):
                connected_pairs.add((i, j))

    all_pairs = {(i, j) for i in range(len(tables)) for j in range(i + 1, len(tables))}
    disconnected = all_pairs - connected_pairs
    if not disconnected:
        log.debug("All table pairs share join keys; no expansion needed")
        return schema_slice

    # Gather all join-key tokens across selected tables, keyed by table index
    key_to_tables: dict[str, set[int]] = defaultdict(set)
    for ti, ts in enumerate(tables):
        for k in _join_key_tokens(ts.columns):
            key_to_tables[k].add(ti)

    # Query Chroma for table cards in this db_id
    all_table_cards = collection.get(
        where={"$and": [{"db_id": schema_slice.db_id}, {"object_type": "table"}]},
        include=["documents", "metadatas"],
    )
    card_ids = all_table_cards["ids"] or []
    card_metas = all_table_cards["metadatas"] or []
    card_docs = all_table_cards["documents"] or []

    best_bridge: tuple[str, str, int] | None = None  # (id, qname, overlap_count)

    for cid, meta, doc in zip(card_ids, card_metas, card_docs):
        qname = meta.get("qualified_name", "")
        if qname in existing_names:
            continue
        # Parse column names from doc text ("Columns: A, B, C")
        col_names: set[str] = set()
        for line in (doc or "").split("\n"):
            if line.startswith("Columns:"):
                col_names = {
                    c.strip().lower() for c in line[len("Columns:") :].split(",")
                }
                break
        # Count how many selected-table indices this bridge connects
        bridge_join_keys = {n for n in col_names if _JOIN_RE.search(n)}
        connected_table_indices: set[int] = set()
        for k in bridge_join_keys:
            connected_table_indices |= key_to_tables.get(k, set())
        if len(connected_table_indices) >= 2:
            if best_bridge is None or len(connected_table_indices) > best_bridge[2]:
                best_bridge = (cid, qname, len(connected_table_indices))

    if best_bridge:
        cid, qname, cnt = best_bridge
        log.info(
            "Connectivity expansion: adding bridge table %s (connects %d tables)",
            qname,
            cnt,
        )
        # Fetch columns for this bridge table
        bridge_cols_result = collection.get(
            where={
                "$and": [
                    {"db_id": schema_slice.db_id},
                    {"object_type": "column"},
                    {"table_qualified_name": qname},
                ]
            },
            include=["metadatas"],
        )
        bridge_col_metas = bridge_cols_result["metadatas"] or []
        col_slices = []
        for cm in bridge_col_metas:
            col_name = cm.get("qualified_name", "").rsplit(".", 1)[-1]
            cs = ColumnSlice(
                name=col_name,
                data_type=cm.get("data_type", "VARCHAR"),
                token_estimate=cm.get("token_estimate", 5),
                fused_rank=999,  # low priority
                is_join_key=bool(_JOIN_RE.search(col_name)),
            )
            col_slices.append(cs)

        bridge_ts = TableSlice(
            qualified_name=qname,
            table_token_estimate=5,
            fused_rank=len(schema_slice.tables) + 1,
            columns=col_slices,
        )
        schema_slice.tables.append(bridge_ts)
    else:
        log.debug("No suitable bridge table found")

    return schema_slice


def expand_connectivity_with_join_graph(
    schema_slice: SchemaSlice,
    collection: chromadb.Collection,
    max_depth: int = 3,
    min_confidence: float = 0.5,
    allow_heuristic_fallback: bool = True,
) -> SchemaSlice:
    """Use JoinCards from Chroma to find bridge tables via a real join graph.

    Falls back to the heuristic ``expand_connectivity`` if no JoinCards exist.
    """
    if len(schema_slice.tables) < 2:
        return schema_slice

    # Fetch JoinCards for this db_id
    try:
        join_results = collection.get(
            where={"$and": [{"db_id": schema_slice.db_id}, {"object_type": "join"}]},
            include=["metadatas"],
        )
    except Exception:
        log.debug("Failed to fetch JoinCards; falling back to heuristic")
        if allow_heuristic_fallback:
            return expand_connectivity(schema_slice, collection)
        return schema_slice

    join_metas = join_results.get("metadatas") or []

    if not join_metas:
        log.debug("No JoinCards found for %s", schema_slice.db_id)
        if allow_heuristic_fallback:
            return expand_connectivity(schema_slice, collection)
        return schema_slice

    graph = JoinGraph.from_join_cards(join_metas)
    selected_tables = [t.qualified_name for t in schema_slice.tables]

    bridges = graph.shortest_bridge_tables(
        selected_tables, max_depth=max_depth, min_confidence=min_confidence
    )

    if not bridges:
        log.debug("JoinGraph: all selected tables already connected or no path found")
        return schema_slice

    existing_names = {t.qualified_name for t in schema_slice.tables}

    for bridge_qname in bridges:
        if bridge_qname in existing_names:
            continue

        # Fetch columns for the bridge table from Chroma
        bridge_cols_result = collection.get(
            where={
                "$and": [
                    {"db_id": schema_slice.db_id},
                    {"object_type": "column"},
                    {"table_qualified_name": bridge_qname},
                ]
            },
            include=["metadatas"],
        )
        bridge_col_metas = bridge_cols_result.get("metadatas") or []
        col_slices = []
        for cm in bridge_col_metas:
            col_name = cm.get("qualified_name", "").rsplit(".", 1)[-1]
            cs = ColumnSlice(
                name=col_name,
                data_type=cm.get("data_type", "VARCHAR"),
                token_estimate=cm.get("token_estimate", 5),
                fused_rank=999,
                is_join_key=bool(_JOIN_RE.search(col_name)),
            )
            col_slices.append(cs)

        bridge_ts = TableSlice(
            qualified_name=bridge_qname,
            table_token_estimate=5,
            fused_rank=len(schema_slice.tables) + 1,
            columns=col_slices,
        )
        schema_slice.tables.append(bridge_ts)
        existing_names.add(bridge_qname)
        log.info("JoinGraph expansion: added bridge table %s", bridge_qname)

    return schema_slice
