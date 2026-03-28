"""CLI: python -m rag_snow_agent.retrieval.debug_join_graph --db_id TESTDB --tables T1 T2 T3

Prints join graph stats, paths between tables, and bridge tables.
"""

from __future__ import annotations

import argparse
import logging
import sys

from ..chroma.chroma_store import ChromaStore
from .join_graph import JoinGraph


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Debug join graph for a database"
    )
    parser.add_argument("--db_id", required=True, help="Database identifier")
    parser.add_argument(
        "--tables",
        nargs="+",
        default=[],
        help="Selected tables to check connectivity for",
    )
    parser.add_argument(
        "--chroma_dir",
        default=None,
        help="ChromaDB persistence directory",
    )
    parser.add_argument(
        "--max_depth",
        type=int,
        default=3,
        help="Max BFS depth for join paths",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    store = ChromaStore(persist_dir=args.chroma_dir)
    collection = store.schema_collection()

    # Fetch JoinCards
    try:
        join_results = collection.get(
            where={"$and": [{"db_id": args.db_id}, {"object_type": "join"}]},
            include=["metadatas"],
        )
    except Exception as exc:
        print(f"Error fetching JoinCards: {exc}", file=sys.stderr)
        sys.exit(1)

    join_metas = join_results.get("metadatas") or []
    print(f"\nJoin Graph for {args.db_id}")
    print(f"  Total join edges: {len(join_metas)}")

    if not join_metas:
        print("  No JoinCards found.")
        return

    graph = JoinGraph.from_join_cards(join_metas)
    print(f"  Tables with edges: {len(graph.all_tables)}")

    # Print edge summary
    print("\nEdges:")
    seen: set[str] = set()
    for table, edges in sorted(graph.edges.items()):
        for e in edges:
            key = f"{e.left_table}.{e.left_column}->{e.right_table}.{e.right_column}"
            if key not in seen:
                seen.add(key)
                print(f"  {key}  (confidence={e.confidence}, source={e.source})")

    if not args.tables:
        return

    print(f"\nSelected tables: {args.tables}")

    # Paths between each pair
    tables = args.tables
    for i in range(len(tables)):
        for j in range(i + 1, len(tables)):
            path = graph.find_join_path(tables[i], tables[j], max_depth=args.max_depth)
            if path is None:
                print(f"\n  {tables[i]} -> {tables[j]}: NO PATH (depth {args.max_depth})")
            elif not path:
                print(f"\n  {tables[i]} -> {tables[j]}: SAME TABLE")
            else:
                print(f"\n  {tables[i]} -> {tables[j]}: {len(path)} hops")
                for edge in path:
                    print(
                        f"    {edge.left_table}.{edge.left_column} -> "
                        f"{edge.right_table}.{edge.right_column} "
                        f"(confidence={edge.confidence})"
                    )

    # Bridge tables
    bridges = graph.shortest_bridge_tables(tables, max_depth=args.max_depth)
    if bridges:
        print(f"\nBridge tables needed: {bridges}")
    else:
        print("\nNo bridge tables needed (all selected tables are connected)")


if __name__ == "__main__":
    main()
