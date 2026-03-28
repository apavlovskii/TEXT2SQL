"""Graph of join edges between tables, built from JoinCards."""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field

log = logging.getLogger(__name__)


@dataclass
class JoinGraphEdge:
    """A single edge in the join graph."""

    left_table: str
    left_column: str
    right_table: str
    right_column: str
    confidence: float
    source: str


class JoinGraph:
    """Graph of join edges between tables."""

    def __init__(self) -> None:
        self.edges: dict[str, list[JoinGraphEdge]] = defaultdict(list)

    @classmethod
    def from_join_cards(cls, join_cards: list[dict]) -> JoinGraph:
        """Build a JoinGraph from Chroma join card metadata dicts."""
        graph = cls()
        for card in join_cards:
            lt = card.get("left_table", "")
            lc = card.get("left_column", "")
            rt = card.get("right_table", "")
            rc = card.get("right_column", "")
            confidence = float(card.get("confidence", 0.5))
            source = card.get("source", "unknown")

            edge = JoinGraphEdge(
                left_table=lt,
                left_column=lc,
                right_table=rt,
                right_column=rc,
                confidence=confidence,
                source=source,
            )
            graph.edges[lt].append(edge)
            # Add reverse edge for bidirectional traversal
            rev_edge = JoinGraphEdge(
                left_table=rt,
                left_column=rc,
                right_table=lt,
                right_column=lc,
                confidence=confidence,
                source=source,
            )
            graph.edges[rt].append(rev_edge)
        return graph

    @property
    def all_tables(self) -> set[str]:
        return set(self.edges.keys())

    def neighbors(self, table_name: str) -> list[JoinGraphEdge]:
        """Return all edges from *table_name*."""
        return list(self.edges.get(table_name, []))

    def find_join_path(
        self, table_a: str, table_b: str, max_depth: int = 3
    ) -> list[JoinGraphEdge] | None:
        """BFS to find a join path between two tables.

        Returns a list of edges forming the path, or None if no path exists
        within *max_depth* hops.
        """
        if table_a == table_b:
            return []

        # BFS: queue items are (current_table, path_of_edges)
        visited: set[str] = {table_a}
        queue: deque[tuple[str, list[JoinGraphEdge]]] = deque()
        queue.append((table_a, []))

        while queue:
            current, path = queue.popleft()
            if len(path) >= max_depth:
                continue
            for edge in self.edges.get(current, []):
                neighbor = edge.right_table
                if neighbor in visited:
                    continue
                new_path = path + [edge]
                if neighbor == table_b:
                    return new_path
                visited.add(neighbor)
                queue.append((neighbor, new_path))

        return None

    def shortest_bridge_tables(
        self,
        selected_tables: list[str],
        max_depth: int = 3,
        min_confidence: float = 0.0,
    ) -> list[str]:
        """Find minimal bridge tables to connect disconnected selected tables.

        Uses BFS with confidence-aware shortest path, capped at *max_depth*.
        Returns table names that should be added as bridges.
        """
        if len(selected_tables) < 2:
            return []

        # Build connected components among selected tables
        selected_set = set(selected_tables)
        # Union-Find
        parent: dict[str, str] = {t: t for t in selected_tables}

        def find(x: str) -> str:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a: str, b: str) -> None:
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb

        # Check direct connections
        for t in selected_tables:
            for edge in self.edges.get(t, []):
                if edge.confidence < min_confidence:
                    continue
                if edge.right_table in selected_set:
                    union(t, edge.right_table)

        # Find disconnected components
        components: dict[str, list[str]] = defaultdict(list)
        for t in selected_tables:
            components[find(t)].append(t)

        if len(components) <= 1:
            return []

        # Try to connect components via bridge tables
        bridge_tables: list[str] = []
        comp_list = list(components.values())

        for i in range(len(comp_list) - 1):
            # Try to connect comp_list[i] with comp_list[i+1]
            best_path: list[JoinGraphEdge] | None = None
            best_min_conf = -1.0

            for t_a in comp_list[i]:
                for t_b in comp_list[i + 1]:
                    path = self.find_join_path(t_a, t_b, max_depth=max_depth)
                    if path is None:
                        continue
                    path_min_conf = min(
                        (e.confidence for e in path), default=1.0
                    )
                    if path_min_conf < min_confidence:
                        continue
                    if best_path is None or (
                        len(path) < len(best_path)
                        or (
                            len(path) == len(best_path)
                            and path_min_conf > best_min_conf
                        )
                    ):
                        best_path = path
                        best_min_conf = path_min_conf

            if best_path:
                for edge in best_path:
                    for tbl in (edge.left_table, edge.right_table):
                        if tbl not in selected_set and tbl not in bridge_tables:
                            bridge_tables.append(tbl)

        return bridge_tables
