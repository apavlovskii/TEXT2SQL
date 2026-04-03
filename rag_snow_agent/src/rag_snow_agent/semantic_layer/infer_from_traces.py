"""Infer semantic facts from stored execution traces."""

from __future__ import annotations

import logging
import re
from collections import Counter

from .models import SemanticFact

log = logging.getLogger(__name__)

_VARIANT_ACCESS_RE = re.compile(r'"(\w+)"\s*:\s*"?(\w+)"?', re.IGNORECASE)


def infer_from_traces(db_id: str, trace_store) -> list[SemanticFact]:
    """Extract semantic facts from trace memory.

    Queries trace_memory collection for this db_id (top 100),
    extracts commonly used tables, columns, join patterns,
    and VARIANT access patterns from stored SQL.

    Returns SemanticFacts with source=["traces"].
    If no traces or trace_store is None, returns empty list.
    """
    if trace_store is None:
        return []

    facts: list[SemanticFact] = []

    try:
        # Query top traces for this db_id
        col = trace_store.collection()
        results = col.get(
            where={"db_id": db_id},
            include=["metadatas", "documents"],
            limit=100,
        )
    except Exception:
        log.debug("Failed to query traces for db_id=%s", db_id, exc_info=True)
        return []

    ids = results.get("ids", [])
    metas = results.get("metadatas", [])
    docs = results.get("documents", [])

    if not ids:
        return []

    # Count table usage
    table_counter: Counter[str] = Counter()
    for meta in metas:
        tables_str = meta.get("tables_used", "")
        if tables_str:
            for t in tables_str.split(","):
                t = t.strip()
                if t:
                    table_counter[t] += 1

    # Emit facts for commonly used tables
    for table_name, count in table_counter.most_common(20):
        if count >= 2:
            facts.append(
                SemanticFact(
                    fact_type="frequently_used_table",
                    subject=table_name,
                    value={"usage_count": count},
                    confidence=min(0.5 + count * 0.1, 0.9),
                    evidence=[f"Used in {count} traced queries"],
                    source=["traces"],
                )
            )

    # Extract VARIANT access patterns from documents (which contain SQL previews)
    variant_patterns: Counter[str] = Counter()
    for doc in docs:
        if doc:
            for match in _VARIANT_ACCESS_RE.finditer(doc):
                pattern = f"{match.group(1)}:{match.group(2)}"
                variant_patterns[pattern] += 1

    for pattern, count in variant_patterns.most_common(10):
        if count >= 2:
            facts.append(
                SemanticFact(
                    fact_type="variant_access_pattern",
                    subject=pattern,
                    value={"usage_count": count},
                    confidence=min(0.5 + count * 0.1, 0.9),
                    evidence=[f"VARIANT access pattern seen {count} times in traces"],
                    source=["traces"],
                )
            )

    log.info(
        "Extracted %d facts from %d traces for %s", len(facts), len(ids), db_id
    )
    return facts
