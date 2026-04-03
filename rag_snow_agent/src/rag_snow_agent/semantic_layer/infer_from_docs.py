"""Infer semantic facts from external documentation files."""

from __future__ import annotations

import logging
import re
from pathlib import Path

from .models import SemanticFact

log = logging.getLogger(__name__)

# Regex patterns for extracting semantic facts from documentation
_FIELD_DEF_RE = re.compile(
    r"`?(\w+)`?\s*(?::|is|represents|contains|stores)\s+(.+?)(?:\.|$)",
    re.IGNORECASE | re.MULTILINE,
)
_DATE_FORMAT_RE = re.compile(
    r"(?:date|time)\s+(?:format|pattern)\s*(?::|is)?\s*[`'\"]?(\w+)[`'\"]?",
    re.IGNORECASE,
)
_METRIC_DEF_RE = re.compile(
    r"(?:metric|measure|kpi)\s*(?::|is)?\s*[`'\"]?(\w+)[`'\"]?\s*(?::|is|=)\s*(.+?)(?:\.|$)",
    re.IGNORECASE | re.MULTILINE,
)


def infer_from_docs(
    db_id: str, docs_dir: str | Path | None = None
) -> list[SemanticFact]:
    """Scan documentation files for semantic facts.

    Looks for Spider2 external_knowledge markdown files if available.
    Uses regex + keyword matching (no LLM calls).

    Returns SemanticFacts with source=["docs"].
    If no docs found, returns empty list.
    """
    if docs_dir is None:
        return []

    docs_path = Path(docs_dir)
    if not docs_path.exists() or not docs_path.is_dir():
        log.debug("Docs directory not found: %s", docs_path)
        return []

    facts: list[SemanticFact] = []

    # Scan markdown and text files
    for pattern in ("*.md", "*.txt", "*.rst"):
        for doc_file in docs_path.glob(pattern):
            try:
                content = doc_file.read_text(encoding="utf-8", errors="ignore")
                facts.extend(_extract_facts_from_text(db_id, content, doc_file.name))
            except Exception:
                log.debug("Failed to read doc file: %s", doc_file, exc_info=True)

    log.info("Extracted %d facts from docs for %s", len(facts), db_id)
    return facts


def _extract_facts_from_text(
    db_id: str, content: str, filename: str
) -> list[SemanticFact]:
    """Extract semantic facts from a single document text."""
    facts: list[SemanticFact] = []

    # Extract field definitions
    for match in _FIELD_DEF_RE.finditer(content):
        field_name = match.group(1)
        description = match.group(2).strip()
        if len(description) > 10:  # skip very short matches
            facts.append(
                SemanticFact(
                    fact_type="field_definition",
                    subject=field_name,
                    value=description,
                    confidence=0.6,
                    evidence=[f"From {filename}: {match.group(0)[:100]}"],
                    source=["docs"],
                )
            )

    # Extract date format descriptions
    for match in _DATE_FORMAT_RE.finditer(content):
        format_str = match.group(1)
        facts.append(
            SemanticFact(
                fact_type="date_format_pattern",
                subject=db_id,
                value=format_str,
                confidence=0.7,
                evidence=[f"From {filename}: {match.group(0)[:100]}"],
                source=["docs"],
            )
        )

    # Extract metric definitions
    for match in _METRIC_DEF_RE.finditer(content):
        metric_name = match.group(1)
        metric_def = match.group(2).strip()
        facts.append(
            SemanticFact(
                fact_type="metric_candidate",
                subject=metric_name,
                value=metric_def,
                confidence=0.7,
                evidence=[f"From {filename}: {match.group(0)[:100]}"],
                source=["docs"],
            )
        )

    return facts
