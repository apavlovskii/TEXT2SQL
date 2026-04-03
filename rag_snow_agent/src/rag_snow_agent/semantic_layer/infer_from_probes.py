"""Infer semantic facts by running lightweight probes against Snowflake."""

from __future__ import annotations

import logging

from ..snowflake.metadata import TableInfo
from .models import SemanticFact

log = logging.getLogger(__name__)

_VARIANT_TYPES = {"VARIANT", "OBJECT", "ARRAY"}
_DATE_TYPES = {"DATE", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ", "TIMESTAMP"}
_STRING_TYPES = {"VARCHAR", "STRING", "TEXT", "CHAR", "CHARACTER"}


def _deduplicate_partition_tables(tables: list[TableInfo]) -> list[TableInfo]:
    """Collapse daily partition tables (GA360/GA4 pattern) into one representative.

    Tables are considered partitions if they share the same schema AND their names
    differ only by a date suffix (e.g., GA_SESSIONS_20170101 vs GA_SESSIONS_20170802).
    All other tables are kept individually — even if they share the same column
    structure (e.g., PATENTS.PUBLICATIONS vs PATENTS.DISCLOSURES_13).
    """
    import re

    _DATE_SUFFIX_RE = re.compile(r"^(.+?)_?\d{6,8}$")

    # Group tables by (schema, base_name_without_date, column_signature)
    partition_groups: dict[str, list[TableInfo]] = {}
    non_partition: list[TableInfo] = []

    for t in tables:
        m = _DATE_SUFFIX_RE.match(t.table_name)
        if m:
            base = m.group(1)
            sig = f"{t.table_schema}||{base}||{'|'.join(sorted(c.column_name for c in t.columns))}"
            partition_groups.setdefault(sig, []).append(t)
        else:
            non_partition.append(t)

    # For partition groups with 3+ members, keep just one representative
    result = list(non_partition)
    for sig, group in partition_groups.items():
        if len(group) >= 3:
            # These are daily partitions — keep one representative
            result.append(group[0])
        else:
            # Only 1-2 tables with date suffix — keep all (not a partition pattern)
            result.extend(group)

    return result


def infer_from_probes(
    db_id: str,
    executor,  # SnowflakeExecutor
    tables: list[TableInfo],
    max_probe_budget: int = 10,
) -> list[SemanticFact]:
    """Run lightweight probes to discover semantic facts.

    De-duplicates tables with the same column signature, then for each unique
    table (up to budget):
    - Probes top 5 values for candidate dimension/filter columns
    - Probes min/max for time columns
    - Probes sample rows

    Returns SemanticFacts with source=["probes"].
    """
    facts: list[SemanticFact] = []
    unique_tables = _deduplicate_partition_tables(tables)

    for table in unique_tables[:max_probe_budget]:
        qname = table.qualified_name

        for col in table.columns:
            dtype = col.data_type.upper().split("(")[0].strip()

            # Probe min/max for time columns
            if dtype in _DATE_TYPES:
                try:
                    sql = (
                        f'SELECT MIN("{col.column_name}"), MAX("{col.column_name}") '
                        f"FROM {qname}"
                    )
                    result = executor.execute(sql, sample_rows=1)
                    if result.success and result.rows_sample:
                        row = result.rows_sample[0]
                        facts.append(
                            SemanticFact(
                                fact_type="column_stats",
                                subject=f"{qname}.{col.column_name}",
                                value={
                                    "min": str(row[0]) if row[0] is not None else None,
                                    "max": str(row[1]) if row[1] is not None else None,
                                },
                                confidence=0.9,
                                evidence=[f"Probed MIN/MAX for {col.column_name}"],
                                source=["probes"],
                            )
                        )
                except Exception:
                    log.debug(
                        "MIN/MAX probe failed for %s.%s",
                        qname, col.column_name,
                        exc_info=True,
                    )

            # Probe top values for string columns (dimension/filter candidates)
            if dtype in _STRING_TYPES:
                try:
                    sql = (
                        f'SELECT DISTINCT "{col.column_name}" '
                        f"FROM {qname} "
                        f'WHERE "{col.column_name}" IS NOT NULL '
                        f"LIMIT 5"
                    )
                    result = executor.execute(sql, sample_rows=5)
                    if result.success and result.rows_sample:
                        values = [str(r[0]) for r in result.rows_sample]
                        facts.append(
                            SemanticFact(
                                fact_type="filter_value_hints",
                                subject=f"{qname}.{col.column_name}",
                                value=values,
                                confidence=0.8,
                                evidence=[
                                    f"Top distinct values for {col.column_name}: {values}"
                                ],
                                source=["probes"],
                            )
                        )
                except Exception:
                    log.debug(
                        "Top values probe failed for %s.%s",
                        qname, col.column_name,
                        exc_info=True,
                    )

        # Probe sample rows
        try:
            sql = f"SELECT * FROM {qname} LIMIT 5"
            result = executor.execute(sql, sample_rows=5)
            if result.success and result.rows_sample and result.column_names:
                facts.append(
                    SemanticFact(
                        fact_type="sample_rows",
                        subject=qname,
                        value={
                            "columns": result.column_names,
                            "rows": [
                                [str(v) for v in row] for row in result.rows_sample
                            ],
                        },
                        confidence=1.0,
                        evidence=[f"Sample rows from {qname}"],
                        source=["probes"],
                    )
                )
        except Exception:
            log.debug("Sample rows probe failed for %s", qname, exc_info=True)

    log.info(
        "Collected %d facts from probes across %d tables for %s",
        len(facts), min(len(unique_tables), max_probe_budget), db_id,
    )
    return facts
