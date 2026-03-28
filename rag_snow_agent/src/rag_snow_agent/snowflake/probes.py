"""Micro-probes: lightweight Snowflake queries that verify schema details."""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)

_PROBE_TIMEOUT_SEC = 5


def probe_column_exists(executor, table_qname: str, column_name: str) -> bool:
    """Check if a column exists by running a cheap LIMIT 0 query.

    Returns True if the column is valid, False otherwise.
    """
    sql = f'SELECT "{column_name}" FROM {table_qname} LIMIT 0'
    try:
        result = executor.execute(sql, sample_rows=0)
        exists = result.success
        log.debug(
            "probe_column_exists(%s, %s) -> %s",
            table_qname, column_name, exists,
        )
        return exists
    except Exception:
        log.debug(
            "probe_column_exists(%s, %s) -> False (exception)",
            table_qname, column_name,
            exc_info=True,
        )
        return False


def probe_variant_field_exists(
    executor, table_qname: str, variant_col: str, field_path: str
) -> bool:
    """Check if a VARIANT field path exists.

    Returns True if the field path is valid, False otherwise.
    """
    sql = f'SELECT "{variant_col}":"{field_path}" FROM {table_qname} LIMIT 0'
    try:
        result = executor.execute(sql, sample_rows=0)
        exists = result.success
        log.debug(
            "probe_variant_field_exists(%s, %s, %s) -> %s",
            table_qname, variant_col, field_path, exists,
        )
        return exists
    except Exception:
        log.debug(
            "probe_variant_field_exists(%s, %s, %s) -> False (exception)",
            table_qname, variant_col, field_path,
            exc_info=True,
        )
        return False


def probe_top_values(
    executor, table_qname: str, column_name: str, limit: int = 5
) -> list[str]:
    """Get top distinct values for a column (useful for filter verification).

    Returns up to *limit* distinct non-null values as strings, or an empty list on failure.
    """
    sql = (
        f'SELECT DISTINCT "{column_name}" FROM {table_qname} '
        f"WHERE \"{column_name}\" IS NOT NULL LIMIT {limit}"
    )
    try:
        result = executor.execute(sql, sample_rows=limit)
        if result.success and result.rows_sample:
            values = [str(row[0]) for row in result.rows_sample]
            log.debug(
                "probe_top_values(%s, %s) -> %d values",
                table_qname, column_name, len(values),
            )
            return values
        return []
    except Exception:
        log.debug(
            "probe_top_values(%s, %s) -> [] (exception)",
            table_qname, column_name,
            exc_info=True,
        )
        return []
