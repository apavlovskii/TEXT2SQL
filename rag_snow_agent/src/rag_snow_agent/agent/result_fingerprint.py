"""Result fingerprinting for candidate SQL outputs."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ResultFingerprint:
    """Lightweight summary of a SQL execution result."""

    row_count: int | None = None
    column_count: int | None = None
    column_names: list[str] = field(default_factory=list)
    null_ratios: dict[str, float] = field(default_factory=dict)
    numeric_stats: dict[str, dict[str, float | int | None]] = field(
        default_factory=dict
    )
    sample_rows: list[tuple] | list[dict] | None = None


def build_result_fingerprint(
    execution_result,
    max_numeric_stats_cols: int = 5,
) -> ResultFingerprint:
    """Build a ResultFingerprint from an ExecutionResult.

    Works with both tuple-based and dict-based sample rows.
    Returns a partial fingerprint if execution failed.
    """
    fp = ResultFingerprint()

    if not execution_result or not execution_result.success:
        return fp

    fp.row_count = execution_result.row_count
    fp.sample_rows = execution_result.rows_sample

    col_names = execution_result.column_names or []
    fp.column_names = list(col_names)
    fp.column_count = len(col_names) if col_names else None

    rows = execution_result.rows_sample
    if not rows or not col_names:
        return fp

    # Normalize rows to list[dict] for uniform processing
    if rows and isinstance(rows[0], dict):
        row_dicts: list[dict] = rows
    else:
        row_dicts = [dict(zip(col_names, row)) for row in rows]

    n_rows = len(row_dicts)
    if n_rows == 0:
        return fp

    # Null ratios
    for col in col_names:
        null_count = sum(1 for r in row_dicts if r.get(col) is None)
        fp.null_ratios[col] = null_count / n_rows

    # Numeric stats (on first max_numeric_stats_cols numeric columns)
    stats_computed = 0
    for col in col_names:
        if stats_computed >= max_numeric_stats_cols:
            break
        values: list[float] = []
        for r in row_dicts:
            v = r.get(col)
            if v is not None:
                try:
                    values.append(float(v))
                except (ValueError, TypeError):
                    break
        else:
            # All non-null values were numeric
            if values:
                fp.numeric_stats[col] = {
                    "min": min(values),
                    "max": max(values),
                    "mean": sum(values) / len(values),
                }
                stats_computed += 1

    return fp
