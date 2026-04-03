"""SchemaSlice: the compact schema fragment sent to the LLM prompt."""

from __future__ import annotations

from dataclasses import dataclass, field

import tiktoken

_enc = tiktoken.get_encoding("cl100k_base")


def _token_count(text: str) -> int:
    return len(_enc.encode(text))


@dataclass
class ColumnSlice:
    """One column kept in the slice."""

    name: str  # just the column name (e.g. ORDER_ID)
    data_type: str
    comment: str | None = None
    original_name: str | None = None  # exact case from Snowflake (e.g. "fullVisitorId")
    token_estimate: int = 0
    fused_rank: int = 0  # 1-based rank from retrieval (lower = better)
    is_join_key: bool = False
    is_time_column: bool = False
    is_variant: bool = False
    variant_kind: str | None = None  # "ARRAY" | "OBJECT" | None — structure of the VARIANT
    variant_fields: list[str] | None = None  # known nested field paths (e.g. ["page.pagePath", "productRevenue"])
    date_format: str | None = None  # explicit format hint (e.g. "YYYYMMDD string", "YYYYMMDD integer")


@dataclass
class TableSlice:
    """One table kept in the slice, with its selected columns."""

    qualified_name: str  # DB.SCHEMA.TABLE
    comment: str | None = None
    table_token_estimate: int = 0  # token cost of the table header line
    fused_rank: int = 0
    columns: list[ColumnSlice] = field(default_factory=list)

    @property
    def token_estimate(self) -> int:
        """Total tokens: table header + all kept columns."""
        return self.table_token_estimate + sum(c.token_estimate for c in self.columns)


@dataclass
class SchemaSlice:
    """The full schema slice ready for prompt injection."""

    db_id: str
    tables: list[TableSlice] = field(default_factory=list)

    @property
    def token_estimate(self) -> int:
        return sum(t.token_estimate for t in self.tables)

    def format_for_prompt(self) -> str:
        """Render a compact, LLM-friendly text block."""
        lines: list[str] = [f"-- Database: {self.db_id}"]
        for ts in self.tables:
            header = f"TABLE {ts.qualified_name}"
            if ts.comment:
                header += f"  -- {ts.comment}"
            lines.append(header)
            for col in ts.columns:
                dtype_display = col.data_type
                annotation = ""
                if col.is_variant:
                    if col.variant_kind == "ARRAY":
                        annotation = (
                            " ARRAY — use LATERAL FLATTEN(input => t.\"{}\")"
                            " alias, then alias.value:\"field\"::TYPE"
                        ).format(col.original_name or col.name)
                    elif col.variant_kind == "OBJECT":
                        annotation = (
                            " OBJECT — access with t.\"{}\":\"field\"::TYPE"
                        ).format(col.original_name or col.name)
                    else:
                        annotation = " (VARIANT — use :field path or LATERAL FLATTEN for arrays)"
                    if col.variant_fields:
                        fields_str = ", ".join(col.variant_fields[:8])
                        annotation += f" [fields: {fields_str}]"
                if col.is_time_column and col.date_format:
                    fmt = col.date_format
                    if "integer" in fmt.lower():
                        annotation += f" — date as integer ({fmt}), compare with integers e.g. >= 20170201"
                    elif "string" in fmt.lower():
                        annotation += f" — date as string ({fmt}), compare with string literals e.g. >= '20170201'"
                    else:
                        annotation += f" — {fmt}"
                parts = [f"  {col.name} {dtype_display}{annotation}"]
                if col.comment:
                    parts.append(f"-- {col.comment}")
                lines.append(" ".join(parts))
        text = "\n".join(lines)
        return text

    def summary(self) -> str:
        n_tables = len(self.tables)
        n_cols = sum(len(t.columns) for t in self.tables)
        return (
            f"SchemaSlice({self.db_id}): {n_tables} tables, "
            f"{n_cols} columns, ~{self.token_estimate} tokens"
        )
