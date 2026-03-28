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
    token_estimate: int = 0
    fused_rank: int = 0  # 1-based rank from retrieval (lower = better)
    is_join_key: bool = False
    is_time_column: bool = False


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
                parts = [f"  {col.name} {col.data_type}"]
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
