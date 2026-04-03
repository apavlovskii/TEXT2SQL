"""Pydantic models for schema cards stored in ChromaDB."""

from __future__ import annotations

import tiktoken
from pydantic import BaseModel, computed_field

_enc = tiktoken.get_encoding("cl100k_base")


def _token_count(text: str) -> int:
    return len(_enc.encode(text))


class TableCard(BaseModel):
    """One card per table/view, stored in the schema_cards collection."""

    db_id: str
    qualified_name: str  # DB.SCHEMA.TABLE
    table_type: str  # BASE TABLE / VIEW
    comment: str | None = None
    row_count: int | None = None
    column_names: list[str] = []
    time_columns: list[str] = []
    common_join_keys: list[str] = []

    @computed_field  # type: ignore[prop-decorator]
    @property
    def document(self) -> str:
        """Text representation used as the ChromaDB document (embedded)."""
        parts = [f"Table: {self.qualified_name}"]
        if self.comment:
            parts.append(f"Description: {self.comment}")
        if self.column_names:
            parts.append(f"Columns: {', '.join(self.column_names)}")
        if self.time_columns:
            parts.append(f"Time columns: {', '.join(self.time_columns)}")
        if self.common_join_keys:
            parts.append(f"Join keys: {', '.join(self.common_join_keys)}")
        return "\n".join(parts)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def token_estimate(self) -> int:
        return _token_count(self.document)

    def chroma_id(self) -> str:
        return f"table:{self.qualified_name}"

    def chroma_metadata(self) -> dict:
        meta = {
            "db_id": self.db_id,
            "object_type": "table",
            "qualified_name": self.qualified_name,
            "source": "information_schema",
            "token_estimate": self.token_estimate,
        }
        if self.comment:
            meta["comment"] = self.comment
        return meta


class ColumnCard(BaseModel):
    """One card per column, stored in the schema_cards collection."""

    db_id: str
    qualified_name: str  # DB.SCHEMA.TABLE.COLUMN
    table_qualified_name: str  # DB.SCHEMA.TABLE
    data_type: str
    is_nullable: str
    comment: str | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def document(self) -> str:
        parts = [f"Column: {self.qualified_name}", f"Type: {self.data_type}"]
        if self.comment:
            parts.append(f"Description: {self.comment}")
        parts.append(f"Nullable: {self.is_nullable}")
        return "\n".join(parts)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def token_estimate(self) -> int:
        return _token_count(self.document)

    def chroma_id(self) -> str:
        return f"column:{self.qualified_name}"

    def chroma_metadata(self) -> dict:
        meta = {
            "db_id": self.db_id,
            "object_type": "column",
            "qualified_name": self.qualified_name,
            "source": "information_schema",
            "token_estimate": self.token_estimate,
            "table_qualified_name": self.table_qualified_name,
            "data_type": self.data_type,
        }
        if self.comment:
            meta["comment"] = self.comment
        return meta


class JoinCard(BaseModel):
    """One card per join edge, stored in the schema_cards collection."""

    db_id: str
    left_table: str  # qualified name DB.SCHEMA.TABLE
    left_column: str
    right_table: str  # qualified name DB.SCHEMA.TABLE
    right_column: str
    confidence: float  # 1.0 for FK, 0.7 for heuristic
    source: str  # "fk" or "heuristic_name"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def document(self) -> str:
        parts = [
            f"Join: {self.left_table}.{self.left_column} -> {self.right_table}.{self.right_column}",
            f"Confidence: {self.confidence}",
            f"Source: {self.source}",
        ]
        return "\n".join(parts)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def token_estimate(self) -> int:
        return _token_count(self.document)

    def chroma_id(self) -> str:
        return (
            f"join:{self.left_table}.{self.left_column}"
            f"->{self.right_table}.{self.right_column}"
        )

    def chroma_metadata(self) -> dict:
        return {
            "db_id": self.db_id,
            "object_type": "join",
            "left_table": self.left_table,
            "left_column": self.left_column,
            "right_table": self.right_table,
            "right_column": self.right_column,
            "confidence": self.confidence,
            "source": self.source,
            "token_estimate": self.token_estimate,
        }
