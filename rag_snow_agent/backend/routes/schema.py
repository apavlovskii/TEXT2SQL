"""Schema browser endpoint."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends

from ..dependencies import get_agent, get_settings
from ..config import AppSettings
from ..models.responses import SchemaResponse, SchemaTableInfo
from ..services.agent_adapter import AgentAdapter

router = APIRouter(prefix="/api/schema", tags=["schema"])
log = logging.getLogger(__name__)


@router.get("/{db_id}", response_model=SchemaResponse)
def get_schema(
    db_id: str,
    agent: AgentAdapter = Depends(get_agent),
    settings: AppSettings = Depends(get_settings),
):
    """Return tables and columns for a database from ChromaDB."""
    tables: list[SchemaTableInfo] = []

    if not agent._chroma_store:
        return SchemaResponse(db_id=db_id, tables=[])

    try:
        col = agent._chroma_store.schema_collection()

        # Get tables
        table_results = col.get(
            where={"$and": [{"db_id": db_id}, {"object_type": "table"}]},
            include=["metadatas"],
            limit=50,
        )
        table_names = set()
        for meta in table_results.get("metadatas") or []:
            qname = meta.get("qualified_name", "")
            comment = meta.get("comment", "")
            table_names.add(qname)
            tables.append(SchemaTableInfo(
                qualified_name=qname,
                comment=comment or None,
                columns=[],
            ))

        # Get columns for each table
        col_results = col.get(
            where={"$and": [{"db_id": db_id}, {"object_type": "column"}]},
            include=["metadatas"],
            limit=500,
        )
        cols_by_table: dict[str, list[dict]] = {}
        for meta in col_results.get("metadatas") or []:
            tqn = meta.get("table_qualified_name", "")
            dtype = meta.get("data_type", "")
            if dtype == "VARIANT_FIELD":
                continue
            col_name = meta.get("qualified_name", "").rsplit(".", 1)[-1]
            comment = meta.get("comment", "")
            cols_by_table.setdefault(tqn, []).append({
                "name": col_name,
                "type": dtype,
                "comment": comment or None,
            })

        for tbl in tables:
            tbl.columns = cols_by_table.get(tbl.qualified_name, [])

    except Exception as exc:
        log.error("Schema fetch failed: %s", exc)

    return SchemaResponse(db_id=db_id, tables=tables)
