import { useCallback, useEffect, useState } from "react";
import { getSchema } from "../api/client";
import type { SchemaInfo, SchemaTable } from "../types";

interface Props {
  dbId: string;
}

export function SchemaExplorer({ dbId }: Props) {
  const [schema, setSchema] = useState<SchemaInfo | null>(null);
  const [loading, setLoading] = useState(false);
  const [expandedTable, setExpandedTable] = useState<string | null>(null);

  const fetchSchema = useCallback(async () => {
    setLoading(true);
    try {
      const data = await getSchema(dbId);
      setSchema(data);
    } catch (err) {
      console.error("Failed to load schema", err);
    } finally {
      setLoading(false);
    }
  }, [dbId]);

  useEffect(() => {
    fetchSchema();
  }, [fetchSchema]);

  if (loading) {
    return <div className="px-4 py-2 text-xs text-gray-400">Loading schema...</div>;
  }

  if (!schema || schema.tables.length === 0) {
    return <div className="px-4 py-2 text-xs text-gray-400">No tables found</div>;
  }

  return (
    <div className="text-xs">
      {schema.tables.map((table) => (
        <TableNode
          key={table.qualified_name}
          table={table}
          expanded={expandedTable === table.qualified_name}
          onToggle={() =>
            setExpandedTable(
              expandedTable === table.qualified_name ? null : table.qualified_name,
            )
          }
        />
      ))}
    </div>
  );
}

function TableNode({
  table,
  expanded,
  onToggle,
}: {
  table: SchemaTable;
  expanded: boolean;
  onToggle: () => void;
}) {
  const shortName = table.qualified_name.split(".").pop() || table.qualified_name;

  return (
    <div className="mb-0.5">
      <button
        onClick={onToggle}
        className="w-full flex items-center gap-1 px-3 py-1.5 text-left text-gray-300 hover:bg-gray-800 rounded transition-colors"
        title={table.comment || table.qualified_name}
      >
        <span className="text-gray-500">{expanded ? "▼" : "▶"}</span>
        <span className="text-yellow-400">⊞</span>
        <span className="truncate">{shortName}</span>
        <span className="text-gray-600 ml-auto">{table.columns.length}</span>
      </button>
      {expanded && (
        <div className="ml-6 border-l border-gray-700 pl-2">
          {table.columns.map((col, i) => (
            <div
              key={i}
              className="flex items-center gap-2 px-2 py-0.5 text-gray-400 hover:text-gray-200"
              title={col.comment || undefined}
            >
              <span className="text-blue-400 text-[10px]">◆</span>
              <span className="truncate">{col.name}</span>
              <span className="text-gray-600 ml-auto text-[10px]">{col.type}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
