import { useState } from "react";
import type { ExecutionMetadata } from "../types";

interface Props {
  metadata: ExecutionMetadata;
}

export function MetadataCard({ metadata }: Props) {
  const [open, setOpen] = useState(false);

  return (
    <div className="mt-2 border border-gray-200 rounded-lg overflow-hidden">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center justify-between px-3 py-2 bg-gray-50 hover:bg-gray-100 text-sm font-medium text-gray-700 transition-colors"
      >
        <span>Metadata</span>
        <span className="text-gray-400">{open ? "▲" : "▼"}</span>
      </button>
      {open && (
        <div className="p-3 text-xs text-gray-600 space-y-1">
          {metadata.elapsed_ms != null && (
            <div>Execution time: {metadata.elapsed_ms}ms</div>
          )}
          <div>LLM calls: {metadata.llm_calls}</div>
          <div>Repairs: {metadata.repair_count}</div>
          <div>Candidates: {metadata.candidate_count}</div>
          <div>Model: {metadata.model}</div>
          <div>Datasource: {metadata.datasource}</div>
        </div>
      )}
    </div>
  );
}
