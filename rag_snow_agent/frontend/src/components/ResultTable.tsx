import { useState } from "react";
import type { QueryResult } from "../types";

interface Props {
  results: QueryResult;
}

export function ResultTable({ results }: Props) {
  const [open, setOpen] = useState(true);

  return (
    <div className="mt-2 border border-gray-200 rounded-lg overflow-hidden">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center justify-between px-3 py-2 bg-gray-50 hover:bg-gray-100 text-sm font-medium text-gray-700 transition-colors"
      >
        <span>
          Results ({results.row_count} row{results.row_count !== 1 ? "s" : ""}
          {results.truncated ? ", truncated" : ""})
        </span>
        <span className="text-gray-400">{open ? "▲" : "▼"}</span>
      </button>
      {open && (
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="bg-gray-50 border-b border-gray-200">
                {results.columns.map((col, i) => (
                  <th
                    key={i}
                    className="px-3 py-2 text-left font-semibold text-gray-600 whitespace-nowrap"
                  >
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {results.rows.map((row, ri) => (
                <tr
                  key={ri}
                  className={ri % 2 === 0 ? "bg-white" : "bg-gray-50"}
                >
                  {row.map((cell, ci) => (
                    <td
                      key={ci}
                      className="px-3 py-1.5 text-gray-700 whitespace-nowrap border-b border-gray-100"
                    >
                      {cell === null ? (
                        <span className="text-gray-400 italic">NULL</span>
                      ) : (
                        String(cell)
                      )}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
          {results.truncated && (
            <div className="px-3 py-2 text-xs text-gray-500 bg-yellow-50 border-t border-yellow-200">
              Showing first {results.rows.length} of {results.row_count} rows
            </div>
          )}
        </div>
      )}
    </div>
  );
}
