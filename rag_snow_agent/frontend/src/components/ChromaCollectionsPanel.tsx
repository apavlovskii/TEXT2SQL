import { useEffect, useState } from "react";

interface CollectionInfo {
  name: string;
  count: number;
  metadata: Record<string, any>;
}

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

export function ChromaCollectionsPanel() {
  const [open, setOpen] = useState(false);
  const [collections, setCollections] = useState<CollectionInfo[]>([]);

  useEffect(() => {
    fetch(`${API_URL}/api/collections`)
      .then((r) => r.json())
      .then(setCollections)
      .catch(() => {});
  }, []);

  return (
    <div className="border-b border-gray-700">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center justify-between px-4 py-2 text-xs text-gray-400 font-medium hover:bg-gray-800 transition-colors"
      >
        <span>Vector DB Collections</span>
        <span>{open ? "▲" : "▼"}</span>
      </button>
      {open && (
        <div className="px-4 pb-3 text-xs">
          {collections.length === 0 ? (
            <div className="text-gray-500 italic">No collections loaded</div>
          ) : (
            <table className="w-full">
              <thead>
                <tr className="text-gray-500">
                  <th className="text-left py-1">Collection</th>
                  <th className="text-right py-1">Items</th>
                </tr>
              </thead>
              <tbody>
                {collections.map((c) => (
                  <tr key={c.name} className="text-gray-300 border-t border-gray-800">
                    <td className="py-1 truncate max-w-[120px]" title={c.name}>
                      {c.name}
                    </td>
                    <td className="text-right py-1 text-gray-400">
                      {c.count.toLocaleString()}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}
    </div>
  );
}
