import { useState } from "react";

interface Props {
  sql: string;
}

export function SqlCard({ sql }: Props) {
  const [open, setOpen] = useState(false);
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(sql);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="mt-2 border border-gray-200 rounded-lg overflow-hidden">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center justify-between px-3 py-2 bg-gray-50 hover:bg-gray-100 text-sm font-medium text-gray-700 transition-colors"
      >
        <span>SQL Query</span>
        <span className="text-gray-400">{open ? "▲" : "▼"}</span>
      </button>
      {open && (
        <div className="relative">
          <button
            onClick={handleCopy}
            className="absolute top-2 right-2 text-xs bg-gray-200 hover:bg-gray-300 px-2 py-1 rounded transition-colors"
          >
            {copied ? "Copied!" : "Copy"}
          </button>
          <pre className="p-3 bg-gray-900 text-green-400 text-xs overflow-x-auto whitespace-pre-wrap">
            {sql}
          </pre>
        </div>
      )}
    </div>
  );
}
