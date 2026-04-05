interface Props {
  log: string[];
  open: boolean;
  onToggle: () => void;
}

export function ExecutionLogPanel({ log, open, onToggle }: Props) {
  return (
    <>
      {/* Toggle tab on the right edge */}
      <button
        onClick={onToggle}
        className="fixed right-0 top-1/2 -translate-y-1/2 z-30 bg-gray-800 text-white text-xs px-1.5 py-6 rounded-l-lg hover:bg-gray-700 transition-colors shadow-lg"
        style={{ writingMode: "vertical-rl" }}
      >
        {open ? "Close Log ▶" : "◀ Exec Log"}
        {log.length > 0 && (
          <span className="ml-1 bg-blue-500 text-white rounded-full px-1 text-[10px]" style={{ writingMode: "horizontal-tb" }}>
            {log.length}
          </span>
        )}
      </button>

      {/* Sliding panel */}
      <div
        className={`fixed top-0 right-0 h-full bg-gray-900 text-white z-20 shadow-2xl transition-transform duration-300 ease-in-out ${
          open ? "translate-x-0" : "translate-x-full"
        }`}
        style={{ width: "20%" }}
      >
        <div className="flex items-center justify-between px-4 py-3 border-b border-gray-700">
          <h2 className="text-sm font-semibold">Execution Log</h2>
          <button
            onClick={onToggle}
            className="text-gray-400 hover:text-white text-lg"
          >
            ✕
          </button>
        </div>
        <div className="overflow-y-auto h-[calc(100%-48px)] p-3 font-mono text-xs">
          {log.length === 0 ? (
            <div className="text-gray-500 italic">No execution log yet. Send a query to see detailed progress.</div>
          ) : (
            log.map((entry, i) => (
              <div key={i} className="py-1 border-b border-gray-800">
                <span className="text-gray-600 mr-2">[{i + 1}]</span>
                <span className="text-gray-300">{entry}</span>
              </div>
            ))
          )}
        </div>
      </div>
    </>
  );
}
