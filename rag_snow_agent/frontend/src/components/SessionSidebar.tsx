import type { AgentSettings, Session } from "../types";
import { ChromaCollectionsPanel } from "./ChromaCollectionsPanel";
import { SchemaExplorer } from "./SchemaExplorer";
import { SettingsPanel } from "./SettingsPanel";

interface Props {
  sessions: Session[];
  activeSessionId: string | null;
  dbId: string;
  availableDbs: string[];
  settings: AgentSettings;
  onSelectSession: (id: string) => void;
  onNewSession: () => void;
  onDeleteSession: (id: string) => void;
  onDbChange: (db: string) => void;
  onDatasourceChange: (datasource: string) => void;
  onSettingsChange: (settings: AgentSettings) => void;
}

export function SessionSidebar({
  sessions,
  activeSessionId,
  dbId,
  availableDbs,
  settings,
  onSelectSession,
  onNewSession,
  onDeleteSession,
  onDbChange,
  onDatasourceChange,
  onSettingsChange,
}: Props) {
  return (
    <div className="w-72 bg-gray-900 text-white flex flex-col h-full">
      {/* Header */}
      <div className="p-4 border-b border-gray-700">
        <h1 className="text-lg font-bold">Analytics Insite</h1>
        <p className="text-xs text-gray-400 mt-1">Text-to-SQL Chatbot</p>
      </div>

      {/* Datasource Selector (first) */}
      <div className="px-4 py-3 border-b border-gray-700">
        <label className="text-xs text-gray-400 block mb-1">Datasource</label>
        <select
          value={settings.datasource}
          onChange={(e) => onDatasourceChange(e.target.value)}
          className="w-full bg-gray-800 text-sm rounded px-2 py-1.5 border border-gray-600 focus:border-blue-500 focus:outline-none text-white"
        >
          <option value="sqlite">SQLite (local)</option>
          <option value="snowflake">Snowflake</option>
        </select>
      </div>

      {/* DB Selector (second, populated from datasource) */}
      <div className="px-4 py-3 border-b border-gray-700">
        <label className="text-xs text-gray-400 block mb-1">Database</label>
        <select
          value={dbId}
          onChange={(e) => onDbChange(e.target.value)}
          className="w-full bg-gray-800 text-sm rounded px-2 py-1.5 border border-gray-600 focus:border-blue-500 focus:outline-none text-white"
        >
          {availableDbs.map((db) => (
            <option key={db} value={db}>{db}</option>
          ))}
        </select>
      </div>

      {/* Settings (model, retries, candidates) */}
      <SettingsPanel settings={settings} onChange={onSettingsChange} />

      {/* Schema Explorer */}
      <div className="border-b border-gray-700">
        <div className="px-4 py-2 text-xs text-gray-400 font-medium">Schema Browser</div>
        <div className="max-h-48 overflow-y-auto pb-2">
          <SchemaExplorer dbId={dbId} />
        </div>
      </div>

      {/* Vector DB Collections */}
      <ChromaCollectionsPanel />

      {/* New Chat */}
      <div className="p-3">
        <button
          onClick={onNewSession}
          className="w-full rounded-lg border border-gray-600 px-3 py-2 text-sm hover:bg-gray-800 transition-colors"
        >
          + New Chat
        </button>
      </div>

      {/* Session List */}
      <div className="flex-1 overflow-y-auto px-2">
        {sessions.map((session) => (
          <div
            key={session.id}
            className={`group flex items-center rounded-lg px-3 py-2 mb-1 text-sm cursor-pointer transition-colors ${
              session.id === activeSessionId
                ? "bg-gray-700 text-white"
                : "text-gray-300 hover:bg-gray-800"
            }`}
            onClick={() => onSelectSession(session.id)}
          >
            <span className="flex-1 truncate">{session.name}</span>
            <button
              onClick={(e) => {
                e.stopPropagation();
                onDeleteSession(session.id);
              }}
              className="hidden group-hover:block text-gray-500 hover:text-red-400 ml-2 text-xs"
            >
              ✕
            </button>
          </div>
        ))}
      </div>
    </div>
  );
}
