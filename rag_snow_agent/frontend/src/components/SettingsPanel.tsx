import type { AgentSettings } from "../types";

interface Props {
  settings: AgentSettings;
  onChange: (settings: AgentSettings) => void;
}

const MODELS = ["gpt-5.4", "gpt-5-mini", "gpt-5-nano", "gpt-4o", "gpt-4o-mini"];

export function SettingsPanel({ settings, onChange }: Props) {
  return (
    <div className="px-4 py-3 border-b border-gray-700 space-y-3">
      <div>
        <label className="text-xs text-gray-400 block mb-1">Model</label>
        <select
          value={settings.model}
          onChange={(e) => onChange({ ...settings, model: e.target.value })}
          className="w-full bg-gray-800 text-sm rounded px-2 py-1.5 border border-gray-600 focus:border-blue-500 focus:outline-none text-white"
        >
          {MODELS.map((m) => (
            <option key={m} value={m}>{m}</option>
          ))}
        </select>
      </div>
      <div className="flex gap-2">
        <div className="flex-1">
          <label className="text-xs text-gray-400 block mb-1">Retries</label>
          <input
            type="number"
            min={1}
            max={20}
            value={settings.maxRetries}
            onChange={(e) => onChange({ ...settings, maxRetries: Number(e.target.value) })}
            className="w-full bg-gray-800 text-sm rounded px-2 py-1.5 border border-gray-600 focus:border-blue-500 focus:outline-none text-white"
          />
        </div>
        <div className="flex-1">
          <label className="text-xs text-gray-400 block mb-1">Candidates</label>
          <input
            type="number"
            min={1}
            max={10}
            value={settings.maxCandidates}
            onChange={(e) => onChange({ ...settings, maxCandidates: Number(e.target.value) })}
            className="w-full bg-gray-800 text-sm rounded px-2 py-1.5 border border-gray-600 focus:border-blue-500 focus:outline-none text-white"
          />
        </div>
      </div>
    </div>
  );
}
