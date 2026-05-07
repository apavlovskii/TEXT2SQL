import { useCallback, useEffect, useMemo, useState } from "react";
import { getDatabases, getHealth } from "./api/client";
import { ChatView } from "./components/ChatView";
import { ExecutionLogPanel } from "./components/ExecutionLogPanel";
import { SessionSidebar } from "./components/SessionSidebar";
import { useChat } from "./hooks/useChat";
import { useSessions } from "./hooks/useSessions";
import type { AgentSettings } from "./types";

function App() {
  const [availableDbs, setAvailableDbs] = useState<string[]>(["GA360", "GA4", "PATENTS", "PATENTS_GOOGLE"]);
  const [agentSettings, setAgentSettings] = useState<AgentSettings>({
    model: "claude-sonnet-4-5",
    maxRetries: 10,
    maxCandidates: 2,
    datasource: "sqlite",
  });
  const [logPanelOpen, setLogPanelOpen] = useState(false);

  const {
    sessions,
    activeSessionId,
    setActiveSessionId,
    dbId,
    setDbId,
    createSession,
    deleteSession,
    refreshSessions,
  } = useSessions();

  const handleSessionCreated = useCallback(
    (id: string) => {
      setActiveSessionId(id);
      refreshSessions();
    },
    [setActiveSessionId, refreshSessions],
  );

  const { messages, setMessages, isLoading, streamStatus, sendMessage, loadHistory, cancel } =
    useChat(activeSessionId, dbId, agentSettings, handleSessionCreated);

  // Load initial health info
  useEffect(() => {
    getHealth()
      .then((h) => {
        setAvailableDbs(h.available_databases);
        setAgentSettings((prev) => ({ ...prev, datasource: h.datasource }));
      })
      .catch(() => {});
  }, []);

  // When datasource changes, fetch databases for that datasource
  const handleDatasourceChange = useCallback(
    async (datasource: string) => {
      setAgentSettings((prev) => ({ ...prev, datasource }));
      try {
        const result = await getDatabases(datasource);
        setAvailableDbs(result.databases);
        // Select first database if current one isn't available
        if (result.databases.length > 0 && !result.databases.includes(dbId)) {
          setDbId(result.databases[0]);
        }
      } catch {
        // Keep existing list on error
      }
    },
    [dbId, setDbId],
  );

  // Load history when session changes
  useEffect(() => {
    if (activeSessionId) {
      loadHistory(activeSessionId);
    } else {
      setMessages([]);
    }
  }, [activeSessionId, loadHistory, setMessages]);

  const handleNewSession = useCallback(async () => {
    await createSession(dbId);
    setMessages([]);
  }, [createSession, dbId, setMessages]);

  // Collect execution logs from assistant messages
  const allLogs = useMemo(() => {
    const logs: string[] = [];
    for (const msg of messages) {
      if (msg.role === "assistant" && msg.execution_log && msg.execution_log.length > 0) {
        logs.push(`── ${msg.content.slice(0, 60)}${msg.content.length > 60 ? "..." : ""} ──`);
        logs.push(...msg.execution_log);
      }
    }
    return logs;
  }, [messages]);

  return (
    <div className="flex h-screen bg-white">
      <SessionSidebar
        sessions={sessions}
        activeSessionId={activeSessionId}
        dbId={dbId}
        availableDbs={availableDbs}
        settings={agentSettings}
        onSelectSession={setActiveSessionId}
        onNewSession={handleNewSession}
        onDeleteSession={deleteSession}
        onDbChange={setDbId}
        onDatasourceChange={handleDatasourceChange}
        onSettingsChange={setAgentSettings}
      />
      <div className="flex-1 flex flex-col relative">
        <ChatView
          messages={messages}
          isLoading={isLoading}
          streamStatus={streamStatus}
          onSend={sendMessage}
          onCancel={cancel}
        />
      </div>
      <ExecutionLogPanel
        log={allLogs}
        open={logPanelOpen}
        onToggle={() => setLogPanelOpen((prev) => !prev)}
      />
    </div>
  );
}

export default App;
