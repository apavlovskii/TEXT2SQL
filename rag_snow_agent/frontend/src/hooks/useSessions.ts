import { useCallback, useEffect, useState } from "react";
import { createSession, deleteSession, getSessions } from "../api/client";
import type { Session } from "../types";

export function useSessions() {
  const [sessions, setSessions] = useState<Session[]>([]);
  const [activeSessionId, setActiveSessionId] = useState<string | null>(null);
  const [dbId, setDbId] = useState("GA360");

  const refresh = useCallback(async () => {
    try {
      const list = await getSessions();
      setSessions(list);
    } catch (err) {
      console.error("Failed to load sessions", err);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const create = useCallback(
    async (db?: string) => {
      const session = await createSession(db || dbId);
      setSessions((prev) => [session, ...prev]);
      setActiveSessionId(session.id);
      return session;
    },
    [dbId],
  );

  const remove = useCallback(
    async (id: string) => {
      await deleteSession(id);
      setSessions((prev) => prev.filter((s) => s.id !== id));
      if (activeSessionId === id) setActiveSessionId(null);
    },
    [activeSessionId],
  );

  return {
    sessions,
    activeSessionId,
    setActiveSessionId,
    dbId,
    setDbId,
    createSession: create,
    deleteSession: remove,
    refreshSessions: refresh,
  };
}
