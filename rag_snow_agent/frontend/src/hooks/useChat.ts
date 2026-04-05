import { useCallback, useRef, useState } from "react";
import { getSession, streamChat } from "../api/client";
import type { AgentSettings, Message } from "../types";

export function useChat(
  sessionId: string | null,
  dbId: string,
  settings: AgentSettings,
  onSessionCreated?: (id: string) => void,
) {
  const [messages, setMessages] = useState<Message[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [streamStatus, setStreamStatus] = useState("");
  const cancelRef = useRef<(() => void) | null>(null);

  const loadHistory = useCallback(async (sid: string) => {
    try {
      const detail = await getSession(sid);
      setMessages(detail.messages);
    } catch (err) {
      console.error("Failed to load history", err);
    }
  }, []);

  const sendMessage = useCallback(
    async (text: string) => {
      if (!text.trim() || isLoading) return;

      const userMsg: Message = {
        id: `temp-${Date.now()}`,
        role: "user",
        content: text,
        timestamp: new Date().toISOString(),
      };
      setMessages((prev) => [...prev, userMsg]);
      setIsLoading(true);
      setStreamStatus("Connecting...");

      const cleanup = streamChat(
        {
          session_id: sessionId || undefined,
          message: text,
          db_id: dbId,
          model: settings.model,
          max_retries: settings.maxRetries,
          max_candidates: settings.maxCandidates,
          datasource: settings.datasource,
        },
        (status) => setStreamStatus(status),
        (response) => {
          if (!sessionId && response.session_id) {
            onSessionCreated?.(response.session_id);
          }
          const assistantMsg: Message = {
            id: response.message_id,
            role: "assistant",
            content: response.answer,
            sql: response.sql,
            results: response.results,
            metadata: response.metadata,
            error: response.error,
            execution_log: response.execution_log,
            timestamp: response.timestamp,
          };
          setMessages((prev) => [...prev, assistantMsg]);
        },
        (error) => {
          const errorMsg: Message = {
            id: `error-${Date.now()}`,
            role: "assistant",
            content: `Error: ${error}`,
            error,
            timestamp: new Date().toISOString(),
          };
          setMessages((prev) => [...prev, errorMsg]);
        },
        () => {
          setIsLoading(false);
          setStreamStatus("");
          cancelRef.current = null;
        },
      );

      cancelRef.current = cleanup;
    },
    [sessionId, dbId, settings, isLoading, onSessionCreated],
  );

  const cancel = useCallback(() => {
    if (cancelRef.current) {
      cancelRef.current();
      setIsLoading(false);
      setStreamStatus("");
      cancelRef.current = null;
      // Add a cancelled message
      setMessages((prev) => [
        ...prev,
        {
          id: `cancelled-${Date.now()}`,
          role: "assistant",
          content: "Query generation was cancelled.",
          timestamp: new Date().toISOString(),
        },
      ]);
    }
  }, []);

  return { messages, setMessages, isLoading, streamStatus, sendMessage, loadHistory, cancel };
}
