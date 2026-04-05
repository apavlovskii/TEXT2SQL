import type { ChatRequest, ChatResponse, HealthInfo, SchemaInfo, Session, SessionDetail } from "../types";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${API_URL}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API error ${res.status}: ${text}`);
  }
  return res.json();
}

export const getHealth = () => request<HealthInfo>("/api/health");
export const getSessions = () => request<Session[]>("/api/sessions");
export const createSession = (db_id: string, name?: string) =>
  request<Session>("/api/sessions", { method: "POST", body: JSON.stringify({ db_id, name }) });
export const getSession = (id: string) => request<SessionDetail>(`/api/sessions/${id}`);
export const deleteSession = (id: string) => request<void>(`/api/sessions/${id}`, { method: "DELETE" });
export const getSchema = (db_id: string) => request<SchemaInfo>(`/api/schema/${db_id}`);

export const postChat = (req: ChatRequest) =>
  request<ChatResponse>("/api/chat", { method: "POST", body: JSON.stringify(req) });

export function streamChat(
  req: ChatRequest,
  onThinking: (status: string) => void,
  onResult: (data: ChatResponse) => void,
  onError: (error: string) => void,
  onDone: () => void,
): () => void {
  const params = new URLSearchParams({
    message: req.message,
    db_id: req.db_id,
    model: req.model,
    max_retries: String(req.max_retries),
    max_candidates: String(req.max_candidates),
    datasource: req.datasource,
  });
  if (req.session_id) params.set("session_id", req.session_id);

  const source = new EventSource(`${API_URL}/api/chat/stream?${params}`);

  source.addEventListener("thinking", (e) => {
    const data = JSON.parse(e.data);
    onThinking(data.status);
  });
  source.addEventListener("result", (e) => {
    onResult(JSON.parse(e.data) as ChatResponse);
  });
  source.addEventListener("error", (e) => {
    try {
      const data = JSON.parse((e as MessageEvent).data);
      onError(data.error || "Unknown error");
    } catch {
      onError("Connection error");
    }
  });
  source.addEventListener("done", () => {
    source.close();
    onDone();
  });

  return () => source.close();
}
