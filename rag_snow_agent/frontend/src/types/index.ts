export interface ChatRequest {
  session_id?: string;
  message: string;
  db_id: string;
  model: string;
  max_retries: number;
  max_candidates: number;
  datasource: string;
}

export interface QueryResult {
  columns: string[];
  rows: any[][];
  row_count: number;
  truncated: boolean;
}

export interface ExecutionMetadata {
  elapsed_ms: number | null;
  llm_calls: number;
  repair_count: number;
  candidate_count: number;
  model: string;
  datasource: string;
}

export interface ChatResponse {
  session_id: string;
  message_id: string;
  answer: string;
  sql?: string | null;
  results?: QueryResult | null;
  metadata?: ExecutionMetadata | null;
  error?: string | null;
  execution_log: string[];
  timestamp: string;
}

export interface Message {
  id: string;
  role: "user" | "assistant";
  content: string;
  sql?: string | null;
  results?: QueryResult | null;
  metadata?: ExecutionMetadata | null;
  error?: string | null;
  execution_log?: string[];
  timestamp: string;
}

export interface Session {
  id: string;
  name: string;
  db_id: string;
  created_at: string;
  updated_at: string;
  message_count: number;
}

export interface SessionDetail extends Session {
  messages: Message[];
}

export interface HealthInfo {
  status: string;
  datasource: string;
  available_databases: string[];
  agent_ready: boolean;
  debug_mode: boolean;
  version: string;
}

export interface SchemaTable {
  qualified_name: string;
  comment?: string | null;
  columns: SchemaColumn[];
}

export interface SchemaColumn {
  name: string;
  type: string;
  comment?: string | null;
}

export interface SchemaInfo {
  db_id: string;
  tables: SchemaTable[];
}

export interface AgentSettings {
  model: string;
  maxRetries: number;
  maxCandidates: number;
  datasource: string;
}
