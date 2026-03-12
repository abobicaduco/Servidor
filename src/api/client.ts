import {
  StatusResponse,
  ScriptInfo,
  ScheduledJob,
  HealthResponse,
  Workflow,
  WorkflowState,
  ReloadResponse,
} from "../types";

const BASE = ""; // Same origin in production; Vite proxy handles /api in dev

async function request<T>(url: string, options?: RequestInit): Promise<T> {
  const res = await fetch(BASE + url, options);
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${url}`);
  return res.json() as Promise<T>;
}

export const fetchStatus = () => request<StatusResponse>("/api/status");
export const fetchScripts = () => request<ScriptInfo[]>("/api/scripts");
export const fetchAreas = () =>
  request<Record<string, ScriptInfo[]>>("/api/areas");
export const fetchJobs = () => request<ScheduledJob[]>("/api/jobs");
export const fetchHealth = () => request<HealthResponse>("/api/health");
export const fetchWorkflows = () =>
  request<{ workflows: Workflow[]; state: WorkflowState }>("/api/workflows");
export const reloadConfig = () =>
  request<ReloadResponse>("/api/reload", { method: "POST" });
export const killProcess = (pid: number) =>
  request<{ status: string; message: string }>(`/api/kill/${pid}`, {
    method: "POST",
  });
export const runScript = (name: string) =>
  request<{ status: string; message: string }>(
    `/api/run/${encodeURIComponent(name)}`,
    { method: "POST" },
  );
export const runWorkflow = (name: string) =>
  request<{ status: string; message: string }>(
    `/api/workflows/run/${encodeURIComponent(name)}`,
    { method: "POST" },
  );
