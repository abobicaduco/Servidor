export interface RunningProcess {
  pid: number;
  script_name: string;
  area_name: string;
  running_time_seconds: number;
  is_workflow: boolean;
  trigger_reason: "scheduled" | "manual" | "catchup" | "workflow";
}

export interface QueuedProcess {
  script_name: string;
  area_name?: string;
  priority_timestamp: number | string; // ISO string from API
  status?: string; // "waiting"
  position?: number;
}

export interface WorkflowStep {
  script: string;
  step: string;
  status: string;
}

export interface WorkflowState {
  active: boolean;
  name: string | null;
  current_script: string | null;
  progress: string | null; // "2/4"
  log: WorkflowStep[];
}

export interface StatusResponse {
  running_processes: RunningProcess[];
  queued_processes: QueuedProcess[];
  workflow_active: boolean;
  workflow_name: string | null;
  workflow_current_script: string | null;
  workflow_progress: string | null;
  workflow_log: WorkflowStep[];
  max_concurrent: number;
  running_count: number;
  queued_count: number;
}

export interface ScriptInfo {
  script_name: string;
  area_name: string;
  is_active: boolean;
  cron_schedule: number[];
  available_locally: boolean;
  path: string | null;
  is_running: boolean;
  emails_principal: string;
  emails_cc: string;
  move_file: boolean;
  movimentacao_financeira: string;
  interacao_cliente: string;
  tempo_manual: number;
}

export interface Workflow {
  workflow_name: string;
  scripts: string[];
  horarios: number[];
}

export interface ScheduledJob {
  id: string;
  name: string;
  next_run_br: string | null;
  trigger: string;
}

export interface HealthResponse {
  status: string;
  uptime_seconds: number;
  running: number;
  queued: number;
}

export interface ReloadResponse {
  status: "success" | "cooldown";
  wait_seconds?: number;
  script_count?: number;
  workflow_count?: number;
}
