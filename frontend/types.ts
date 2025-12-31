
export enum JobStatus {
  RUNNING = 'RUNNING',
  SUCCESS = 'SUCCESS',
  ERROR = 'ERROR',
  FAILED = 'FAILED', // Alias for ERROR
  DISABLED = 'DISABLED',
  NO_DATA = 'NO_DATA',
  SCHEDULED = 'SCHEDULED',
  IDLE = 'IDLE'
}

export enum JobType {
  PYTHON = 'PYTHON',
  BIGQUERY = 'BIGQUERY'
}

export type CronType = 'ALL' | 'LIST' | 'MANUAL';

export interface AutomationScript {
  id: string;
  script_name: string;
  area_name: string;
  path: string;
  cron_schedule: string; // Texto original do Excel/Config
  cron_type: CronType;
  cron_hours: number[]; // Horas específicas se type for LIST
  active: boolean;
  status: JobStatus;
  last_execution?: {
    timestamp: string;
    duration: string;
    status: string;
  };
  daily_runs: number;
  target_runs: number;
}

export interface CronJob {
  id: string;
  name: string;
  description: string;
  schedule: string;
  type: JobType;
  code: string;
  tags: string[];
  status: JobStatus;
}

export interface ExecutionLog {
  id: string;
  jobName: string;
  timestamp: string;
  duration: string;
  status: string;
  output: string;
}

export interface SystemStats {
  lastDiscovery: number;
  nextDiscovery: number;
  lastBqSync: number;
  nextBqSync: number;
  bqVerified: boolean;
  concurrentRunning: number;
  queueSize: number;
}
