import React, { useState, useEffect } from "react";
import { HealthResponse } from "../types";
import { fetchHealth } from "../api/client";
import { Activity, Clock, Server, CheckCircle2 } from "lucide-react";

export default function HealthPage() {
  const [health, setHealth] = useState<HealthResponse | null>(null);

  useEffect(() => {
    const loadData = async () => {
      try {
        const data = await fetchHealth();
        setHealth(data);
      } catch (err) {
        console.error(err);
      }
    };

    loadData();
    const id = setInterval(loadData, 5000);
    return () => clearInterval(id);
  }, []);

  if (!health) {
    return <div className="p-6 text-slate-400">Loading health data...</div>;
  }

  const formatUptime = (seconds: number) => {
    const d = Math.floor(seconds / 86400);
    const h = Math.floor((seconds % 86400) / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    return `${d}d ${h}h ${m}m`;
  };

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <h2 className="text-xl font-medium text-slate-100 mb-6">Server Health</h2>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div className="bg-slate-800 border border-slate-700 rounded-lg p-6">
          <div className="flex items-center gap-3 mb-6">
            <Server className="text-blue-400" size={24} />
            <h3 className="text-lg font-medium text-slate-100">
              System Status
            </h3>
          </div>

          <div className="space-y-4">
            <div className="flex justify-between items-center py-2 border-b border-slate-700/50">
              <span className="text-slate-400">Status</span>
              <span className="flex items-center gap-1.5 text-emerald-400 font-medium">
                <CheckCircle2 size={16} />
                {health.status.toUpperCase()}
              </span>
            </div>
            <div className="flex justify-between items-center py-2 border-b border-slate-700/50">
              <span className="text-slate-400 flex items-center gap-2">
                <Clock size={16} /> Uptime
              </span>
              <span className="text-slate-200 font-mono">
                {formatUptime(health.uptime_seconds)}
              </span>
            </div>
            <div className="flex justify-between items-center py-2 border-b border-slate-700/50">
              <span className="text-slate-400 flex items-center gap-2">
                <Activity size={16} /> Running Processes
              </span>
              <span className="text-slate-200 font-mono">{health.running}</span>
            </div>
            <div className="flex justify-between items-center py-2">
              <span className="text-slate-400">Queued Tasks</span>
              <span className="text-slate-200 font-mono">{health.queued}</span>
            </div>
          </div>
        </div>

        <div className="bg-slate-800 border border-slate-700 rounded-lg p-6">
          <h3 className="text-lg font-medium text-slate-100 mb-4">
            Environment Info
          </h3>
          <div className="space-y-3 text-sm text-slate-300">
            <p>The server is running in a 24x7 production environment.</p>
            <p>
              Timezone is strictly enforced to{" "}
              <strong>America/Sao_Paulo</strong>.
            </p>
            <p>
              All process executions are isolated and managed via{" "}
              <code>subprocess.Popen</code>.
            </p>
            <p>
              Process termination is handled recursively via <code>psutil</code>{" "}
              to prevent zombie processes on Windows.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
