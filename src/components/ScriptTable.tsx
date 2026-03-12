import React from "react";
import { ScriptInfo } from "../types";
import { runScript } from "../api/client";
import StatusBadge from "./StatusBadge";
import { Play, AlertCircle } from "lucide-react";

interface ScriptTableProps {
  scripts: ScriptInfo[];
}

export default function ScriptTable({ scripts }: ScriptTableProps) {
  const handleRun = async (name: string) => {
    try {
      await runScript(name);
    } catch (err) {
      console.error(err);
    }
  };

  return (
    <div className="bg-slate-800 border border-slate-700 rounded-lg overflow-hidden">
      <table className="w-full text-left text-sm text-slate-300">
        <thead className="bg-slate-900/50 text-slate-400 border-b border-slate-700">
          <tr>
            <th className="px-4 py-3 font-medium">Script Name</th>
            <th className="px-4 py-3 font-medium">Status</th>
            <th className="px-4 py-3 font-medium">Scheduled Hours</th>
            <th className="px-4 py-3 font-medium text-center">Fin. / Client</th>
            <th className="px-4 py-3 font-medium text-right">Actions</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-700/50">
          {scripts.map((script) => {
            const isMissing = !script.available_locally;
            const isInactive = !script.is_active;
            const isRunning = script.is_running;

            return (
              <tr
                key={script.script_name}
                className={`hover:bg-slate-700/20 ${isInactive ? "opacity-50" : ""}`}
              >
                <td className="px-4 py-3 font-mono text-slate-200">
                  {script.script_name}
                  {isMissing && (
                    <div className="flex items-center gap-1 text-xs text-red-400 mt-1 font-sans">
                      <AlertCircle size={12} /> Not found on disk
                    </div>
                  )}
                </td>
                <td className="px-4 py-3">
                  <StatusBadge
                    status={
                      isRunning ? "running" : isInactive ? "inactive" : "active"
                    }
                  />
                </td>
                <td className="px-4 py-3">
                  <div className="flex flex-wrap gap-1">
                    {script.cron_schedule.length > 0 ? (
                      script.cron_schedule.map((h) => (
                        <span
                          key={h}
                          className="px-1.5 py-0.5 bg-slate-700 rounded text-xs text-slate-300"
                        >
                          {h.toString().padStart(2, "0")}h
                        </span>
                      ))
                    ) : (
                      <span className="text-slate-500">—</span>
                    )}
                  </div>
                </td>
                <td className="px-4 py-3 text-center">
                  <div className="flex items-center justify-center gap-2">
                    {script.movimentacao_financeira === "sim" && (
                      <span title="Financial">💰</span>
                    )}
                    {script.interacao_cliente === "sim" && (
                      <span title="Client">👤</span>
                    )}
                    {script.movimentacao_financeira !== "sim" &&
                      script.interacao_cliente !== "sim" && (
                        <span className="text-slate-600">—</span>
                      )}
                  </div>
                </td>
                <td className="px-4 py-3 text-right">
                  <button
                    onClick={() => handleRun(script.script_name)}
                    disabled={isMissing || isInactive || isRunning}
                    className="inline-flex items-center gap-1.5 px-3 py-1.5 bg-blue-500/10 text-blue-400 hover:bg-blue-500/20 disabled:opacity-50 disabled:cursor-not-allowed rounded transition-colors text-xs font-medium"
                  >
                    <Play size={14} />
                    Run Now
                  </button>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
