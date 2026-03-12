import React from "react";
import { WorkflowState } from "../types";
import { CheckCircle2, Circle, Loader2, XCircle } from "lucide-react";

interface WorkflowProgressProps {
  state: WorkflowState;
}

export default function WorkflowProgress({ state }: WorkflowProgressProps) {
  if (!state.active) return null;

  const getProgressPercentage = () => {
    if (!state.progress) return 0;
    const [current, total] = state.progress.split("/").map(Number);
    if (!total) return 0;
    return Math.round(((current - 1) / total) * 100);
  };

  return (
    <div className="bg-slate-800 border border-violet-500/30 rounded-lg p-5 mb-6 relative overflow-hidden">
      <div className="absolute top-0 left-0 h-1 bg-violet-500/20 w-full">
        <div
          className="h-full bg-violet-500 transition-all duration-500 ease-out"
          style={{ width: `${getProgressPercentage()}%` }}
        />
      </div>

      <div className="flex justify-between items-center mb-4 mt-2">
        <div className="flex items-center gap-3">
          <h3 className="text-lg font-medium text-slate-100">{state.name}</h3>
          <span className="px-2 py-0.5 bg-violet-500/20 text-violet-400 text-xs rounded-full font-medium flex items-center gap-1.5">
            <Loader2 size={12} className="animate-spin" />
            Running...
          </span>
        </div>
        <div className="text-sm font-medium text-slate-400">
          Step {state.progress}
        </div>
      </div>

      <div className="space-y-3">
        {state.log.map((entry, index) => {
          const isError =
            entry.status.includes("error") ||
            entry.status.includes("exception");
          const isNotFound = entry.status === "not_found";
          const isSuccess = entry.status === "success";

          return (
            <div key={index} className="flex items-center gap-3 text-sm">
              {isSuccess ? (
                <CheckCircle2 size={16} className="text-emerald-500" />
              ) : isError ? (
                <XCircle size={16} className="text-red-500" />
              ) : isNotFound ? (
                <Circle size={16} className="text-slate-500" />
              ) : (
                <Loader2 size={16} className="text-violet-400 animate-spin" />
              )}

              <span className="font-mono text-slate-300 w-48 truncate">
                {entry.script}
              </span>
              <span
                className={`capitalize ${
                  isSuccess
                    ? "text-emerald-400"
                    : isError
                      ? "text-red-400"
                      : isNotFound
                        ? "text-slate-500"
                        : "text-violet-400"
                }`}
              >
                {entry.status}
              </span>
            </div>
          );
        })}

        {state.current_script &&
          !state.log.find((l) => l.script === state.current_script) && (
            <div className="flex items-center gap-3 text-sm">
              <Loader2 size={16} className="text-violet-400 animate-spin" />
              <span className="font-mono text-slate-300 w-48 truncate">
                {state.current_script}
              </span>
              <span className="text-violet-400 animate-pulse">
                executing...
              </span>
            </div>
          )}
      </div>
    </div>
  );
}
