import React from "react";
import { Workflow } from "../types";
import { runWorkflow } from "../api/client";
import { Play, ArrowRight } from "lucide-react";

interface WorkflowCardProps {
  key?: React.Key;
  workflow: Workflow;
  isActive: boolean;
}

export default function WorkflowCard({
  workflow,
  isActive,
}: WorkflowCardProps) {
  const handleRun = async () => {
    try {
      await runWorkflow(workflow.workflow_name);
    } catch (err) {
      console.error(err);
    }
  };

  return (
    <div className="bg-slate-800 border border-slate-700 rounded-lg p-5">
      <div className="flex justify-between items-start mb-4">
        <h3 className="text-lg font-medium text-slate-100">
          {workflow.workflow_name}
        </h3>
        <button
          onClick={handleRun}
          disabled={isActive}
          className="inline-flex items-center gap-1.5 px-3 py-1.5 bg-violet-500/10 text-violet-400 hover:bg-violet-500/20 disabled:opacity-50 disabled:cursor-not-allowed rounded transition-colors text-sm font-medium"
        >
          <Play size={16} />
          Run Now
        </button>
      </div>

      <div className="mb-4">
        <div className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2">
          Pipeline
        </div>
        <div className="flex flex-wrap items-center gap-2">
          {workflow.scripts.map((script, index) => (
            <React.Fragment key={script}>
              <div className="px-2 py-1 bg-slate-900 border border-slate-700 rounded font-mono text-sm text-slate-300">
                {script}
              </div>
              {index < workflow.scripts.length - 1 && (
                <ArrowRight size={14} className="text-slate-500" />
              )}
            </React.Fragment>
          ))}
        </div>
      </div>

      <div>
        <div className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2">
          Scheduled Hours
        </div>
        <div className="flex flex-wrap gap-2">
          {workflow.horarios.map((h) => (
            <span
              key={h}
              className="px-2 py-1 bg-slate-700 rounded text-xs text-slate-300"
            >
              {h.toString().padStart(2, "0")}h
            </span>
          ))}
        </div>
      </div>
    </div>
  );
}
