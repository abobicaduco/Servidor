import React, { useState, useEffect } from "react";
import { RunningProcess } from "../types";
import { killProcess } from "../api/client";
import { Activity, Clock, Tag } from "lucide-react";

interface ProcessCardProps {
  key?: React.Key;
  process: RunningProcess;
}

export default function ProcessCard({ process }: ProcessCardProps) {
  const [elapsed, setElapsed] = useState(process.running_time_seconds);

  useEffect(() => {
    setElapsed(process.running_time_seconds);
    const id = setInterval(() => {
      setElapsed((prev) => prev + 1);
    }, 1000);
    return () => clearInterval(id);
  }, [process.running_time_seconds]);

  const formatTime = (seconds: number) => {
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = seconds % 60;
    if (h > 0) return `${h}h ${m}m ${s}s`;
    if (m > 0) return `${m}m ${s}s`;
    return `${s}s`;
  };

  const handleKill = async () => {
    try {
      await killProcess(process.pid);
    } catch (err) {
      console.error(err);
    }
  };

  const reasonColors = {
    scheduled: "bg-slate-700 text-slate-300",
    manual: "bg-indigo-500/20 text-indigo-400",
    catchup: "bg-orange-500/20 text-orange-400",
    workflow: "bg-violet-500/20 text-violet-400",
  };

  return (
    <div className="bg-slate-800 border border-slate-700 rounded-lg overflow-hidden flex flex-col">
      <div className="p-4 flex-1">
        <div className="flex justify-between items-start mb-3">
          <div
            className="font-mono text-sm text-slate-100 truncate pr-2"
            title={process.script_name}
          >
            {process.script_name}
          </div>
          <div
            className={`text-xs px-2 py-0.5 rounded-full whitespace-nowrap ${reasonColors[process.trigger_reason]}`}
          >
            {process.trigger_reason}
          </div>
        </div>

        <div className="space-y-2 text-sm text-slate-400">
          <div className="flex items-center gap-2">
            <Tag size={14} />
            <span className="capitalize">{process.area_name}</span>
          </div>
          <div className="flex items-center gap-2">
            <Activity size={14} />
            <span className="font-mono">PID: {process.pid}</span>
          </div>
          <div className="flex items-center gap-2 text-[#10B981]">
            <Clock size={14} />
            <span className="font-mono">{formatTime(elapsed)}</span>
          </div>
        </div>
      </div>

      <button
        onClick={handleKill}
        className="w-full py-2 bg-slate-800 hover:bg-red-500/20 text-slate-400 hover:text-red-500 border-t border-slate-700 transition-colors text-sm font-medium"
      >
        Encerrar Processo
      </button>
    </div>
  );
}
