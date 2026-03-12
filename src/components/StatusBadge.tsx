import React from "react";

interface StatusBadgeProps {
  status: "running" | "active" | "inactive";
}

export default function StatusBadge({ status }: StatusBadgeProps) {
  if (status === "running") {
    return (
      <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-emerald-500/10 text-emerald-400 text-xs font-medium">
        <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse"></span>
        Running
      </span>
    );
  }

  if (status === "active") {
    return (
      <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-blue-500/10 text-blue-400 text-xs font-medium">
        <span className="w-1.5 h-1.5 rounded-full bg-blue-500"></span>
        Active
      </span>
    );
  }

  return (
    <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full bg-slate-500/10 text-slate-400 text-xs font-medium">
      <span className="w-1.5 h-1.5 rounded-full bg-slate-500"></span>
      Inactive
    </span>
  );
}
