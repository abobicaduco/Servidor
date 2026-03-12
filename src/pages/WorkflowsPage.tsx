import React from "react";
import { useWorkflows } from "../hooks/useWorkflows";
import WorkflowCard from "../components/WorkflowCard";
import WorkflowProgress from "../components/WorkflowProgress";

export default function WorkflowsPage() {
  const data = useWorkflows();

  if (!data) {
    return <div className="p-6 text-slate-400">Loading workflows...</div>;
  }

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <WorkflowProgress state={data.state} />

      <div className="mb-6 flex items-center justify-between">
        <h2 className="text-xl font-medium text-slate-100">
          Configured Workflows
        </h2>
        <div className="text-sm text-slate-400">
          {data.workflows.length} workflows
        </div>
      </div>

      {data.workflows.length === 0 ? (
        <div className="bg-slate-800 border border-slate-700 rounded-lg p-8 text-center text-slate-400">
          No workflows defined in workflows.xlsx
        </div>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4">
          {data.workflows.map((w) => (
            <WorkflowCard
              key={w.workflow_name}
              workflow={w}
              isActive={data.state.active}
            />
          ))}
        </div>
      )}
    </div>
  );
}
