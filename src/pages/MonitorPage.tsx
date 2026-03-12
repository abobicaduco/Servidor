import React, { useMemo } from "react";
import { useStatus } from "../hooks/useStatus";
import ProcessCard from "../components/ProcessCard";
import QueueTable from "../components/QueueTable";

function matchesSearch(name: string, query: string): boolean {
  if (!query.trim()) return true;
  return name.toLowerCase().includes(query.trim().toLowerCase());
}

export default function MonitorPage({ searchQuery = "" }: { searchQuery?: string }) {
  const status = useStatus();

  const runningFiltered = useMemo(() => {
    if (!status) return [];
    return status.running_processes.filter((p) =>
      matchesSearch(p.script_name.replace(/^\[FLOW\]\s*/, ""), searchQuery),
    );
  }, [status, searchQuery]);

  const queuedFiltered = useMemo(() => {
    if (!status) return [];
    return status.queued_processes.filter((q) =>
      matchesSearch(q.script_name, searchQuery),
    );
  }, [status, searchQuery]);

  if (!status) {
    return <div className="p-6 text-slate-400">Loading monitor...</div>;
  }

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <div className="grid grid-cols-3 gap-4 mb-8">
        <div className="bg-slate-800 border border-slate-700 rounded-lg p-4">
          <div className="text-sm text-slate-400 mb-1">Running Processes</div>
          <div className="text-3xl font-light text-slate-100">
            {status.running_count}
          </div>
        </div>
        <div className="bg-slate-800 border border-slate-700 rounded-lg p-4">
          <div className="text-sm text-slate-400 mb-1">Queued Processes</div>
          <div className="text-3xl font-light text-slate-100">
            {status.queued_count}
          </div>
        </div>
        <div className="bg-slate-800 border border-slate-700 rounded-lg p-4">
          <div className="text-sm text-slate-400 mb-1">Max Concurrent</div>
          <div className="text-3xl font-light text-slate-100">
            {status.max_concurrent}
          </div>
        </div>
      </div>

      {status.running_count === 0 && status.queued_count === 0 ? (
        <div className="flex flex-col items-center justify-center py-20 text-slate-500">
          <div className="w-16 h-16 rounded-full bg-slate-800 flex items-center justify-center mb-4">
            <span className="text-2xl">💤</span>
          </div>
          <p className="text-lg">No processes running.</p>
        </div>
      ) : (
        <>
          {runningFiltered.length > 0 && (
            <div className="mb-8">
              <h3 className="text-lg font-medium text-slate-100 mb-4">
                Running ({runningFiltered.length}
                {searchQuery.trim() ? ` of ${status.running_count}` : ""})
              </h3>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 2xl:grid-cols-5 gap-4">
                {runningFiltered.map((p) => (
                  <ProcessCard key={p.pid} process={p} />
                ))}
              </div>
            </div>
          )}

          {queuedFiltered.length > 0 && (
            <QueueTable queue={queuedFiltered} />
          )}

          {searchQuery.trim() &&
            runningFiltered.length === 0 &&
            queuedFiltered.length === 0 && (
              <p className="text-slate-500 py-4">
                Nenhum script encontrado para &quot;{searchQuery}&quot;.
              </p>
            )}
        </>
      )}
    </div>
  );
}
