import React, { useState, useEffect } from "react";
import { ScheduledJob } from "../types";
import { fetchJobs } from "../api/client";
import { Calendar, Clock } from "lucide-react";

export default function JobsPage() {
  const [jobs, setJobs] = useState<ScheduledJob[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadData = async () => {
      try {
        const data = await fetchJobs();
        setJobs(data);
      } catch (err) {
        console.error(err);
      } finally {
        setLoading(false);
      }
    };

    loadData();
    const id = setInterval(loadData, 30000);
    return () => clearInterval(id);
  }, []);

  if (loading) {
    return <div className="p-6 text-slate-400">Loading jobs...</div>;
  }

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <div className="mb-6 flex items-center justify-between">
        <h2 className="text-xl font-medium text-slate-100">Scheduled Jobs</h2>
        <div className="text-sm text-slate-400">{jobs.length} active jobs</div>
      </div>

      <div className="bg-slate-800 border border-slate-700 rounded-lg overflow-hidden">
        <table className="w-full text-left text-sm text-slate-300">
          <thead className="bg-slate-900/50 text-slate-400 border-b border-slate-700">
            <tr>
              <th className="px-4 py-3 font-medium">Job Name</th>
              <th className="px-4 py-3 font-medium">Trigger</th>
              <th className="px-4 py-3 font-medium">Next Run (São Paulo)</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-700/50">
            {jobs.map((job) => (
              <tr key={job.id} className="hover:bg-slate-700/20">
                <td className="px-4 py-3 font-mono text-slate-200">
                  {job.name}
                </td>
                <td className="px-4 py-3">
                  <span className="px-2 py-1 bg-slate-700 rounded text-xs text-slate-300">
                    {job.trigger}
                  </span>
                </td>
                <td className="px-4 py-3">
                  {job.next_run_br ? (
                    <div className="flex items-center gap-2 text-slate-300">
                      <Calendar size={14} className="text-slate-500" />
                      {new Date(job.next_run_br).toLocaleDateString()}
                      <Clock size={14} className="text-slate-500 ml-2" />
                      {new Date(job.next_run_br).toLocaleTimeString()}
                    </div>
                  ) : (
                    <span className="text-slate-500">Not scheduled</span>
                  )}
                </td>
              </tr>
            ))}
            {jobs.length === 0 && (
              <tr>
                <td
                  colSpan={3}
                  className="px-4 py-8 text-center text-slate-500"
                >
                  No jobs scheduled
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
