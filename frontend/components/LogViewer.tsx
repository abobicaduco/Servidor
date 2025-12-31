
import React from 'react';
import { ExecutionLog } from '../types';
import { ActivityIcon } from './Icons';

interface LogViewerProps {
  logs: ExecutionLog[];
}

const LogViewer: React.FC<LogViewerProps> = ({ logs }) => {
  return (
    <div className="bg-slate-900 border border-slate-800 rounded-2xl overflow-hidden shadow-xl">
      <div className="px-6 py-4 border-b border-slate-800 bg-slate-800/50 flex items-center gap-3">
        <ActivityIcon />
        <h2 className="text-lg font-bold text-white">Execution Logs</h2>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-left border-collapse">
          <thead>
            <tr className="bg-slate-900/50 text-slate-500 text-[10px] uppercase tracking-widest font-bold">
              <th className="px-6 py-4 border-b border-slate-800">Job Name</th>
              <th className="px-6 py-4 border-b border-slate-800">Timestamp</th>
              <th className="px-6 py-4 border-b border-slate-800">Duration</th>
              <th className="px-6 py-4 border-b border-slate-800">Status</th>
              <th className="px-6 py-4 border-b border-slate-800">Output snippet</th>
            </tr>
          </thead>
          <tbody className="text-slate-300 text-sm">
            {logs.length === 0 ? (
              <tr>
                <td colSpan={5} className="px-6 py-10 text-center text-slate-500 italic">No execution logs found.</td>
              </tr>
            ) : (
              logs.map(log => (
                <tr key={log.id} className="hover:bg-slate-800/30 transition-colors border-b border-slate-800/50">
                  <td className="px-6 py-4 font-medium text-slate-200">{log.jobName}</td>
                  <td className="px-6 py-4 text-slate-400 font-mono text-xs">{log.timestamp}</td>
                  <td className="px-6 py-4 text-slate-400">{log.duration}</td>
                  <td className="px-6 py-4">
                    <span className={`px-2 py-0.5 rounded text-[10px] font-bold uppercase ${log.status === 'SUCCESS' ? 'bg-green-500/10 text-green-400' : 'bg-red-500/10 text-red-400'}`}>
                      {log.status}
                    </span>
                  </td>
                  <td className="px-6 py-4 font-mono text-[11px] text-slate-500 truncate max-w-xs">{log.output}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default LogViewer;
