import React from "react";
import { QueuedProcess } from "../types";

interface QueueTableProps {
  queue: QueuedProcess[];
}

export default function QueueTable({ queue }: QueueTableProps) {
  if (queue.length === 0) return null;

  return (
    <div className="mt-8">
      <h3 className="text-lg font-medium text-slate-100 mb-4">
        Queue ({queue.length})
      </h3>
      <div className="bg-slate-800 border border-slate-700 rounded-lg overflow-hidden">
        <table className="w-full text-left text-sm text-slate-300">
          <thead className="bg-slate-900/50 text-slate-400 border-b border-slate-700">
            <tr>
              <th className="px-4 py-3 font-medium">Pos</th>
              <th className="px-4 py-3 font-medium">Script</th>
              <th className="px-4 py-3 font-medium">Area</th>
              <th className="px-4 py-3 font-medium">Priority Timestamp</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-700/50">
            {queue.map((item) => (
              <tr
                key={`${item.script_name}-${item.position}`}
                className="hover:bg-slate-700/20"
              >
                <td className="px-4 py-3 text-slate-500">{item.position}</td>
                <td className="px-4 py-3 font-mono text-slate-200">
                  {item.script_name}
                </td>
                <td className="px-4 py-3 capitalize">{item.area_name}</td>
                <td className="px-4 py-3 font-mono text-slate-400">
                  {new Date(
                    typeof item.priority_timestamp === "string"
                      ? item.priority_timestamp
                      : item.priority_timestamp * 1000,
                  ).toLocaleString()}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
