import React from "react";
import { useStatus } from "../hooks/useStatus";
import { useAreas } from "../hooks/useAreas";
import { Activity, Clock, HeartPulse, List, Search, Zap } from "lucide-react";

interface SidebarProps {
  activeTab: string;
  setActiveTab: (tab: string) => void;
  searchQuery: string;
  setSearchQuery: (q: string) => void;
}

export default function Sidebar({
  activeTab,
  setActiveTab,
  searchQuery,
  setSearchQuery,
}: SidebarProps) {
  const status = useStatus();
  const areas = useAreas();

  const navItems = [
    { id: "monitor", label: "Monitor", icon: <Activity size={18} /> },
    { id: "history", label: "History", icon: <List size={18} /> },
    { id: "workflows", label: "Workflows", icon: <Zap size={18} /> },
    { id: "jobs", label: "Jobs", icon: <Clock size={18} /> },
    { id: "health", label: "Health", icon: <HeartPulse size={18} /> },
  ];

  return (
    <div className="w-[220px] bg-[#0B0F19] border-r border-slate-800 flex flex-col h-full text-slate-300">
      <div className="p-4 border-b border-slate-800">
        <h1 className="font-bold text-slate-100 flex items-center gap-2">
          <div className="w-6 h-6 bg-blue-600 rounded flex items-center justify-center text-xs">
            AS
          </div>
          Abobi Cron Server
        </h1>
      </div>

      <div className="flex-1 overflow-y-auto py-4">
        <div className="px-3 mb-3">
          <div className="relative">
            <Search
              size={16}
              className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500"
            />
            <input
              type="text"
              placeholder="Buscar scripts..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full pl-9 pr-3 py-2 bg-slate-800 border border-slate-700 rounded-lg text-sm text-slate-100 placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
          </div>
        </div>
        <div className="px-3 mb-2 text-xs font-semibold text-slate-500 uppercase tracking-wider">
          Navigation
        </div>
        <div className="space-y-1 px-2">
          {navItems.map((item) => (
            <button
              key={item.id}
              onClick={() => setActiveTab(item.id)}
              className={`w-full flex items-center justify-between px-3 py-2 rounded-lg text-sm transition-colors ${
                activeTab === item.id
                  ? "bg-blue-500/20 text-blue-400 border-l-2 border-blue-500"
                  : "hover:bg-slate-800/50 border-l-2 border-transparent"
              }`}
            >
              <div className="flex items-center gap-3">
                {item.icon}
                {item.label}
              </div>
              {item.id === "monitor" && status && status.running_count > 0 && (
                <div className="flex items-center gap-1.5 text-xs text-emerald-400 font-medium">
                  <span className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></span>
                  {status.running_count}
                </div>
              )}
            </button>
          ))}
        </div>

        <div className="px-3 mt-8 mb-2 text-xs font-semibold text-slate-500 uppercase tracking-wider">
          Areas
        </div>
        <div className="space-y-1 px-2">
          {Object.entries(areas).map(([areaName, scripts]: [string, any]) => {
            const runningInArea = scripts.filter(
              (s: any) => s.is_running,
            ).length;
            return (
              <button
                key={areaName}
                onClick={() => setActiveTab(`area_${areaName}`)}
                className={`w-full flex items-center justify-between px-3 py-2 rounded-lg text-sm transition-colors ${
                  activeTab === `area_${areaName}`
                    ? "bg-blue-500/20 text-blue-400 border-l-2 border-blue-500"
                    : "hover:bg-slate-800/50 border-l-2 border-transparent"
                }`}
              >
                <div className="truncate">{areaName}</div>
                <div className="flex items-center gap-1.5 text-xs">
                  {runningInArea > 0 ? (
                    <span className="text-emerald-400 font-medium flex items-center gap-1">
                      <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse"></span>
                      {runningInArea}
                    </span>
                  ) : (
                    <span className="text-slate-600">0</span>
                  )}
                </div>
              </button>
            );
          })}
        </div>
      </div>

      <div className="p-4 border-t border-slate-800 text-xs text-slate-500 flex items-center gap-2">
        <div
          className={`w-2 h-2 rounded-full ${status ? "bg-emerald-500" : "bg-red-500"}`}
        ></div>
        {status ? `${status.running_count} running` : "Disconnected"}
      </div>
    </div>
  );
}
