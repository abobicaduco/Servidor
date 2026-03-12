import React, { useState } from "react";
import Sidebar from "./components/Sidebar";
import Topbar from "./components/Topbar";
import MonitorPage from "./pages/MonitorPage";
import AreaPage from "./pages/AreaPage";
import WorkflowsPage from "./pages/WorkflowsPage";
import JobsPage from "./pages/JobsPage";
import HealthPage from "./pages/HealthPage";

export default function App() {
  const [activeTab, setActiveTab] = useState("monitor");
  const [searchQuery, setSearchQuery] = useState("");

  const renderContent = () => {
    if (activeTab === "monitor") return <MonitorPage searchQuery={searchQuery} />;
    if (activeTab === "workflows") return <WorkflowsPage />;
    if (activeTab === "jobs") return <JobsPage />;
    if (activeTab === "health") return <HealthPage />;
    if (activeTab === "history")
      return <div className="p-6 text-slate-400">History coming soon...</div>;

    if (activeTab.startsWith("area_")) {
      const areaName = activeTab.replace("area_", "");
      return <AreaPage areaName={areaName} searchQuery={searchQuery} />;
    }

    return <div className="p-6 text-slate-400">Page not found</div>;
  };

  const getTitle = () => {
    if (activeTab.startsWith("area_")) return activeTab.replace("area_", "");
    return activeTab;
  };

  return (
    <div className="flex h-screen bg-[#0F172A] text-slate-100 overflow-hidden font-sans">
      <Sidebar
        activeTab={activeTab}
        setActiveTab={setActiveTab}
        searchQuery={searchQuery}
        setSearchQuery={setSearchQuery}
      />

      <div className="flex-1 flex flex-col overflow-hidden">
        <Topbar title={getTitle()} />

        <main className="flex-1 overflow-y-auto">{renderContent()}</main>
      </div>
    </div>
  );
}
