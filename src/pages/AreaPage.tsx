import React, { useState, useEffect, useMemo } from "react";
import { ScriptInfo } from "../types";
import { fetchScripts } from "../api/client";
import ScriptTable from "../components/ScriptTable";

function matchesSearch(name: string, query: string): boolean {
  if (!query.trim()) return true;
  return name.toLowerCase().includes(query.trim().toLowerCase());
}

interface AreaPageProps {
  areaName: string;
  searchQuery?: string;
}

export default function AreaPage({ areaName, searchQuery = "" }: AreaPageProps) {
  const [scripts, setScripts] = useState<ScriptInfo[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadData = async () => {
      try {
        const allScripts = await fetchScripts();
        setScripts(allScripts.filter((s) => s.area_name === areaName));
      } catch (err) {
        console.error(err);
      } finally {
        setLoading(false);
      }
    };

    setLoading(true);
    loadData();

    const id = setInterval(loadData, 30000);
    return () => clearInterval(id);
  }, [areaName]);

  const filteredScripts = useMemo(
    () => scripts.filter((s) => matchesSearch(s.script_name, searchQuery)),
    [scripts, searchQuery],
  );

  if (loading) {
    return <div className="p-6 text-slate-400">Loading area data...</div>;
  }

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <div className="mb-6 flex items-center justify-between">
        <h2 className="text-xl font-medium text-slate-100 capitalize">
          {areaName} Scripts
        </h2>
        <div className="text-sm text-slate-400">
          {filteredScripts.length}
          {searchQuery.trim() ? ` of ${scripts.length}` : ""} scripts
        </div>
      </div>

      <ScriptTable scripts={filteredScripts} />
      {searchQuery.trim() && filteredScripts.length === 0 && (
        <p className="text-slate-500 py-4">
          Nenhum script encontrado para &quot;{searchQuery}&quot;.
        </p>
      )}
    </div>
  );
}
