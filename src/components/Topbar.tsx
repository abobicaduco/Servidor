import React from "react";
import ReloadButton from "./ReloadButton";

interface TopbarProps {
  title: string;
}

export default function Topbar({ title }: TopbarProps) {
  return (
    <div className="h-14 border-b border-slate-800 bg-slate-900/50 backdrop-blur flex items-center justify-between px-6 sticky top-0 z-10">
      <h2 className="text-lg font-medium text-slate-100 capitalize">
        {title.replace("area_", "")}
      </h2>
      <ReloadButton />
    </div>
  );
}
