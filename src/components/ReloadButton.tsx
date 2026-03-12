import React, { useState, useEffect } from "react";
import { reloadConfig } from "../api/client";
import { RefreshCw } from "lucide-react";

export default function ReloadButton() {
  const [countdown, setCountdown] = useState<number>(0);

  const handleClick = async () => {
    try {
      const res = await reloadConfig();
      if (res.status === "cooldown") {
        setCountdown(res.wait_seconds ?? 60);
        return;
      }
      setCountdown(60);
    } catch (err) {
      console.error(err);
    }
  };

  useEffect(() => {
    if (countdown <= 0) return;
    const id = setInterval(
      () => setCountdown((c) => (c <= 1 ? 0 : c - 1)),
      1000,
    );
    return () => clearInterval(id);
  }, [countdown]);

  return (
    <button
      onClick={handleClick}
      disabled={countdown > 0}
      className="inline-flex items-center gap-2 px-3 py-1.5 bg-slate-800 hover:bg-slate-700 border border-slate-700 rounded-lg text-sm font-medium text-slate-200 transition-colors disabled:opacity-50 disabled:cursor-not-allowed focus:ring-2 focus:ring-blue-500 focus:ring-offset-0 focus:ring-offset-slate-900"
    >
      <RefreshCw size={14} className={countdown > 0 ? "animate-spin" : ""} />
      {countdown > 0
        ? `Recarregando... (Aguarde ${countdown}s)`
        : "Recarregar Configurações"}
    </button>
  );
}
