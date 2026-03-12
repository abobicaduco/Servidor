import { useState, useEffect } from "react";
import { ScriptInfo } from "../types";
import { fetchAreas } from "../api/client";

export function useAreas() {
  const [areas, setAreas] = useState<Record<string, ScriptInfo[]>>({});
  useEffect(() => {
    const fetch_ = () => fetchAreas().then(setAreas).catch(console.error);
    fetch_();
    const id = setInterval(fetch_, 30000);
    return () => clearInterval(id);
  }, []);
  return areas;
}
