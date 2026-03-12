import { useState, useEffect } from "react";
import { Workflow, WorkflowState } from "../types";
import { fetchWorkflows } from "../api/client";

export function useWorkflows() {
  const [data, setData] = useState<{
    workflows: Workflow[];
    state: WorkflowState;
  } | null>(null);
  useEffect(() => {
    const fetch_ = () => fetchWorkflows().then(setData).catch(console.error);
    fetch_();
    const id = setInterval(fetch_, 5000);
    return () => clearInterval(id);
  }, []);
  return data;
}
