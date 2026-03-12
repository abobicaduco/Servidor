import { useState, useEffect } from "react";
import { StatusResponse } from "../types";
import { fetchStatus } from "../api/client";

export function useStatus() {
  const [status, setStatus] = useState<StatusResponse | null>(null);
  useEffect(() => {
    const fetch_ = () => fetchStatus().then(setStatus).catch(console.error);
    fetch_();
    const id = setInterval(fetch_, 2000);
    return () => clearInterval(id);
  }, []);
  return status;
}
