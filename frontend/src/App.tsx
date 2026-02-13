import { useEffect, useState } from "react";

type HealthResponse = { ok: boolean };

export function App() {
  const [status, setStatus] = useState<string>("Checking backend...");

  useEffect(() => {
    async function checkHealth(): Promise<void> {
      try {
        const response = await fetch("/api/health");
        const data = (await response.json()) as HealthResponse;
        setStatus(data.ok ? "Backend is healthy" : "Backend responded with an issue");
      } catch {
        setStatus("Backend is not reachable");
      }
    }

    void checkHealth();
  }, []);

  return (
    <main className="container">
      <h1>Apollo Filter App</h1>
      <p>Frontend is running with recommended baseline practices.</p>
      <p className="status">{status}</p>
    </main>
  );
}
