import { useEffect, useMemo, useState } from "react";

type RunStatus = "queued" | "running" | "completed" | "failed" | "cancelled";
type RunSummary = {
  id: string;
  status: RunStatus;
  createdAt: string;
  updatedAt: string;
  inputFileName: string;
  progress: {
    totalSteps: number;
    completedSteps: number;
    currentStep: string | null;
    message: string;
  };
  error: string | null;
  pid: number | null;
};

type RunDetail = {
  id: string;
  status: RunStatus;
  inputFileName: string;
  createdAt: string;
  updatedAt: string;
  error: string | null;
  progress: RunSummary["progress"];
  logs: Array<{ ts: string; source: string; line: string }>;
  analysisRunDir: string | null;
};

type CompanyRow = {
  run_id: string;
  company_id: string;
  company_name: string;
  company_domain: string;
  decision: string;
  confidence: string;
  evidence: string;
  raw: string;
};

type PersonRow = Record<string, string>;

type ViewName = "submit" | "runs" | "companies" | "people";

function getViewFromHash(): ViewName {
  const h = window.location.hash.replace(/^#\/?/, "");
  if (h === "runs" || h === "companies" || h === "people") return h;
  return "submit";
}

function toSnakeCase(value: string): string {
  return (value ?? "")
    .trim()
    .replace(/[^0-9A-Za-z]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();
}

function parseCsvHeaders(text: string): string[] {
  let field = "";
  let i = 0;
  let inQuotes = false;
  const headers: string[] = [];
  while (i < text.length) {
    const ch = text[i];
    if (inQuotes) {
      if (ch === "\"") {
        if (text[i + 1] === "\"") {
          field += "\"";
          i += 2;
          continue;
        }
        inQuotes = false;
        i += 1;
        continue;
      }
      field += ch;
      i += 1;
      continue;
    }
    if (ch === "\"") {
      inQuotes = true;
      i += 1;
      continue;
    }
    if (ch === ",") {
      headers.push(field.trim());
      field = "";
      i += 1;
      continue;
    }
    if (ch === "\n" || ch === "\r") {
      headers.push(field.trim());
      break;
    }
    field += ch;
    i += 1;
  }
  if (headers.length === 0 && field.trim()) headers.push(field.trim());
  return headers;
}

function collectRequiredFields(config: unknown): string[] {
  const out = new Set<string>();
  if (!config || typeof config !== "object") return [];
  const cfg = config as Record<string, unknown>;
  const run = (cfg.run ?? {}) as Record<string, unknown>;
  const normalize = (cfg.normalize ?? {}) as Record<string, unknown>;
  const derive = (normalize.derive ?? {}) as Record<string, unknown>;
  const steps = Array.isArray(cfg.steps) ? cfg.steps : [];

  if (typeof run.id_field === "string" && run.id_field.trim()) out.add(run.id_field);
  if (typeof derive.domain_from === "string" && derive.domain_from.trim()) out.add(derive.domain_from);
  if (Array.isArray(derive.profile_text_fields)) {
    for (const field of derive.profile_text_fields) if (typeof field === "string" && field.trim()) out.add(field);
  }
  for (const step of steps) {
    if (!step || typeof step !== "object") continue;
    const stepObj = step as Record<string, unknown>;
    const type = stepObj.type;
    const input = (stepObj.input ?? {}) as Record<string, unknown>;
    const where = (input.where ?? {}) as Record<string, unknown>;
    if (typeof where.field === "string" && where.field.trim()) out.add(where.field);
    if (type === "filter") {
      const rules = (stepObj.rules ?? {}) as Record<string, unknown>;
      const collectRules = (arr: unknown): void => {
        if (!Array.isArray(arr)) return;
        for (const rule of arr) {
          if (!rule || typeof rule !== "object") continue;
          const field = (rule as Record<string, unknown>).field;
          if (typeof field === "string" && field.trim()) out.add(field);
        }
      };
      collectRules(rules.keep_if_any);
      collectRules(rules.drop_if_any);
    }
    if (type === "ai_text" || type === "web_ai") {
      const task = (stepObj.task ?? {}) as Record<string, unknown>;
      if (Array.isArray(task.read_fields)) {
        for (const field of task.read_fields) if (typeof field === "string" && field.trim()) out.add(field);
      }
    }
    if (type === "apollo_people") {
      const company = (stepObj.company ?? {}) as Record<string, unknown>;
      for (const key of ["id_field", "name_field", "domain_field"]) {
        const field = company[key];
        if (typeof field === "string" && field.trim()) out.add(field);
      }
    }
  }
  return Array.from(out);
}

function validateConfigShape(config: unknown): string[] {
  const errors: string[] = [];
  if (!config || typeof config !== "object") {
    errors.push("Config must be a JSON object.");
    return errors;
  }
  const cfg = config as Record<string, unknown>;
  if (!Array.isArray(cfg.steps) || cfg.steps.length === 0) {
    errors.push("Config must include non-empty steps[]");
    return errors;
  }
  const hasAiText = cfg.steps.some(
    (step) => step && typeof step === "object" && (step as Record<string, unknown>).type === "ai_text"
  );
  if (!hasAiText) errors.push("Config must include at least one ai_text step.");
  for (const step of cfg.steps) {
    if (!step || typeof step !== "object") continue;
    const obj = step as Record<string, unknown>;
    if (typeof obj.id !== "string" || !obj.id.trim()) errors.push("Every step requires a non-empty id.");
    if (typeof obj.type !== "string" || !obj.type.trim()) errors.push("Every step requires a type.");
    if ((obj.type === "ai_text" || obj.type === "web_ai") && (!obj.task || !obj.ai)) {
      errors.push(`Step ${String(obj.id ?? "<unknown>")} requires both ai and task.`);
    }
  }
  return errors;
}

function fmtTs(value: string): string {
  if (!value) return "-";
  return new Date(value).toLocaleString();
}

async function apiGet<T>(path: string): Promise<T> {
  const response = await fetch(`/api${path}`);
  if (!response.ok) throw new Error(await response.text());
  return (await response.json()) as T;
}

export function App() {
  const [view, setView] = useState<ViewName>(() => getViewFromHash());
  const [backendHealth, setBackendHealth] = useState("Checking backend...");

  const [csvFile, setCsvFile] = useState<File | null>(null);
  const [csvHeaders, setCsvHeaders] = useState<string[]>([]);
  const [configText, setConfigText] = useState("");
  const [configErrors, setConfigErrors] = useState<string[]>([]);
  const [missingColumns, setMissingColumns] = useState<string[]>([]);
  const [submitMessage, setSubmitMessage] = useState<string>("");
  const [submitting, setSubmitting] = useState(false);

  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [selectedRunDetail, setSelectedRunDetail] = useState<RunDetail | null>(null);
  const [selectedRunCompanies, setSelectedRunCompanies] = useState<CompanyRow[]>([]);
  const [runDetailTab, setRunDetailTab] = useState<"overview" | "companies">("overview");

  const [allCompanies, setAllCompanies] = useState<CompanyRow[]>([]);
  const [selectedCompany, setSelectedCompany] = useState<CompanyRow | null>(null);
  const [selectedCompanyPeople, setSelectedCompanyPeople] = useState<PersonRow[]>([]);
  const [companyDetailTab, setCompanyDetailTab] = useState<"overview" | "people">("overview");

  const [allPeople, setAllPeople] = useState<PersonRow[]>([]);

  useEffect(() => {
    const onHash = (): void => setView(getViewFromHash());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  useEffect(() => {
    void (async () => {
      try {
        const data = await apiGet<{ ok: boolean }>("/health");
        setBackendHealth(data.ok ? "Backend is healthy" : "Backend responded with issue");
      } catch {
        setBackendHealth("Backend is not reachable");
      }
    })();
  }, []);

  useEffect(() => {
    void refreshRuns();
    const sse = new EventSource("/api/runs/stream");
    sse.addEventListener("snapshot", (event) => {
      const payload = JSON.parse((event as MessageEvent).data) as { runs: RunSummary[] };
      setRuns(payload.runs);
    });
    sse.addEventListener("run_update", (event) => {
      const updated = JSON.parse((event as MessageEvent).data) as RunSummary;
      setRuns((prev) => {
        const next = prev.filter((run) => run.id !== updated.id);
        next.unshift(updated);
        next.sort((a, b) => b.createdAt.localeCompare(a.createdAt));
        return next;
      });
      if (selectedRunId === updated.id) {
        void loadRunDetail(updated.id);
      }
    });
    return () => sse.close();
  }, [selectedRunId]);

  useEffect(() => {
    if (view === "companies") void refreshCompanies();
    if (view === "people") void refreshPeople();
  }, [view]);

  const canSubmit = useMemo(
    () =>
      Boolean(csvFile)
      && configErrors.length === 0
      && missingColumns.length === 0
      && configText.trim().length > 0
      && !submitting,
    [csvFile, configErrors, missingColumns, configText, submitting]
  );

  async function refreshRuns(): Promise<void> {
    const response = await apiGet<{ ok: boolean; runs: RunSummary[] }>("/runs");
    setRuns(response.runs);
  }

  async function refreshCompanies(): Promise<void> {
    const response = await apiGet<{ ok: boolean; companies: CompanyRow[] }>("/companies");
    setAllCompanies(response.companies);
  }

  async function refreshPeople(): Promise<void> {
    const response = await apiGet<{ ok: boolean; people: PersonRow[] }>("/people");
    setAllPeople(response.people);
  }

  async function onCsvSelected(file: File): Promise<void> {
    setCsvFile(file);
    const text = await file.text();
    const headers = parseCsvHeaders(text);
    setCsvHeaders(headers);
    recomputeMissingColumns(headers, configText);
  }

  function onConfigTextChange(next: string): void {
    setConfigText(next);
    recomputeMissingColumns(csvHeaders, next);
  }

  function recomputeMissingColumns(headers: string[], rawConfigText: string): void {
    const nextConfigErrors: string[] = [];
    let requiredFields: string[] = [];
    if (!rawConfigText.trim()) {
      nextConfigErrors.push("Config JSON is required.");
    } else {
      try {
        const parsed = JSON.parse(rawConfigText) as unknown;
        nextConfigErrors.push(...validateConfigShape(parsed));
        requiredFields = collectRequiredFields(parsed);
      } catch {
        nextConfigErrors.push("Invalid JSON syntax.");
      }
    }
    setConfigErrors(nextConfigErrors);

    const canonical = new Set<string>();
    for (const h of headers) {
      canonical.add(h);
      canonical.add(toSnakeCase(h));
    }
    const missing = requiredFields.filter((field) => !canonical.has(field));
    setMissingColumns(nextConfigErrors.length > 0 ? [] : missing);
  }

  async function submitRun(): Promise<void> {
    if (!csvFile) return;
    setSubmitting(true);
    setSubmitMessage("");
    try {
      const body = new FormData();
      body.append("csvFile", csvFile);
      body.append("configJson", configText);
      const response = await fetch("/api/runs", { method: "POST", body });
      const data = (await response.json()) as { ok: boolean; run?: RunSummary; error?: string };
      if (!response.ok || !data.ok || !data.run) {
        throw new Error(data.error ?? "Failed to submit run");
      }
      setSubmitMessage(`Run ${data.run.id} submitted. Redirecting to Run Monitor...`);
      window.location.hash = "#/runs";
      setCsvFile(null);
      setCsvHeaders([]);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to submit run";
      setSubmitMessage(message);
    } finally {
      setSubmitting(false);
    }
  }

  async function cancelRun(runId: string): Promise<void> {
    await fetch(`/api/runs/${runId}/cancel`, { method: "POST" });
    await refreshRuns();
  }

  async function loadRunDetail(runId: string): Promise<void> {
    setSelectedRunId(runId);
    const detail = await apiGet<{ ok: boolean; run: RunDetail }>(`/runs/${runId}`);
    setSelectedRunDetail(detail.run);
    const companies = await apiGet<{ ok: boolean; companies: CompanyRow[] }>(`/runs/${runId}/companies`);
    setSelectedRunCompanies(companies.companies);
  }

  async function openCompanyDetail(company: CompanyRow): Promise<void> {
    setSelectedCompany(company);
    setCompanyDetailTab("overview");
    const people = await apiGet<{ ok: boolean; people: PersonRow[] }>(
      `/companies/${company.run_id}/${encodeURIComponent(company.company_id)}/people`
    );
    setSelectedCompanyPeople(people.people);
  }

  const nav = (
    <nav className="tabs">
      {[
        { key: "submit", label: "Submit Run" },
        { key: "runs", label: "Run Monitor" },
        { key: "companies", label: "Company" },
        { key: "people", label: "People" }
      ].map((item) => (
        <button
          key={item.key}
          className={view === item.key ? "active" : ""}
          onClick={() => {
            window.location.hash = `#/${item.key}`;
          }}
        >
          {item.label}
        </button>
      ))}
    </nav>
  );

  return (
    <main className="app-shell">
      <h1>Apollo Filter App</h1>
      <p className="status">{backendHealth}</p>
      {nav}

      {view === "submit" && (
        <section className="panel">
          <h2>Upload CSV + Config</h2>
          <div
            className="dropzone"
            onDragOver={(event) => event.preventDefault()}
            onDrop={(event) => {
              event.preventDefault();
              const file = event.dataTransfer.files?.[0];
              if (file) void onCsvSelected(file);
            }}
          >
            <p>{csvFile ? `Selected: ${csvFile.name}` : "Drag and drop CSV here, or choose file"}</p>
            <input
              type="file"
              accept=".csv,text/csv"
              onChange={(event) => {
                const file = event.target.files?.[0];
                if (file) void onCsvSelected(file);
              }}
            />
          </div>
          <textarea
            className="config-input"
            value={configText}
            onChange={(event) => onConfigTextChange(event.target.value)}
            placeholder="Paste JSON config (same schema as mortgage-lender.mvp.json)"
          />
          {configErrors.length > 0 && (
            <div className="error-list">
              {configErrors.map((error) => (
                <p key={error}>{error}</p>
              ))}
            </div>
          )}
          {missingColumns.length > 0 && (
            <div className="error-list">
              <p>CSV is missing required columns:</p>
              <p>{missingColumns.join(", ")}</p>
            </div>
          )}
          <button disabled={!canSubmit} onClick={() => void submitRun()}>
            {submitting ? "Submitting..." : "Submit Run"}
          </button>
          {submitMessage && <p className="status">{submitMessage}</p>}
        </section>
      )}

      {view === "runs" && (
        <section className="panel">
          <h2>Run Monitor</h2>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Run ID</th>
                  <th>Status</th>
                  <th>Progress</th>
                  <th>Input</th>
                  <th>Updated</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {runs.map((run) => (
                  <tr key={run.id}>
                    <td>{run.id}</td>
                    <td>{run.status}</td>
                    <td>
                      {run.progress.completedSteps}/{run.progress.totalSteps} - {run.progress.message}
                    </td>
                    <td>{run.inputFileName}</td>
                    <td>{fmtTs(run.updatedAt)}</td>
                    <td>
                      {(run.status === "running" || run.status === "queued") && (
                        <button onClick={() => void cancelRun(run.id)}>Cancel</button>
                      )}
                      <button onClick={() => void loadRunDetail(run.id)}>Detail</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {view === "companies" && (
        <section className="panel">
          <h2>Company</h2>
          <button onClick={() => void refreshCompanies()}>Refresh</button>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Run</th>
                  <th>Company</th>
                  <th>Domain</th>
                  <th>Decision</th>
                  <th>Confidence</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {allCompanies.map((company) => (
                  <tr key={`${company.run_id}-${company.company_id}`}>
                    <td>{company.run_id}</td>
                    <td>{company.company_name}</td>
                    <td>{company.company_domain}</td>
                    <td>{company.decision}</td>
                    <td>{company.confidence}</td>
                    <td>
                      <button onClick={() => void openCompanyDetail(company)}>Detail</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {view === "people" && (
        <section className="panel">
          <h2>People</h2>
          <button onClick={() => void refreshPeople()}>Refresh</button>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Run</th>
                  <th>Company</th>
                  <th>Name</th>
                  <th>Title</th>
                  <th>Email</th>
                  <th>LinkedIn</th>
                </tr>
              </thead>
              <tbody>
                {allPeople.map((person) => (
                  <tr
                    key={`${person.run_id ?? ""}-${person.company_id ?? ""}-${person.apollo_person_id ?? ""}-${person.email ?? ""}`}
                  >
                    <td>{person.run_id ?? ""}</td>
                    <td>{person.company_name ?? ""}</td>
                    <td>{person.full_name ?? ""}</td>
                    <td>{person.title ?? ""}</td>
                    <td>{person.email ?? ""}</td>
                    <td>{person.linkedin_url ?? ""}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {selectedRunDetail && (
        <dialog open className="dialog">
          <header>
            <strong>Run Detail: {selectedRunDetail.id}</strong>
            <button onClick={() => setSelectedRunDetail(null)}>Close</button>
          </header>
          <div className="tabs">
            <button
              className={runDetailTab === "overview" ? "active" : ""}
              onClick={() => setRunDetailTab("overview")}
            >
              Overview
            </button>
            <button
              className={runDetailTab === "companies" ? "active" : ""}
              onClick={() => setRunDetailTab("companies")}
            >
              Companies
            </button>
          </div>
          {runDetailTab === "overview" ? (
            <div className="dialog-body">
              <p>Status: {selectedRunDetail.status}</p>
              <p>
                Progress: {selectedRunDetail.progress.completedSteps}/{selectedRunDetail.progress.totalSteps}
              </p>
              <p>Message: {selectedRunDetail.progress.message}</p>
              <p>Artifacts: {selectedRunDetail.analysisRunDir ?? "-"}</p>
              {selectedRunDetail.error && <p className="error">{selectedRunDetail.error}</p>}
              <pre className="log-box">
                {selectedRunDetail.logs.slice(-60).map((log) => `[${log.ts}] ${log.source} ${log.line}`).join("\n")}
              </pre>
            </div>
          ) : (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Company</th>
                    <th>Domain</th>
                    <th>Decision</th>
                    <th>Confidence</th>
                    <th>Evidence</th>
                  </tr>
                </thead>
                <tbody>
                  {selectedRunCompanies.map((company) => (
                    <tr key={`${company.run_id}-${company.company_id}`}>
                      <td>{company.company_name}</td>
                      <td>{company.company_domain}</td>
                      <td>{company.decision}</td>
                      <td>{company.confidence}</td>
                      <td>{company.evidence}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </dialog>
      )}

      {selectedCompany && (
        <dialog open className="dialog">
          <header>
            <strong>
              Company Detail: {selectedCompany.company_name} ({selectedCompany.company_id})
            </strong>
            <button onClick={() => setSelectedCompany(null)}>Close</button>
          </header>
          <div className="tabs">
            <button
              className={companyDetailTab === "overview" ? "active" : ""}
              onClick={() => setCompanyDetailTab("overview")}
            >
              Overview
            </button>
            <button
              className={companyDetailTab === "people" ? "active" : ""}
              onClick={() => setCompanyDetailTab("people")}
            >
              People
            </button>
          </div>
          {companyDetailTab === "overview" ? (
            <div className="dialog-body">
              <p>Run: {selectedCompany.run_id}</p>
              <p>Company: {selectedCompany.company_name}</p>
              <p>Domain: {selectedCompany.company_domain}</p>
              <p>
                Decision/Confidence: {selectedCompany.decision} / {selectedCompany.confidence}
              </p>
              <p>Evidence: {selectedCompany.evidence}</p>
              <pre className="log-box">{selectedCompany.raw}</pre>
            </div>
          ) : (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Title</th>
                    <th>Email</th>
                    <th>LinkedIn</th>
                    <th>Location</th>
                  </tr>
                </thead>
                <tbody>
                  {selectedCompanyPeople.map((person) => (
                    <tr
                      key={`${person.company_id ?? ""}-${person.apollo_person_id ?? ""}-${person.email ?? ""}-${person.full_name ?? ""}`}
                    >
                      <td>{person.full_name ?? ""}</td>
                      <td>{person.title ?? ""}</td>
                      <td>{person.email ?? ""}</td>
                      <td>{person.linkedin_url ?? ""}</td>
                      <td>{person.location ?? ""}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </dialog>
      )}
    </main>
  );
}
