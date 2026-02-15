import { useEffect, useMemo, useRef, useState } from "react";

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
  inputConfigJson?: string;
  stepSummaries?: Array<{
    id: string;
    type: string;
    title: string;
    inputRows: number | null;
    outputRows: number | null;
    progressValue: string | null;
    status: "not_started" | "running" | "cancelled" | "failed" | "finished";
  }>;
};

type StepSummaryJobProgress = {
  id: string;
  title: string;
  type: string;
  status: "pending" | "running" | "finished" | "failed";
  percent: number;
};

type CompanyRow = {
  run_id: string;
  company_id: string;
  company_name: string;
  company_domain: string;
  people_count?: string;
  decision?: string;
  confidence?: string;
  evidence?: string;
  raw?: string;
};

type PersonRow = Record<string, string>;
type ReviewRow = Record<string, string>;

type ViewName = "submit" | "runs" | "companies" | "people" | "reviews";
const DEFAULT_PAGE_SIZE = 20;
const PAGE_SIZE_OPTIONS = [20, 50, 100];

function getViewFromHash(): ViewName {
  const h = window.location.hash.replace(/^#\/?/, "");
  if (h === "runs" || h === "companies" || h === "people" || h === "reviews") return h;
  return "submit";
}

function toSnakeCase(value: string): string {
  return (value ?? "")
    .trim()
    .replace(/[^0-9A-Za-z]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();
}

function toTitleCaseFromSnake(value: string): string {
  if (!value) return value;
  return value
    .split("_")
    .filter((part) => part.length > 0)
    .map((part) => part.slice(0, 1).toUpperCase() + part.slice(1))
    .join(" ");
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
  const normalize = (cfg.normalize ?? {}) as Record<string, unknown>;
  const derive = (normalize.derive ?? {}) as Record<string, unknown>;
  const steps = Array.isArray(cfg.steps) ? cfg.steps : [];
  const generated = new Set<string>(["__row_id", "domain", "profile_text", "Decision-Final", "Confidence-Final", "Final-Source"]);

  for (const step of steps) {
    if (!step || typeof step !== "object") continue;
    const stepObj = step as Record<string, unknown>;
    const type = stepObj.type;
    if (type !== "ai_text" && type !== "web_ai") continue;
    const task = (stepObj.task ?? {}) as Record<string, unknown>;
    for (const key of ["decision_field", "confidence_field", "reason_field", "evidence_field"]) {
      const field = task[key];
      if (typeof field === "string" && field.trim()) generated.add(field);
    }
  }

  // run.id_field is optional in practice due __row_id fallback.
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
    if (typeof where.field === "string" && where.field.trim() && !generated.has(where.field)) out.add(where.field);
    if (type === "filter") {
      const rules = (stepObj.rules ?? {}) as Record<string, unknown>;
      const collectRules = (arr: unknown): void => {
        if (!Array.isArray(arr)) return;
        for (const rule of arr) {
          if (!rule || typeof rule !== "object") continue;
          const field = (rule as Record<string, unknown>).field;
          if (typeof field === "string" && field.trim() && !generated.has(field)) out.add(field);
        }
      };
      collectRules(rules.keep_if_any);
      collectRules(rules.drop_if_any);
    }
    if (type === "ai_text" || type === "web_ai") {
      const task = (stepObj.task ?? {}) as Record<string, unknown>;
      if (Array.isArray(task.read_fields)) {
        for (const field of task.read_fields) if (typeof field === "string" && field.trim() && !generated.has(field)) out.add(field);
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

function stringifyValue(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function buildCompanyDetailRows(company: CompanyRow): Array<{ field: string; value: string }> {
  const rows: Array<{ field: string; value: string }> = [];
  let rawObject: Record<string, unknown> | null = null;
  if (company.raw?.trim()) {
    try {
      const parsed = JSON.parse(company.raw) as unknown;
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        rawObject = parsed as Record<string, unknown>;
      }
    } catch {
      rawObject = null;
    }
  }
  if (rawObject) {
    const dedup = new Map<string, { field: string; value: string }>();
    for (const [field, value] of Object.entries(rawObject)) {
      const canonical = toSnakeCase(field);
      if (!canonical) continue;
      const nextValue = stringifyValue(value).trim();
      const existing = dedup.get(canonical);
      if (!existing) {
        dedup.set(canonical, {
          field: toTitleCaseFromSnake(canonical),
          value: nextValue
        });
        continue;
      }
      if (!existing.value && nextValue) {
        dedup.set(canonical, { ...existing, value: nextValue });
      }
    }
    rows.push(...dedup.values());
    return rows;
  }
  rows.push(
    ...Object.entries(company)
      .filter(([key]) => !["run_id", "raw", "evidence"].includes(key))
      .map(([key, value]) => ({
        field: toTitleCaseFromSnake(key),
        value: stringifyValue(value)
      }))
      .filter((item) => item.value.trim().length > 0)
  );
  return rows;
}

function formatEvidence(value: string): string {
  const trimmed = (value ?? "").trim();
  if (!trimmed) return "";
  try {
    const parsed = JSON.parse(trimmed) as unknown;
    if (Array.isArray(parsed)) {
      return parsed
        .map((item) => stringifyValue(item).trim())
        .filter((item) => item.length > 0)
        .join("\n");
    }
    if (parsed && typeof parsed === "object") {
      return JSON.stringify(parsed, null, 2);
    }
  } catch {
    // Keep plain-text evidence untouched when not valid JSON.
  }
  return value;
}

function toStepStatusLabel(status: "not_started" | "running" | "cancelled" | "failed" | "finished"): string {
  if (status === "not_started") return "Not Started";
  if (status === "running") return "Running";
  if (status === "cancelled") return "Cancelled";
  if (status === "failed") return "Failed";
  return "Finished";
}

function PaginationControls(props: {
  totalItems: number;
  page: number;
  pageSize: number;
  onPageChange: (page: number) => void;
  onPageSizeChange: (size: number) => void;
}) {
  const { totalItems, page, pageSize, onPageChange, onPageSizeChange } = props;
  const totalPages = Math.max(1, Math.ceil(totalItems / pageSize));
  const start = totalItems === 0 ? 0 : (page - 1) * pageSize + 1;
  const end = Math.min(totalItems, page * pageSize);
  return (
    <div className="table-pagination">
      <div className="pagination-summary">
        {start}-{end} of {totalItems}
      </div>
      <label className="page-size">
        Page size
        <select
          value={String(pageSize)}
          onChange={(event) => onPageSizeChange(Number.parseInt(event.target.value, 10) || DEFAULT_PAGE_SIZE)}
        >
          {PAGE_SIZE_OPTIONS.map((size) => (
            <option key={size} value={String(size)}>
              {size}
            </option>
          ))}
        </select>
      </label>
      <div className="pagination-nav">
        <button disabled={page <= 1} onClick={() => onPageChange(page - 1)}>
          Prev
        </button>
        <span>
          Page {page}/{totalPages}
        </span>
        <button disabled={page >= totalPages} onClick={() => onPageChange(page + 1)}>
          Next
        </button>
      </div>
    </div>
  );
}

async function apiGet<T>(path: string): Promise<T> {
  const response = await fetch(`/api${path}`);
  if (!response.ok) throw new Error(await response.text());
  return (await response.json()) as T;
}

async function apiPost<T>(path: string): Promise<T> {
  const response = await fetch(`/api${path}`, { method: "POST" });
  if (!response.ok) throw new Error(await response.text());
  return (await response.json()) as T;
}

async function apiGetWithProgress<T>(
  path: string,
  onProgress: (percent: number | null) => void
): Promise<T> {
  const response = await fetch(`/api${path}`);
  if (!response.ok) throw new Error(await response.text());
  const totalHeader = response.headers?.get?.("content-length") ?? "0";
  const total = Number.parseInt(totalHeader, 10);
  if (!response.body) {
    onProgress(null);
    return (await response.json()) as T;
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let loaded = 0;
  let text = "";
  onProgress(total > 0 ? 0 : null);
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value) {
      loaded += value.byteLength;
      text += decoder.decode(value, { stream: true });
      if (total > 0) {
        const pct = Math.max(0, Math.min(99, Math.round((loaded / total) * 100)));
        onProgress(pct);
      }
    }
  }
  text += decoder.decode();
  if (total > 0) onProgress(100);
  return JSON.parse(text) as T;
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
  const [runDetailLoading, setRunDetailLoading] = useState(false);
  const [runDetailError, setRunDetailError] = useState("");
  const [selectedRunCompanies, setSelectedRunCompanies] = useState<CompanyRow[]>([]);
  const [runCompaniesLoading, setRunCompaniesLoading] = useState(false);
  const [runCompaniesProgress, setRunCompaniesProgress] = useState<number | null>(null);
  const [runCompaniesError, setRunCompaniesError] = useState("");
  const [stepSummariesLoading, setStepSummariesLoading] = useState(false);
  const [stepSummariesProgress, setStepSummariesProgress] = useState<number>(0);
  const [stepSummariesProgressSteps, setStepSummariesProgressSteps] = useState<StepSummaryJobProgress[]>([]);
  const [stepSummariesError, setStepSummariesError] = useState("");
  const [logsLoading, setLogsLoading] = useState(false);
  const [logsProgress, setLogsProgress] = useState<number>(0);
  const [logsError, setLogsError] = useState("");
  const [runDetailTab, setRunDetailTab] = useState<"overview" | "companies" | "input_json">("overview");
  const activeRunDetailIdRef = useRef<string | null>(null);
  const [runsPage, setRunsPage] = useState(1);
  const [runsPageSize, setRunsPageSize] = useState(DEFAULT_PAGE_SIZE);

  const [allCompanies, setAllCompanies] = useState<CompanyRow[]>([]);
  const [companiesLoading, setCompaniesLoading] = useState(false);
  const [companiesError, setCompaniesError] = useState("");
  const [companiesPage, setCompaniesPage] = useState(1);
  const [companiesPageSize, setCompaniesPageSize] = useState(DEFAULT_PAGE_SIZE);
  const [selectedCompany, setSelectedCompany] = useState<CompanyRow | null>(null);
  const [selectedCompanyPeople, setSelectedCompanyPeople] = useState<PersonRow[]>([]);
  const [companyDetailTab, setCompanyDetailTab] = useState<"overview" | "people">("overview");

  const [allPeople, setAllPeople] = useState<PersonRow[]>([]);
  const [peopleLoading, setPeopleLoading] = useState(false);
  const [peopleError, setPeopleError] = useState("");
  const [peopleStatusMessage, setPeopleStatusMessage] = useState("");
  const [enrichingPersonId, setEnrichingPersonId] = useState<string | null>(null);
  const [peoplePage, setPeoplePage] = useState(1);
  const [peoplePageSize, setPeoplePageSize] = useState(DEFAULT_PAGE_SIZE);
  const [companyReviews, setCompanyReviews] = useState<ReviewRow[]>([]);
  const [peopleReviews, setPeopleReviews] = useState<ReviewRow[]>([]);

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
        setSelectedRunDetail((prev) => {
          if (!prev) return prev;
          return {
            ...prev,
            status: updated.status,
            updatedAt: updated.updatedAt,
            error: updated.error,
            progress: updated.progress
          };
        });
      }
    });
    return () => sse.close();
  }, [selectedRunId]);

  useEffect(() => {
    if (view === "companies") void refreshCompanies();
    if (view === "people") void refreshPeople();
    if (view === "reviews") void refreshReviews();
  }, [view]);

  const canSubmit = useMemo(
    () =>
      Boolean(csvFile)
      && configErrors.length === 0
      && configText.trim().length > 0
      && !submitting,
    [csvFile, configErrors, configText, submitting]
  );

  const pagedRuns = useMemo(() => {
    const totalPages = Math.max(1, Math.ceil(runs.length / runsPageSize));
    const page = Math.min(runsPage, totalPages);
    const start = (page - 1) * runsPageSize;
    return runs.slice(start, start + runsPageSize);
  }, [runs, runsPage, runsPageSize]);

  const pagedCompanies = useMemo(() => {
    const totalPages = Math.max(1, Math.ceil(allCompanies.length / companiesPageSize));
    const page = Math.min(companiesPage, totalPages);
    const start = (page - 1) * companiesPageSize;
    return allCompanies.slice(start, start + companiesPageSize);
  }, [allCompanies, companiesPage, companiesPageSize]);

  const pagedPeople = useMemo(() => {
    const totalPages = Math.max(1, Math.ceil(allPeople.length / peoplePageSize));
    const page = Math.min(peoplePage, totalPages);
    const start = (page - 1) * peoplePageSize;
    return allPeople.slice(start, start + peoplePageSize);
  }, [allPeople, peoplePage, peoplePageSize]);

  useEffect(() => {
    const totalPages = Math.max(1, Math.ceil(runs.length / runsPageSize));
    if (runsPage > totalPages) setRunsPage(totalPages);
  }, [runs.length, runsPage, runsPageSize]);

  useEffect(() => {
    const totalPages = Math.max(1, Math.ceil(allCompanies.length / companiesPageSize));
    if (companiesPage > totalPages) setCompaniesPage(totalPages);
  }, [allCompanies.length, companiesPage, companiesPageSize]);

  useEffect(() => {
    const totalPages = Math.max(1, Math.ceil(allPeople.length / peoplePageSize));
    if (peoplePage > totalPages) setPeoplePage(totalPages);
  }, [allPeople.length, peoplePage, peoplePageSize]);

  async function refreshRuns(): Promise<void> {
    const response = await apiGet<{ ok: boolean; runs: RunSummary[] }>("/runs");
    setRuns(response.runs);
  }

  async function refreshCompanies(): Promise<void> {
    setCompaniesLoading(true);
    setCompaniesError("");
    try {
      const response = await apiGet<{ ok: boolean; companies: CompanyRow[] }>("/companies");
      setAllCompanies(response.companies);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load companies";
      setCompaniesError(message);
    } finally {
      setCompaniesLoading(false);
    }
  }

  async function refreshPeople(): Promise<void> {
    setPeopleLoading(true);
    setPeopleError("");
    try {
      const response = await apiGet<{ ok: boolean; people: PersonRow[] }>("/people");
      setAllPeople(response.people);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load people";
      setPeopleError(message);
    } finally {
      setPeopleLoading(false);
    }
  }

  async function enrichPerson(personId: string): Promise<void> {
    if (!personId || enrichingPersonId) return;
    setPeopleStatusMessage("");
    setPeopleError("");
    setEnrichingPersonId(personId);
    try {
      const response = await fetch(`/api/people/${encodeURIComponent(personId)}/enrich`, { method: "POST" });
      const contentType = response.headers.get("content-type") ?? "";
      const isJson = contentType.includes("application/json");
      const payload = isJson ? (await response.json()) as { ok: boolean; error?: string } : null;
      if (!response.ok || !payload?.ok) {
        throw new Error(payload?.error ?? `Failed to enrich person (HTTP ${response.status})`);
      }
      await refreshPeople();
      setPeopleStatusMessage("Person enriched and updated.");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to enrich person";
      setPeopleError(message);
    } finally {
      setEnrichingPersonId(null);
    }
  }

  async function refreshReviews(): Promise<void> {
    const [companyResponse, peopleResponse] = await Promise.all([
      apiGet<{ ok: boolean; reviews: ReviewRow[] }>("/reviews/companies"),
      apiGet<{ ok: boolean; reviews: ReviewRow[] }>("/reviews/people")
    ]);
    setCompanyReviews(companyResponse.reviews);
    setPeopleReviews(peopleResponse.reviews);
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
    if (canonical.has("company_name")) canonical.add("name");
    if (canonical.has("apollo_account_id")) canonical.add("id");
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
      const responseType = response.headers.get("content-type") ?? "";
      const isJson = responseType.includes("application/json");
      const data = isJson
        ? (await response.json()) as { ok: boolean; run?: RunSummary; error?: string }
        : null;
      if (!response.ok || !data?.ok || !data.run) {
        const text = isJson ? "" : await response.text();
        const fallback = response.status === 413
          ? "Upload is too large. Please reduce file size or increase proxy body limit."
          : `Failed to submit run (HTTP ${response.status})`;
        throw new Error(data?.error ?? (text.trim().length > 0 ? text : fallback));
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

  async function loadRunDetail(run: RunSummary): Promise<void> {
    setSelectedRunId(run.id);
    activeRunDetailIdRef.current = run.id;
    setRunDetailTab("overview");
    setRunDetailLoading(true);
    setRunDetailError("");
    setSelectedRunDetail({
      id: run.id,
      status: run.status,
      inputFileName: run.inputFileName,
      createdAt: run.createdAt,
      updatedAt: run.updatedAt,
      error: run.error,
      progress: run.progress,
      logs: [],
      analysisRunDir: null,
      inputConfigJson: "",
      stepSummaries: []
    });
    setSelectedRunCompanies([]);
    setRunCompaniesLoading(false);
    setRunCompaniesProgress(null);
    setRunCompaniesError("");
    setStepSummariesLoading(true);
    setStepSummariesProgress(0);
    setStepSummariesProgressSteps([]);
    setStepSummariesError("");
    setLogsLoading(true);
    setLogsProgress(0);
    setLogsError("");
    try {
      const detail = await apiGet<{ ok: boolean; run: RunDetail }>(`/runs/${run.id}/overview`);
      setSelectedRunDetail((prev) => ({ ...(prev ?? detail.run), ...detail.run }));
      void startStepSummariesLoad(run.id);
      void startLogsLoad(run.id);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load run detail";
      setRunDetailError(message);
    } finally {
      setRunDetailLoading(false);
    }
  }

  async function startStepSummariesLoad(runId: string): Promise<void> {
    setStepSummariesLoading(true);
    try {
      const started = await apiPost<{ ok: boolean; jobId: string }>(`/runs/${runId}/heavy/step-summaries/start`);
      const jobId = started.jobId;
      while (activeRunDetailIdRef.current === runId) {
        const progress = await apiGet<{
          ok: boolean;
          job: { status: "running" | "completed" | "failed"; percent: number; steps: StepSummaryJobProgress[]; error?: string };
        }>(`/runs/${runId}/heavy/step-summaries/progress?jobId=${encodeURIComponent(jobId)}`);
        setStepSummariesProgress(progress.job.percent);
        setStepSummariesProgressSteps(progress.job.steps ?? []);
        if (progress.job.status === "failed") {
          throw new Error(progress.job.error ?? "Step summaries job failed");
        }
        if (progress.job.status === "completed") {
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 300));
      }
      const result = await apiGet<{ ok: boolean; ready: boolean; stepSummaries: RunDetail["stepSummaries"] }>(
        `/runs/${runId}/heavy/step-summaries/result?jobId=${encodeURIComponent(jobId)}`
      );
      if (result.ready) {
        setSelectedRunDetail((prev) => (prev ? { ...prev, stepSummaries: result.stepSummaries ?? [] } : prev));
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed loading step summaries";
      setStepSummariesError(message);
    } finally {
      setStepSummariesLoading(false);
    }
  }

  async function startLogsLoad(runId: string): Promise<void> {
    setLogsLoading(true);
    try {
      const started = await apiPost<{ ok: boolean; jobId: string }>(`/runs/${runId}/heavy/logs/start`);
      const jobId = started.jobId;
      while (activeRunDetailIdRef.current === runId) {
        const progress = await apiGet<{
          ok: boolean;
          job: { status: "running" | "completed" | "failed"; percent: number; error?: string };
        }>(`/runs/${runId}/heavy/logs/progress?jobId=${encodeURIComponent(jobId)}`);
        setLogsProgress(progress.job.percent);
        if (progress.job.status === "failed") {
          throw new Error(progress.job.error ?? "Logs job failed");
        }
        if (progress.job.status === "completed") {
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 300));
      }
      const result = await apiGet<{ ok: boolean; ready: boolean; logs: Array<{ ts: string; source: string; line: string }> }>(
        `/runs/${runId}/heavy/logs/result?jobId=${encodeURIComponent(jobId)}`
      );
      if (result.ready) {
        setSelectedRunDetail((prev) => (prev ? { ...prev, logs: result.logs ?? [] } : prev));
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed loading logs";
      setLogsError(message);
    } finally {
      setLogsLoading(false);
    }
  }

  async function loadRunCompanies(runId: string): Promise<void> {
    if (runCompaniesLoading) return;
    setRunCompaniesLoading(true);
    setRunCompaniesError("");
    setRunCompaniesProgress(null);
    try {
      const companies = await apiGetWithProgress<{ ok: boolean; companies: CompanyRow[] }>(
        `/runs/${runId}/companies`,
        (percent) => setRunCompaniesProgress(percent)
      );
      setSelectedRunCompanies(companies.companies);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load companies";
      setRunCompaniesError(message);
    } finally {
      setRunCompaniesLoading(false);
    }
  }

  async function openCompanyDetail(company: CompanyRow): Promise<void> {
    setSelectedCompany(company);
    setCompanyDetailTab("overview");
    const people = await apiGet<{ ok: boolean; people: PersonRow[] }>(
      `/companies/${encodeURIComponent(company.company_id)}/people`
    );
    setSelectedCompanyPeople(people.people);
  }

  async function resolveReview(entity: "companies" | "people", id: string, decision: "keep_old" | "keep_new"): Promise<void> {
    const response = await fetch(`/api/reviews/${entity}/${encodeURIComponent(id)}/resolve`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ decision })
    });
    if (!response.ok) throw new Error(await response.text());
    await refreshReviews();
    if (view === "companies") await refreshCompanies();
    if (view === "people") await refreshPeople();
  }

  const nav = (
    <nav className="tabs">
      {[
        { key: "submit", label: "Submit Run" },
        { key: "runs", label: "Run Monitor" },
        { key: "companies", label: "Company" },
          { key: "people", label: "People" },
          { key: "reviews", label: "Review Queue" }
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
            <div className="warn-list">
              <p>CSV is missing some referenced fields (run can still proceed):</p>
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
          <PaginationControls
            totalItems={runs.length}
            page={runsPage}
            pageSize={runsPageSize}
            onPageChange={setRunsPage}
            onPageSizeChange={(size) => {
              setRunsPageSize(size);
              setRunsPage(1);
            }}
          />
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
                {pagedRuns.map((run) => (
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
                      <button onClick={() => void loadRunDetail(run)}>Detail</button>
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
          <button onClick={() => void refreshCompanies()} disabled={companiesLoading}>
            {companiesLoading ? "Loading..." : "Refresh"}
          </button>
          {companiesLoading && <p className="status">Loading companies...</p>}
          {companiesError && <p className="error">{companiesError}</p>}
          <PaginationControls
            totalItems={allCompanies.length}
            page={companiesPage}
            pageSize={companiesPageSize}
            onPageChange={setCompaniesPage}
            onPageSizeChange={(size) => {
              setCompaniesPageSize(size);
              setCompaniesPage(1);
            }}
          />
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Company</th>
                  <th>Domain</th>
                  <th>People/Leads</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {pagedCompanies.map((company) => (
                  <tr key={`${company.run_id}-${company.company_id}`}>
                    <td>{company.company_name}</td>
                    <td>{company.company_domain}</td>
                    <td>{company.people_count ?? "0"}</td>
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
          <button onClick={() => void refreshPeople()} disabled={peopleLoading || Boolean(enrichingPersonId)}>
            {peopleLoading ? "Loading..." : "Refresh"}
          </button>
          {peopleStatusMessage && <p className="status">{peopleStatusMessage}</p>}
          {peopleError && <p className="error">{peopleError}</p>}
          <PaginationControls
            totalItems={allPeople.length}
            page={peoplePage}
            pageSize={peoplePageSize}
            onPageChange={setPeoplePage}
            onPageSizeChange={(size) => {
              setPeoplePageSize(size);
              setPeoplePage(1);
            }}
          />
          <div className="table-wrap">
            <table className="people-table">
              <colgroup>
                <col className="col-company" />
                <col className="col-apollo-id" />
                <col className="col-name" />
                <col className="col-title" />
                <col className="col-email" />
                <col className="col-linkedin" />
                <col className="col-actions" />
              </colgroup>
              <thead>
                <tr>
                  <th>Company</th>
                  <th>Apollo ID</th>
                  <th>Name</th>
                  <th>Title</th>
                  <th>Email</th>
                  <th>LinkedIn</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {pagedPeople.map((person) => (
                  <tr
                    key={`${person.person_id ?? ""}-${person.company_id ?? ""}-${person.email ?? ""}`}
                  >
                    <td>{person.company_name ?? person.company_id ?? ""}</td>
                    <td>{person.apollo_contact_id ?? person.person_id ?? ""}</td>
                    <td>{person.full_name ?? ""}</td>
                    <td>{person.title ?? ""}</td>
                    <td>{person.email ?? ""}</td>
                    <td>{person.linkedin_url ?? ""}</td>
                    <td>
                      <button
                        onClick={() => void enrichPerson(person.person_id ?? "")}
                        disabled={!person.person_id || enrichingPersonId === (person.person_id ?? "")}
                      >
                        {enrichingPersonId === (person.person_id ?? "") ? "Enriching..." : "Enrich"}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {view === "reviews" && (
        <section className="panel">
          <h2>Review Queue</h2>
          <button onClick={() => void refreshReviews()}>Refresh</button>
          <h3>Company Conflicts ({companyReviews.length})</h3>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Company ID</th>
                  <th>Field</th>
                  <th>Old Value</th>
                  <th>New Value</th>
                  <th>Source Run</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {companyReviews.map((review) => (
                  <tr key={review.id}>
                    <td>{review.company_id}</td>
                    <td>{review.field_name}</td>
                    <td>{review.old_value}</td>
                    <td>{review.new_value}</td>
                    <td>{review.source_run_id}</td>
                    <td>
                      <button onClick={() => void resolveReview("companies", review.id ?? "", "keep_old")}>Keep old</button>
                      <button onClick={() => void resolveReview("companies", review.id ?? "", "keep_new")}>Keep new</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <h3>People Conflicts ({peopleReviews.length})</h3>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Person ID</th>
                  <th>Field</th>
                  <th>Old Value</th>
                  <th>New Value</th>
                  <th>Source Run</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {peopleReviews.map((review) => (
                  <tr key={review.id}>
                    <td>{review.person_id}</td>
                    <td>{review.field_name}</td>
                    <td>{review.old_value}</td>
                    <td>{review.new_value}</td>
                    <td>{review.source_run_id}</td>
                    <td>
                      <button onClick={() => void resolveReview("people", review.id ?? "", "keep_old")}>Keep old</button>
                      <button onClick={() => void resolveReview("people", review.id ?? "", "keep_new")}>Keep new</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {selectedRunDetail && (
        <div
          className="dialog-overlay"
          role="presentation"
          onClick={() => {
            activeRunDetailIdRef.current = null;
            setSelectedRunId(null);
            setSelectedRunDetail(null);
            setSelectedRunCompanies([]);
            setRunCompaniesLoading(false);
            setRunCompaniesProgress(null);
            setRunCompaniesError("");
            setStepSummariesLoading(false);
            setStepSummariesProgress(0);
            setStepSummariesProgressSteps([]);
            setStepSummariesError("");
            setLogsLoading(false);
            setLogsProgress(0);
            setLogsError("");
          }}
        >
          <dialog open className="dialog" onClick={(event) => event.stopPropagation()}>
          <header>
            <strong>Run Detail: {selectedRunDetail.id}</strong>
            <button
              onClick={() => {
                activeRunDetailIdRef.current = null;
                setSelectedRunId(null);
                setSelectedRunDetail(null);
                setSelectedRunCompanies([]);
                setRunCompaniesLoading(false);
                setRunCompaniesProgress(null);
                setRunCompaniesError("");
                setStepSummariesLoading(false);
                setStepSummariesProgress(0);
                setStepSummariesProgressSteps([]);
                setStepSummariesError("");
                setLogsLoading(false);
                setLogsProgress(0);
                setLogsError("");
              }}
            >
              Close
            </button>
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
              onClick={() => {
                setRunDetailTab("companies");
                if (selectedRunId && selectedRunCompanies.length === 0) {
                  void loadRunCompanies(selectedRunId);
                }
              }}
            >
              Companies
            </button>
            <button
              className={runDetailTab === "input_json" ? "active" : ""}
              onClick={() => setRunDetailTab("input_json")}
            >
              Input JSON
            </button>
          </div>
          {runDetailTab === "overview" ? (
            <div className="dialog-body">
              {runDetailLoading && <p className="status">Loading run overview...</p>}
              {runDetailError && <p className="error">{runDetailError}</p>}
              <p>Status: {selectedRunDetail.status}</p>
              <p>
                Progress: {selectedRunDetail.progress.completedSteps}/{selectedRunDetail.progress.totalSteps}
              </p>
              <p>Message: {selectedRunDetail.progress.message}</p>
              <p>Artifacts: {selectedRunDetail.analysisRunDir ?? "-"}</p>
              {stepSummariesLoading && (
                <p className="status">Loading step summaries... {stepSummariesProgress}%</p>
              )}
              {stepSummariesError && <p className="error">{stepSummariesError}</p>}
              {stepSummariesLoading && stepSummariesProgressSteps.length > 0 && (
                <div className="table-wrap">
                  <table className="step-summary-table">
                    <thead>
                      <tr>
                        <th>Step</th>
                        <th>Type</th>
                        <th>Progress</th>
                        <th>Input Rows</th>
                        <th>Output Rows</th>
                      </tr>
                    </thead>
                    <tbody>
                      {stepSummariesProgressSteps.map((step) => (
                        <tr key={step.id}>
                          <td>{step.title}</td>
                          <td>{step.type}</td>
                          <td>{step.percent}% <span className={`status-pill status-${step.status === "running" ? "running" : step.status === "failed" ? "failed" : step.status === "finished" ? "finished" : "not_started"}`}>{step.status}</span></td>
                          <td>-</td>
                          <td>-</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
              {selectedRunDetail.stepSummaries && selectedRunDetail.stepSummaries.length > 0 && (
                <div className="table-wrap">
                  <table className="step-summary-table">
                    <thead>
                      <tr>
                        <th>Step</th>
                        <th>Type</th>
                        <th>Progress</th>
                        <th>Input Rows</th>
                        <th>Output Rows</th>
                      </tr>
                    </thead>
                    <tbody>
                      {selectedRunDetail.stepSummaries.map((step) => (
                        <tr key={step.id}>
                          <td>{step.title}</td>
                          <td>{step.type}</td>
                          <td>
                            <span>{step.progressValue ?? "-"}</span>{" "}
                            <span className={`status-pill status-${step.status}`}>{toStepStatusLabel(step.status)}</span>
                          </td>
                          <td>{step.inputRows ?? "-"}</td>
                          <td>{step.outputRows ?? "-"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
              {selectedRunDetail.error && <p className="error">{selectedRunDetail.error}</p>}
              {logsLoading && <p className="status">Loading logs... {logsProgress}%</p>}
              {logsError && <p className="error">{logsError}</p>}
              <pre className="log-box">
                {selectedRunDetail.logs.slice(-60).map((log) => `[${log.ts}] ${log.source} ${log.line}`).join("\n")}
              </pre>
            </div>
          ) : runDetailTab === "companies" ? (
            <div className="table-wrap">
              {runCompaniesLoading && (
                <p className="status">
                  Loading companies
                  {runCompaniesProgress !== null ? `... ${runCompaniesProgress}%` : "..."}
                </p>
              )}
              {runCompaniesError && <p className="error">{runCompaniesError}</p>}
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
                  {!runCompaniesLoading && selectedRunCompanies.length === 0 && (
                    <tr>
                      <td colSpan={5}>No company rows loaded for this run yet.</td>
                    </tr>
                  )}
                  {selectedRunCompanies.map((company) => (
                    <tr key={`${company.run_id}-${company.company_id}`}>
                      <td>{company.company_name}</td>
                      <td>{company.company_domain}</td>
                      <td>{company.decision}</td>
                      <td>{company.confidence}</td>
                      <td className="value-wrap">{formatEvidence(company.evidence ?? "")}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="dialog-body">
              <pre className="log-box">
                {selectedRunDetail.inputConfigJson?.trim().length
                  ? selectedRunDetail.inputConfigJson
                  : "No original input JSON was captured for this run."}
              </pre>
            </div>
          )}
          </dialog>
        </div>
      )}

      {selectedCompany && (
        <div className="dialog-overlay" role="presentation" onClick={() => setSelectedCompany(null)}>
          <dialog open className="dialog" onClick={(event) => event.stopPropagation()}>
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
              <table>
                <thead>
                  <tr>
                    <th>Field</th>
                    <th>Value</th>
                  </tr>
                </thead>
                <tbody>
                  {buildCompanyDetailRows(selectedCompany).map((row) => (
                    <tr key={row.field}>
                      <td>{row.field}</td>
                      <td className="value-wrap">{row.value}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="table-wrap">
              <table className="company-people-table">
                <colgroup>
                  <col className="col-apollo-id" />
                  <col className="col-name" />
                  <col className="col-title" />
                  <col className="col-email" />
                  <col className="col-linkedin" />
                  <col className="col-location" />
                </colgroup>
                <thead>
                  <tr>
                    <th>Apollo ID</th>
                    <th>Name</th>
                    <th>Title</th>
                    <th>Email</th>
                    <th>LinkedIn</th>
                    <th>Location</th>
                  </tr>
                </thead>
                <tbody>
                  {selectedCompanyPeople.length === 0 && (
                    <tr>
                      <td colSpan={6}>No people results for this company yet.</td>
                    </tr>
                  )}
                  {selectedCompanyPeople.map((person) => (
                    <tr
                      key={`${person.person_id ?? ""}-${person.company_id ?? ""}-${person.email ?? ""}-${person.full_name ?? ""}`}
                    >
                      <td>{person.apollo_contact_id ?? person.person_id ?? ""}</td>
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
        </div>
      )}
    </main>
  );
}
