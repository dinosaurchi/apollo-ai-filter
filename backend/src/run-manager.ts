import { EventEmitter } from "node:events";
import { randomUUID } from "node:crypto";
import path from "node:path";
import fsp from "node:fs/promises";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { parseCsv, type CsvRow, toSnakeCase } from "./csv";
import { collectRequiredInputFields, RunConfigSchema, type RunConfig } from "./run-config";

type RunStatus = "queued" | "running" | "completed" | "failed" | "cancelled";

type RunProgress = {
  totalSteps: number;
  completedSteps: number;
  currentStep: string | null;
  message: string;
};

type RunLog = {
  ts: string;
  source: "stdout" | "stderr" | "system";
  line: string;
};

type RunMeta = {
  id: string;
  status: RunStatus;
  createdAt: string;
  updatedAt: string;
  inputFileName: string;
  csvHeaders: string[];
  config: RunConfig;
  effectiveConfigPath: string;
  inputCsvPath: string;
  runRootDir: string;
  analysisOutputRoot: string;
  analysisRunDir: string | null;
  progress: RunProgress;
  pid: number | null;
  error: string | null;
  logs: RunLog[];
  cancelledAt: string | null;
};

type CreateRunInput = {
  configJson: string;
  csvFileName: string;
  csvBuffer: Buffer;
};

export type RunSummary = {
  id: string;
  status: RunStatus;
  createdAt: string;
  updatedAt: string;
  inputFileName: string;
  progress: RunProgress;
  error: string | null;
  pid: number | null;
};

type ValidateResult = {
  config: RunConfig;
  csvHeaders: string[];
  missingRequiredColumns: string[];
};

type ParseOutcome = {
  config: RunConfig;
  csvText: string;
  csvHeaders: string[];
};

type GetRunCompaniesOptions = {
  includeRaw?: boolean;
};

export type RunStepSummary = {
  id: string;
  type: string;
  title: string;
  inputRows: number | null;
  outputRows: number | null;
  progressText: string;
  status: "not_started" | "running" | "cancelled" | "failed" | "finished";
};

type RunningProc = {
  runId: string;
  child: ChildProcessWithoutNullStreams;
};

type ProviderCatalog = {
  providers?: Array<{
    id?: string;
    env?: string[];
    models?: Record<string, unknown>;
  }>;
};

const DEFAULT_MODEL_FALLBACK = "opencode/gpt-5-nano";

function nowIso(): string {
  return new Date().toISOString();
}

function basenameSafe(name: string): string {
  return name.replace(/[^a-zA-Z0-9._-]+/g, "_");
}

function toSummary(meta: RunMeta): RunSummary {
  return {
    id: meta.id,
    status: meta.status,
    createdAt: meta.createdAt,
    updatedAt: meta.updatedAt,
    inputFileName: meta.inputFileName,
    progress: meta.progress,
    error: meta.error,
    pid: meta.pid
  };
}

function parseJsonObject(value: string): Record<string, unknown> | null {
  try {
    const parsed = JSON.parse(value) as unknown;
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>;
    }
    return null;
  } catch {
    return null;
  }
}

async function ensureDir(dirPath: string): Promise<void> {
  await fsp.mkdir(dirPath, { recursive: true });
}

async function writeJson(filePath: string, value: unknown): Promise<void> {
  await ensureDir(path.dirname(filePath));
  await fsp.writeFile(filePath, JSON.stringify(value, null, 2) + "\n", "utf8");
}

async function readJson<T>(filePath: string): Promise<T> {
  const raw = await fsp.readFile(filePath, "utf8");
  return JSON.parse(raw) as T;
}

async function pathExists(filePath: string): Promise<boolean> {
  try {
    await fsp.access(filePath);
    return true;
  } catch {
    return false;
  }
}

function normalizeConfigForRun(config: RunConfig, runAnalysisOutputRoot: string): RunConfig {
  return {
    ...config,
    io: {
      ...(config.io ?? {}),
      output_root: runAnalysisOutputRoot,
      copy_input_csv: true
    }
  };
}

function parseAnalyzeLine(line: string): { message: string; details: Record<string, unknown> | null } | null {
  const trimmed = line.trim();
  const prefix = "[analyze-companies]";
  if (!trimmed.startsWith(prefix)) return null;
  const afterPrefix = trimmed.slice(prefix.length).trim();
  const firstSpace = afterPrefix.indexOf(" ");
  if (firstSpace <= 0) return null;
  const afterTs = afterPrefix.slice(firstSpace + 1).trim();
  const detailsStart = afterTs.indexOf("{");
  if (detailsStart < 0) {
    return { message: afterTs, details: null };
  }
  const message = afterTs.slice(0, detailsStart).trim();
  const details = parseJsonObject(afterTs.slice(detailsStart).trim());
  return { message, details };
}

function updateProgressFromLine(meta: RunMeta, line: string): void {
  const parsed = parseAnalyzeLine(line);
  if (!parsed) return;
  const noisyMessages = new Set([
    "creating opencode session",
    "opencode session created"
  ]);
  if (!noisyMessages.has(parsed.message)) {
    meta.progress.message = parsed.message;
  }
  if (parsed.message === "executing step") {
    const step = typeof parsed.details?.step === "string" ? parsed.details.step : null;
    meta.progress.currentStep = step;
  }
  if (
    parsed.message.endsWith("step completed")
    && typeof parsed.details?.step === "string"
    && meta.progress.currentStep === parsed.details.step
  ) {
    meta.progress.completedSteps = Math.min(meta.progress.totalSteps, meta.progress.completedSteps + 1);
  }
  if (parsed.message === "finalization completed") {
    meta.progress.currentStep = "finalization";
  }
}

function detectDoneOutputDir(line: string): string | null {
  const trimmed = line.trim();
  const match = trimmed.match(/^Done\.\s+Output:\s+(.+)$/i);
  return match?.[1]?.trim() ?? null;
}

function detectRunDirFromAnalyzeLine(line: string): string | null {
  const parsed = parseAnalyzeLine(line);
  if (!parsed || parsed.message !== "run directories prepared") return null;
  const runDir = parsed.details?.run_dir;
  return typeof runDir === "string" && runDir.trim().length > 0 ? runDir : null;
}

async function newestChildDirectory(dirPath: string): Promise<string | null> {
  if (!(await pathExists(dirPath))) return null;
  const entries = await fsp.readdir(dirPath, { withFileTypes: true });
  const withStats = await Promise.all(
    entries
      .filter((entry) => entry.isDirectory())
      .map(async (entry) => {
        const fullPath = path.resolve(dirPath, entry.name);
        const stat = await fsp.stat(fullPath);
        return { fullPath, mtimeMs: stat.mtimeMs };
      })
  );
  withStats.sort((a, b) => b.mtimeMs - a.mtimeMs);
  return withStats[0]?.fullPath ?? null;
}

function rowValue(row: CsvRow, field: string): string {
  return row[field] ?? "";
}

function parseUnknownArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.map((item) => String(item ?? "").trim()).filter((item) => item.length > 0);
}

function detectDecisionField(headers: string[]): string {
  if (headers.includes("Decision-Final")) return "Decision-Final";
  const fallback = headers.find((h) => /^Decision/i.test(h));
  return fallback ?? "Decision-Final";
}

function detectConfidenceField(headers: string[]): string {
  if (headers.includes("Confidence-Final")) return "Confidence-Final";
  const fallback = headers.find((h) => /^Confidence/i.test(h));
  return fallback ?? "Confidence-Final";
}

function detectEvidenceField(headers: string[]): string | null {
  return headers.find((h) => /^Evidence/i.test(h)) ?? null;
}

function parseProviderModel(model: string): { providerID: string; modelID: string } {
  const trimmed = model.trim();
  const sep = trimmed.indexOf("/");
  if (sep <= 0 || sep >= trimmed.length - 1) {
    throw new Error(`Invalid model "${model}". Expected provider/model format.`);
  }
  return {
    providerID: trimmed.slice(0, sep),
    modelID: trimmed.slice(sep + 1)
  };
}

export class RunManager extends EventEmitter {
  private readonly runs = new Map<string, RunMeta>();
  private readonly running = new Map<string, RunningProc>();
  private readonly queue: string[] = [];
  private analyzerReady = false;

  public constructor(
    private readonly outputRoot: string,
    private readonly analyzeScriptDir: string,
    private readonly opencodeServerUrl: string,
    private readonly analyzerNodeOptions: string
  ) {
    super();
  }

  public async init(): Promise<void> {
    const runsRoot = path.resolve(this.outputRoot, "runs");
    await ensureDir(runsRoot);
    const runIds = await fsp.readdir(runsRoot).catch(() => []);
    for (const runId of runIds) {
      const metaPath = path.resolve(runsRoot, runId, "meta.json");
      if (!(await pathExists(metaPath))) continue;
      try {
        const meta = await readJson<RunMeta>(metaPath);
        if (meta.status === "running" || meta.status === "queued") {
          meta.status = "queued";
          meta.error = null;
          meta.progress.message = "Resuming after backend restart...";
          meta.updatedAt = nowIso();
          meta.logs.push({
            ts: nowIso(),
            source: "system",
            line: "Backend restarted; run was re-queued for resume."
          });
          if (meta.logs.length > 500) meta.logs = meta.logs.slice(-500);
        }
        this.runs.set(meta.id, meta);
        if (meta.status === "queued") {
          this.queue.push(meta.id);
          await this.persistMeta(meta);
        }
      } catch {
        // Ignore invalid historical records.
      }
    }
    await this.maybeStartNext();
  }

  public validateInput(input: CreateRunInput): ValidateResult {
    const parsed = this.parseInputOrThrow(input);
    const missing = this.computeMissingColumns(parsed.config, parsed.csvHeaders);
    return {
      config: parsed.config,
      csvHeaders: parsed.csvHeaders,
      missingRequiredColumns: missing
    };
  }

  public async createRun(input: CreateRunInput): Promise<RunSummary> {
    const parsed = this.parseInputOrThrow(input);
    const _missingReferencedColumns = this.computeMissingColumns(parsed.config, parsed.csvHeaders);
    const prepared = await this.prepareConfigForRuntime(parsed.config);

    const id = randomUUID();
    const createdAt = nowIso();
    const runRootDir = path.resolve(this.outputRoot, "runs", id);
    const inputDir = path.resolve(runRootDir, "input");
    const analysisOutputRoot = path.resolve(runRootDir, "analysis-output");
    await ensureDir(inputDir);
    await ensureDir(analysisOutputRoot);

    const inputCsvPath = path.resolve(inputDir, basenameSafe(input.csvFileName || "input.csv"));
    await fsp.writeFile(inputCsvPath, parsed.csvText, "utf8");
    const originalConfigPath = path.resolve(inputDir, "config.original.json");
    await fsp.writeFile(originalConfigPath, input.configJson, "utf8");
    const effectiveConfig = normalizeConfigForRun(prepared.config, analysisOutputRoot);
    const effectiveConfigPath = path.resolve(inputDir, "config.effective.json");
    await writeJson(effectiveConfigPath, effectiveConfig);

    const meta: RunMeta = {
      id,
      status: "queued",
      createdAt,
      updatedAt: createdAt,
      inputFileName: basenameSafe(input.csvFileName || "input.csv"),
      csvHeaders: parsed.csvHeaders,
      config: effectiveConfig,
      effectiveConfigPath,
      inputCsvPath,
      runRootDir,
      analysisOutputRoot,
      analysisRunDir: null,
      progress: {
        totalSteps: effectiveConfig.steps.length + 1,
        completedSteps: 0,
        currentStep: null,
        message: "Queued"
      },
      pid: null,
      error: null,
      logs: [],
      cancelledAt: null
    };
    for (const warning of prepared.warnings) {
      meta.logs.push({
        ts: nowIso(),
        source: "system",
        line: warning
      });
    }

    this.runs.set(meta.id, meta);
    this.queue.push(meta.id);
    await this.persistMeta(meta);
    this.emitUpdate(meta);
    void this.maybeStartNext();
    return toSummary(meta);
  }

  public listRuns(): RunSummary[] {
    return Array.from(this.runs.values())
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt))
      .map(toSummary);
  }

  public getRun(runId: string): RunMeta | null {
    return this.runs.get(runId) ?? null;
  }

  public async cancelRun(runId: string): Promise<RunSummary> {
    const meta = this.runs.get(runId);
    if (!meta) throw new Error(`Run not found: ${runId}`);
    if (meta.status === "completed" || meta.status === "failed" || meta.status === "cancelled") {
      return toSummary(meta);
    }

    meta.cancelledAt = nowIso();
    meta.progress.message = "Cancelling...";
    const proc = this.running.get(runId);
    if (proc) {
      proc.child.kill("SIGTERM");
    } else {
      meta.status = "cancelled";
      meta.updatedAt = nowIso();
      await this.persistMeta(meta);
      this.emitUpdate(meta);
    }
    return toSummary(meta);
  }

  public async getRunCompanies(
    runId: string,
    options: GetRunCompaniesOptions = {}
  ): Promise<Array<Record<string, string>>> {
    const includeRaw = options.includeRaw ?? true;
    const meta = this.runs.get(runId);
    if (!meta) throw new Error(`Run not found: ${runId}`);

    let analysisRunDir = meta.analysisRunDir;
    if (!analysisRunDir) {
      analysisRunDir = await newestChildDirectory(meta.analysisOutputRoot);
      if (!analysisRunDir) return [];
      meta.analysisRunDir = analysisRunDir;
      meta.updatedAt = nowIso();
      await this.persistMeta(meta);
      this.emitUpdate(meta);
    }

    const outputCsv = meta.config.finalize?.output_csv ?? "A3.csv";
    const candidatePaths: string[] = [path.resolve(analysisRunDir, "final", outputCsv)];
    let filterOutPath = "";
    for (const step of [...meta.config.steps].reverse()) {
      if (step.type === "ai_text" || step.type === "web_ai" || step.type === "filter") {
        const outPath = path.resolve(analysisRunDir, "steps", step.id, "out.csv");
        candidatePaths.push(outPath);
        if (step.type === "filter" && !filterOutPath) filterOutPath = outPath;
      }
    }
    const sourcePath = await (async (): Promise<string | null> => {
      for (const p of candidatePaths) {
        if (await pathExists(p)) return p;
      }
      return null;
    })();
    if (!sourcePath) {
      const partialOnly = await this.buildAiTextPartialCompanies(meta, analysisRunDir, includeRaw);
      if (partialOnly) return partialOnly;
      return [];
    }

    if (sourcePath === filterOutPath) {
      const partial = await this.buildAiTextPartialCompanies(meta, analysisRunDir, includeRaw);
      if (partial) return partial;
    }

    const parsed = parseCsv(await fsp.readFile(sourcePath, "utf8"));
    const decisionField = detectDecisionField(parsed.headers);
    const confidenceField = detectConfidenceField(parsed.headers);
    const evidenceField = detectEvidenceField(parsed.headers);
    const idField = meta.config.run?.id_field ?? "__row_id";
    return parsed.rows.map((row) => ({
      run_id: runId,
      company_id: rowValue(row, "apollo_account_id") || rowValue(row, idField),
      company_name: rowValue(row, "name") || rowValue(row, "Company Name"),
      company_domain: rowValue(row, "domain") || rowValue(row, "website") || rowValue(row, "Website"),
      decision: rowValue(row, decisionField),
      confidence: rowValue(row, confidenceField),
      evidence: evidenceField ? rowValue(row, evidenceField) : "",
      raw: includeRaw ? JSON.stringify(row) : ""
    }));
  }

  private async buildAiTextPartialCompanies(
    meta: RunMeta,
    analysisRunDir: string,
    includeRaw: boolean
  ): Promise<Array<Record<string, string>> | null> {
    const aiStep = meta.config.steps.find((step) => step.type === "ai_text");
    if (!aiStep) return null;
    const stepDir = path.resolve(analysisRunDir, "steps", aiStep.id);
    const inCsvPath = path.resolve(stepDir, "in.csv");
    const batchesDir = path.resolve(stepDir, "batches");
    if (!(await pathExists(inCsvPath)) || !(await pathExists(batchesDir))) return null;

    const inParsed = parseCsv(await fsp.readFile(inCsvPath, "utf8"));
    const byId = new Map<string, { decision: string; confidence: string; evidence: string }>();

    const entries = await fsp.readdir(batchesDir, { withFileTypes: true });
    for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      const parsedPath = path.resolve(batchesDir, entry.name, "response.parsed.json");
      if (!(await pathExists(parsedPath))) continue;
      try {
        const raw = JSON.parse(await fsp.readFile(parsedPath, "utf8")) as unknown;
        if (!Array.isArray(raw)) continue;
        for (const item of raw) {
          if (!item || typeof item !== "object" || Array.isArray(item)) continue;
          const rec = item as Record<string, unknown>;
          const id = String(rec.id ?? "").trim();
          if (!id) continue;
          const evidence = parseUnknownArray(rec.evidence_snippets).join(" | ");
          byId.set(id, {
            decision: String(rec.decision ?? ""),
            confidence: String(rec.confidence ?? ""),
            evidence
          });
        }
      } catch {
        // Ignore malformed partial files and continue.
      }
    }
    if (byId.size === 0) return null;

    const idField = meta.config.run?.id_field ?? "__row_id";
    return inParsed.rows.map((row) => {
      const idCandidates = [rowValue(row, idField), rowValue(row, "__row_id"), rowValue(row, "id"), rowValue(row, "apollo_account_id")];
      const resolved = idCandidates.find((id) => id && byId.has(id));
      const partial = resolved ? byId.get(resolved) : null;
      return {
        run_id: meta.id,
        company_id: rowValue(row, "apollo_account_id") || rowValue(row, idField),
        company_name: rowValue(row, "name") || rowValue(row, "Company Name"),
        company_domain: rowValue(row, "domain") || rowValue(row, "website") || rowValue(row, "Website"),
        decision: partial?.decision ?? "",
        confidence: partial?.confidence ?? "",
        evidence: partial?.evidence ?? "",
        raw: includeRaw ? JSON.stringify(row) : ""
      };
    });
  }

  public async getRunPeople(runId: string): Promise<Array<Record<string, string>>> {
    const meta = this.runs.get(runId);
    if (!meta) throw new Error(`Run not found: ${runId}`);
    if (!meta.analysisRunDir) return [];
    const peopleStep = meta.config.steps.find((step) => step.type === "apollo_people");
    if (!peopleStep) return [];
    const peoplePath = path.resolve(meta.analysisRunDir, "steps", peopleStep.id, "out.csv");
    if (!(await pathExists(peoplePath))) return [];
    const parsed = parseCsv(await fsp.readFile(peoplePath, "utf8"));
    return parsed.rows.map((row) => ({ run_id: runId, ...row }));
  }

  public async getRunStepSummaries(runId: string): Promise<RunStepSummary[]> {
    const meta = this.runs.get(runId);
    if (!meta) throw new Error(`Run not found: ${runId}`);

    let analysisRunDir = meta.analysisRunDir;
    if (!analysisRunDir) {
      analysisRunDir = await newestChildDirectory(meta.analysisOutputRoot);
      if (!analysisRunDir) return [];
      meta.analysisRunDir = analysisRunDir;
      meta.updatedAt = nowIso();
      await this.persistMeta(meta);
      this.emitUpdate(meta);
    }

    const countRows = async (filePath: string): Promise<number | null> => {
      if (!(await pathExists(filePath))) return null;
      const content = await fsp.readFile(filePath, "utf8");
      return parseCsv(content).rows.length;
    };

    const startedSteps = new Set<string>();
    const completedSteps = new Set<string>();
    for (const log of meta.logs) {
      const parsed = parseAnalyzeLine(log.line);
      if (!parsed || typeof parsed.details?.step !== "string") continue;
      if (parsed.message === "executing step") startedSteps.add(parsed.details.step);
      if (parsed.message.endsWith("step completed")) completedSteps.add(parsed.details.step);
    }

    const stepOrder = meta.config.steps.map((step) => step.id);
    const currentStep = typeof meta.progress.currentStep === "string" ? meta.progress.currentStep : null;
    const currentFromProgress = currentStep && stepOrder.includes(currentStep) ? currentStep : null;
    const currentFromLogs = [...stepOrder].reverse().find((stepId) => startedSteps.has(stepId) && !completedSteps.has(stepId)) ?? null;
    const activeStepId = currentFromProgress ?? currentFromLogs;

    return Promise.all(
      meta.config.steps.map(async (step) => {
        const stepDir = path.resolve(analysisRunDir, "steps", step.id);
        const inputRows = await countRows(path.resolve(stepDir, "in.csv"));
        const outputRows = await countRows(path.resolve(stepDir, "out.csv"));
        const inferStatus = (): RunStepSummary["status"] => {
          if (meta.status === "queued") return "not_started";
          if (meta.status === "running") {
            if (completedSteps.has(step.id)) return "finished";
            if (step.id === activeStepId) return "running";
            if (startedSteps.has(step.id) && outputRows !== null) return "finished";
            return "not_started";
          }
          if (meta.status === "completed") return "finished";
          if (meta.status === "failed") {
            if (completedSteps.has(step.id)) return "finished";
            if (step.id === activeStepId) return "failed";
            if (startedSteps.has(step.id) && outputRows !== null) return "finished";
            return "not_started";
          }
          if (meta.status === "cancelled") {
            if (completedSteps.has(step.id)) return "finished";
            if (step.id === activeStepId) return "cancelled";
            if (startedSteps.has(step.id) && outputRows !== null) return "finished";
            return "not_started";
          }
          return "not_started";
        };
        const status = inferStatus();
        const progressText = (() => {
          if (status === "finished") return "Completed";
          if (status === "running") return "In progress";
          if (status === "failed") return "Stopped (error)";
          if (status === "cancelled") return "Cancelled";
          return "Pending";
        })();
        return {
          id: step.id,
          type: step.type,
          title: step.id,
          inputRows,
          outputRows,
          progressText,
          status
        };
      })
    );
  }

  public async getAllCompanies(): Promise<Array<Record<string, string>>> {
    const all: Array<Record<string, string>> = [];
    for (const run of this.listRuns()) {
      const companies = await this.getRunCompanies(run.id, { includeRaw: false });
      if (companies.length === 0) continue;
      all.push(...companies);
    }
    return all;
  }

  public async getAllPeople(): Promise<Array<Record<string, string>>> {
    const all: Array<Record<string, string>> = [];
    for (const run of this.listRuns()) {
      const people = await this.getRunPeople(run.id);
      if (people.length === 0) continue;
      all.push(...people);
    }
    return all;
  }

  private parseInputOrThrow(input: CreateRunInput): ParseOutcome {
    if (!input.csvBuffer || input.csvBuffer.length === 0) {
      throw new Error("CSV file is empty");
    }
    const csvText = input.csvBuffer.toString("utf8");
    const csvParsed = parseCsv(csvText);
    if (csvParsed.headers.length === 0) {
      throw new Error("CSV has no headers");
    }
    const rawConfig = JSON.parse(input.configJson) as unknown;
    const config = RunConfigSchema.parse(rawConfig);
    return {
      config,
      csvText,
      csvHeaders: csvParsed.headers
    };
  }

  private computeMissingColumns(config: RunConfig, csvHeaders: string[]): string[] {
    const canonical = new Set<string>();
    for (const header of csvHeaders) {
      canonical.add(header);
      canonical.add(toSnakeCase(header));
    }
    // Common Apollo export aliases.
    if (canonical.has("company_name")) canonical.add("name");
    if (canonical.has("apollo_account_id")) canonical.add("id");
    const required = collectRequiredInputFields(config);
    return required.filter((field) => !canonical.has(field));
  }

  private async maybeStartNext(): Promise<void> {
    if (this.running.size > 0) return;
    const nextId = this.queue.shift();
    if (!nextId) return;
    const meta = this.runs.get(nextId);
    if (!meta || meta.status !== "queued") {
      await this.maybeStartNext();
      return;
    }
    await this.startRun(meta);
  }

  private async startRun(meta: RunMeta): Promise<void> {
    meta.status = "running";
    meta.updatedAt = nowIso();
    meta.progress.message = "Starting analyzer...";
    await this.persistMeta(meta);
    this.emitUpdate(meta);

    await this.ensureAnalyzerReady();

    const args = [
      "--dir",
      this.analyzeScriptDir,
      "run",
      "analyze:companies",
      "--",
      "--input",
      meta.inputCsvPath,
      "--config",
      meta.effectiveConfigPath,
      "--server",
      this.opencodeServerUrl
    ];
    if (meta.analysisRunDir) {
      args.push("--resume", meta.analysisRunDir);
    }
    const child = spawn("pnpm", args, {
      cwd: this.analyzeScriptDir,
      env: this.buildAnalyzerEnv()
    });
    this.running.set(meta.id, { runId: meta.id, child });
    meta.pid = child.pid ?? null;
    await this.persistMeta(meta);
    this.emitUpdate(meta);

    const handleLine = (source: RunLog["source"], rawLine: string): void => {
      const line = rawLine.trimEnd();
      if (!line) return;
      meta.logs.push({ ts: nowIso(), source, line });
      if (meta.logs.length > 500) meta.logs = meta.logs.slice(-500);
      updateProgressFromLine(meta, line);
      const doneDir = detectDoneOutputDir(line);
      if (doneDir) meta.analysisRunDir = doneDir;
      const runDir = detectRunDirFromAnalyzeLine(line);
      if (runDir) meta.analysisRunDir = runDir;
      meta.updatedAt = nowIso();
      void this.persistMeta(meta);
      this.emitUpdate(meta);
    };

    let stdoutBuf = "";
    let stderrBuf = "";
    child.stdout.on("data", (chunk: Buffer) => {
      stdoutBuf += chunk.toString("utf8");
      let idx = stdoutBuf.indexOf("\n");
      while (idx >= 0) {
        const line = stdoutBuf.slice(0, idx);
        stdoutBuf = stdoutBuf.slice(idx + 1);
        handleLine("stdout", line);
        idx = stdoutBuf.indexOf("\n");
      }
    });
    child.stderr.on("data", (chunk: Buffer) => {
      stderrBuf += chunk.toString("utf8");
      let idx = stderrBuf.indexOf("\n");
      while (idx >= 0) {
        const line = stderrBuf.slice(0, idx);
        stderrBuf = stderrBuf.slice(idx + 1);
        handleLine("stderr", line);
        idx = stderrBuf.indexOf("\n");
      }
    });

    child.on("error", async (error) => {
      meta.status = "failed";
      meta.error = error.message;
      meta.progress.message = "Failed to start";
      meta.updatedAt = nowIso();
      this.running.delete(meta.id);
      await this.persistMeta(meta);
      this.emitUpdate(meta);
      await this.maybeStartNext();
    });

    child.on("close", async (exitCode) => {
      if (stdoutBuf.trim().length > 0) handleLine("stdout", stdoutBuf);
      if (stderrBuf.trim().length > 0) handleLine("stderr", stderrBuf);

      this.running.delete(meta.id);
      meta.pid = null;
      if (!meta.analysisRunDir) {
        meta.analysisRunDir = await newestChildDirectory(meta.analysisOutputRoot);
      }
      if (meta.cancelledAt) {
        meta.status = "cancelled";
        meta.progress.message = "Cancelled";
      } else if (exitCode === 0) {
        meta.status = "completed";
        meta.progress.completedSteps = meta.progress.totalSteps;
        meta.progress.currentStep = null;
        meta.progress.message = "Completed";
      } else {
        meta.status = "failed";
        meta.error = `Analyzer exited with code ${String(exitCode)}`;
        meta.progress.message = "Failed";
      }
      meta.updatedAt = nowIso();
      await this.persistMeta(meta);
      this.emitUpdate(meta);
      await this.maybeStartNext();
    });
  }

  private async persistMeta(meta: RunMeta): Promise<void> {
    const metaPath = path.resolve(meta.runRootDir, "meta.json");
    await writeJson(metaPath, meta);
  }

  private emitUpdate(meta: RunMeta): void {
    this.emit("run_updated", toSummary(meta));
  }

  private buildAnalyzerEnv(): NodeJS.ProcessEnv {
    const existing = (process.env.NODE_OPTIONS ?? "").trim();
    const extra = this.analyzerNodeOptions.trim();
    if (!extra) return process.env;
    const nodeOptions = existing.length > 0 ? `${existing} ${extra}` : extra;
    return { ...process.env, NODE_OPTIONS: nodeOptions };
  }

  private async ensureAnalyzerReady(): Promise<void> {
    if (this.analyzerReady) return;
    const distScript = path.resolve(this.analyzeScriptDir, "dist/scripts/analyze/analyze-companies.js");
    const nodeModules = path.resolve(this.analyzeScriptDir, "node_modules");
    if (await pathExists(distScript) && await pathExists(nodeModules)) {
      this.analyzerReady = true;
      return;
    }
    const installCode = await new Promise<number>((resolve) => {
      const proc = spawn("pnpm", ["install", "--no-frozen-lockfile"], {
        cwd: this.analyzeScriptDir,
        env: process.env,
        stdio: "ignore"
      });
      proc.on("close", (exitCode) => resolve(exitCode ?? 1));
      proc.on("error", () => resolve(1));
    });
    if (installCode !== 0) throw new Error("Failed to install analyzer dependencies");
    const buildCode = await new Promise<number>((resolve) => {
      const proc = spawn("pnpm", ["build"], {
        cwd: this.analyzeScriptDir,
        env: process.env,
        stdio: "ignore"
      });
      proc.on("close", (exitCode) => resolve(exitCode ?? 1));
      proc.on("error", () => resolve(1));
    });
    if (buildCode !== 0) throw new Error("Failed to build analyzer");
    this.analyzerReady = true;
  }

  private async prepareConfigForRuntime(config: RunConfig): Promise<{ config: RunConfig; warnings: string[] }> {
    const availableModels = await this.fetchAvailableModels();
    const cloned = JSON.parse(JSON.stringify(config)) as RunConfig;
    const warnings: string[] = [];
    const fallback = DEFAULT_MODEL_FALLBACK;
    const allowExternalProviderModels = process.env.ALLOW_EXTERNAL_PROVIDER_MODELS === "true";
    const preflightCache = new Map<string, { ok: boolean; error?: string }>();

    const preflight = async (modelSpec: string): Promise<{ ok: boolean; error?: string }> => {
      const cached = preflightCache.get(modelSpec);
      if (cached) return cached;
      try {
        await this.preflightModel(modelSpec);
        const ok = { ok: true } as const;
        preflightCache.set(modelSpec, ok);
        return ok;
      } catch (error) {
        const failed = {
          ok: false,
          error: error instanceof Error ? error.message : String(error)
        } as const;
        preflightCache.set(modelSpec, failed);
        return failed;
      }
    };

    for (const step of cloned.steps) {
      if (step.type !== "ai_text" && step.type !== "web_ai") continue;
      if (step.type === "ai_text") {
        const current = step.ai.batch_size ?? 25;
        const capped = Math.min(current, 10);
        if (capped !== current) {
          step.ai.batch_size = capped;
          warnings.push(`Step ${step.id}: capped batch_size from ${current} to ${capped} to avoid oversized prompts`);
        }
      } else {
        const current = step.ai.batch_size ?? 10;
        const capped = Math.min(current, 8);
        if (capped !== current) {
          step.ai.batch_size = capped;
          warnings.push(`Step ${step.id}: capped batch_size from ${current} to ${capped} to avoid oversized prompts`);
        }
      }
      const model = step.ai.model?.trim();
      if (!model) continue;
      let key = "";
      let parsedModel: { providerID: string; modelID: string } | null = null;
      try {
        parsedModel = parseProviderModel(model);
        key = `${parsedModel.providerID}/${parsedModel.modelID}`;
      } catch {
        continue;
      }
      if (parsedModel.providerID !== "opencode" && !allowExternalProviderModels) {
        if (!availableModels.has(fallback)) {
          throw new Error(
            `Step ${step.id}: external provider model ${key} is disabled by default and fallback ${fallback} is unavailable.`
          );
        }
        step.ai.model = fallback;
        warnings.push(
          `Step ${step.id}: switched external provider model ${key} to ${fallback} `
          + `(set ALLOW_EXTERNAL_PROVIDER_MODELS=true to keep original model)`
        );
        continue;
      }
      if (!availableModels.has(key)) {
        if (!availableModels.has(fallback)) {
          const list = Array.from(availableModels).slice(0, 15).join(", ");
          throw new Error(
            `Model ${key} is unavailable on OpenCode (${this.opencodeServerUrl}). `
            + `No fallback available. Available: ${list}`
          );
        }
        step.ai.model = fallback;
        warnings.push(
          `Step ${step.id}: model ${key} unavailable on OpenCode; falling back to ${fallback}`
        );
        continue;
      }

      const checked = await preflight(key);
      if (checked.ok) continue;
      if (key !== fallback && availableModels.has(fallback)) {
        const fallbackChecked = await preflight(fallback);
        if (fallbackChecked.ok) {
          step.ai.model = fallback;
          warnings.push(
            `Step ${step.id}: model ${key} failed preflight (${checked.error}); `
            + `falling back to ${fallback}`
          );
          continue;
        }
      }
      throw new Error(
        `Step ${step.id}: model ${key} failed preflight (${checked.error ?? "unknown error"})`
      );
    }
    return { config: cloned, warnings };
  }

  private async fetchAvailableModels(): Promise<Set<string>> {
    const payload = await this.requestJsonWithTimeout<ProviderCatalog>(
      `${this.opencodeServerUrl.replace(/\/+$/, "")}/config/providers`,
      { method: "GET" },
      10000
    );
    const out = new Set<string>();
    for (const provider of payload.providers ?? []) {
      if (!provider?.id) continue;
      for (const modelID of Object.keys(provider.models ?? {})) {
        out.add(`${provider.id}/${modelID}`);
      }
    }
    return out;
  }

  private async preflightModel(modelSpec: string): Promise<void> {
    const { providerID, modelID } = parseProviderModel(modelSpec);
    const base = this.opencodeServerUrl.replace(/\/+$/, "");
    const session = await this.requestJsonWithTimeout<{ id?: string }>(
      `${base}/session`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ title: `model preflight ${new Date().toISOString()}` })
      },
      10000
    );
    if (!session.id) {
      throw new Error("session id missing");
    }
    await this.requestJsonWithTimeout(
      `${base}/session/${encodeURIComponent(String(session.id))}/message`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          model: { providerID, modelID },
          parts: [{ type: "text", text: "Reply with exactly OK." }]
        })
      },
      30000
    );
  }

  private async requestJsonWithTimeout<T>(
    url: string,
    init: RequestInit,
    timeoutMs: number
  ): Promise<T> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetch(url, { ...init, signal: controller.signal });
      const text = await response.text();
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${text.slice(0, 400)}`);
      }
      return text.trim().length === 0 ? ({} as T) : (JSON.parse(text) as T);
    } catch (error) {
      if (error instanceof Error && error.name === "AbortError") {
        throw new Error(`timeout after ${timeoutMs}ms`);
      }
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }
}
