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

type RunningProc = {
  runId: string;
  child: ChildProcessWithoutNullStreams;
};

type RunManagerOptions = {
  analyzerImage: string;
  analyzerDockerfile: string;
  opencodeAuthFile: string;
  opencodeConfigDir: string;
  opencodeDataDir: string;
};

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
  meta.progress.message = parsed.message;
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

export class RunManager extends EventEmitter {
  private readonly runs = new Map<string, RunMeta>();
  private readonly running = new Map<string, RunningProc>();
  private readonly queue: string[] = [];
  private imageReady = false;

  public constructor(
    private readonly outputRoot: string,
    private readonly analyzeScriptDir: string,
    private readonly options: RunManagerOptions
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
        if (meta.status === "running") {
          meta.status = "failed";
          meta.error = "Backend restarted while run was in progress.";
        }
        this.runs.set(meta.id, meta);
      } catch {
        // Ignore invalid historical records.
      }
    }
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
    const missingRequiredColumns = this.computeMissingColumns(parsed.config, parsed.csvHeaders);
    if (missingRequiredColumns.length > 0) {
      throw new Error(`CSV is missing required columns: ${missingRequiredColumns.join(", ")}`);
    }

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
    const effectiveConfig = normalizeConfigForRun(parsed.config, "/run/analysis-output");
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

  public async getRunCompanies(runId: string): Promise<Array<Record<string, string>>> {
    const meta = this.runs.get(runId);
    if (!meta) throw new Error(`Run not found: ${runId}`);
    if (!meta.analysisRunDir) return [];
    const outputCsv = meta.config.finalize?.output_csv ?? "A3.csv";
    const finalPath = path.resolve(meta.analysisRunDir, "final", outputCsv);
    if (!(await pathExists(finalPath))) return [];
    const parsed = parseCsv(await fsp.readFile(finalPath, "utf8"));
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
      raw: JSON.stringify(row)
    }));
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

  public async getAllCompanies(): Promise<Array<Record<string, string>>> {
    const all: Array<Record<string, string>> = [];
    for (const run of this.listRuns()) {
      if (run.status !== "completed") continue;
      const companies = await this.getRunCompanies(run.id);
      all.push(...companies);
    }
    return all;
  }

  public async getAllPeople(): Promise<Array<Record<string, string>>> {
    const all: Array<Record<string, string>> = [];
    for (const run of this.listRuns()) {
      if (run.status !== "completed") continue;
      const people = await this.getRunPeople(run.id);
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

    await this.ensureOpenCodeMountsPrepared();
    await this.ensureAnalyzerImage();

    const args = this.buildDockerRunArgs(meta);
    const child = spawn("docker", args, {
      cwd: this.analyzeScriptDir,
      env: process.env
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
      if (doneDir) {
        meta.analysisRunDir = doneDir.startsWith("/run/")
          ? path.resolve(meta.runRootDir, doneDir.slice("/run/".length))
          : doneDir;
      }
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

  private async ensureAnalyzerImage(): Promise<void> {
    if (this.imageReady) return;
    const inspect = await this.runCommand("docker", ["image", "inspect", this.options.analyzerImage]);
    if (inspect === 0) {
      this.imageReady = true;
      return;
    }
    const dockerfile = this.options.analyzerDockerfile;
    const build = await this.runCommand("docker", [
      "build",
      "-f",
      dockerfile,
      "-t",
      this.options.analyzerImage,
      this.analyzeScriptDir
    ]);
    if (build !== 0) {
      throw new Error(`Failed to build analyzer image ${this.options.analyzerImage}`);
    }
    this.imageReady = true;
  }

  private buildDockerRunArgs(meta: RunMeta): string[] {
    const runName = `apollo-filter-run-${meta.id.slice(0, 12)}`;
    const envKeys = [
      "APOLLO_API_KEY",
      "OPENAI_API_KEY",
      "ANTHROPIC_API_KEY",
      "GOOGLE_API_KEY",
      "GEMINI_API_KEY",
      "XAI_API_KEY",
      "GROQ_API_KEY",
      "MISTRAL_API_KEY",
      "OPENROUTER_API_KEY",
      "OPENCODE_SERVER_USERNAME",
      "OPENCODE_SERVER_PASSWORD"
    ];
    const envArgs = envKeys.flatMap((key) => (process.env[key] ? ["-e", key] : []));
    const configInContainer = "/run/input/config.effective.json";
    const inputInContainer = `/run/input/${meta.inputFileName}`;
    const runScript = [
      "set -euo pipefail",
      "corepack enable >/dev/null 2>&1 || true",
      "cd /workspace",
      "pnpm install --no-frozen-lockfile",
      "opencode serve --host 0.0.0.0 --port 3000 >/tmp/opencode-server.log 2>&1 &",
      "OP_PID=$!",
      "trap 'kill ${OP_PID} || true' EXIT",
      "sleep 2",
      `pnpm run analyze:companies -- --input "${inputInContainer}" --config "${configInContainer}" --server "http://127.0.0.1:3000"`
    ].join("; ");

    return [
      "run",
      "--rm",
      "--name",
      runName,
      "-v",
      `${meta.runRootDir}:/run`,
      "-v",
      `${this.analyzeScriptDir}:/workspace`,
      "-v",
      `${this.options.opencodeConfigDir}:/home/agent/.config/opencode`,
      "-v",
      `${this.options.opencodeDataDir}:/home/agent/.local/share/opencode`,
      ...envArgs,
      this.options.analyzerImage,
      "/bin/bash",
      "-lc",
      runScript
    ];
  }

  private async ensureOpenCodeMountsPrepared(): Promise<void> {
    await ensureDir(this.options.opencodeConfigDir);
    await ensureDir(this.options.opencodeDataDir);
    const opencodeConfigPath = path.resolve(this.options.opencodeConfigDir, "opencode.json");
    if (!(await pathExists(opencodeConfigPath))) {
      await fsp.writeFile(
        opencodeConfigPath,
        JSON.stringify({ $schema: "https://opencode.ai/config.json", permission: "allow" }) + "\n",
        "utf8"
      );
    }
    if (await pathExists(this.options.opencodeAuthFile)) {
      const authTarget = path.resolve(this.options.opencodeDataDir, "auth.json");
      const authContent = await fsp.readFile(this.options.opencodeAuthFile);
      await fsp.writeFile(authTarget, authContent);
    }
  }

  private async runCommand(bin: string, args: string[]): Promise<number> {
    return await new Promise<number>((resolve) => {
      const proc = spawn(bin, args, { cwd: this.analyzeScriptDir, env: process.env, stdio: "ignore" });
      proc.on("close", (code) => resolve(code ?? 1));
      proc.on("error", () => resolve(1));
    });
  }
}
