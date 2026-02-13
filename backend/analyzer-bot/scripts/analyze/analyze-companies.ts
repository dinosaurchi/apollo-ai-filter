/**
 * scripts/analyze-companies.ts
 *
 * General, config-driven pipeline to classify companies using:
 *  - Step A1: deterministic filters (codes/industry/keywords/regex)
 *  - Step A2: AI text-only classification (Decision-1 + Confidence-1)
 *  - Step A3: (only for not_sure) web scrape + AI classification (Decision-2 + Confidence-2)
 *
 * This script is designed to work with the project's existing OpenCode server integration.
 * It talks to a local OpenCode server over HTTP (default: http://127.0.0.1:3000).
 *
 * Output directory:
 *   ./output/analysis/<csv-file-name>-<timestamp>/
 *     run.json
 *     input/original.csv
 *     input/normalized.csv
 *     steps/<step-id>/...
 *     final/A3.csv (+ views)
 *
 * Run:
 *   pnpm run build
 *   node ./dist/scripts/analyze-companies.js \
 *     --input ./path/to/input.csv \
 *     --config ./path/to/config.json \
 *     --server http://127.0.0.1:3000
 */

import fsp from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import dotenv from "dotenv";
import { requestJson, serverBaseUrl } from "../../src/opencode/http";
import { callPeopleSearch, type PeopleSearchResponse } from "../../src/apollo/people_search";
import type { PeopleQueryV1Normalized } from "../../src/people_query/types";
import { assertValidPeopleQueryV1 } from "../../src/people_query/validate";

dotenv.config({ path: path.resolve(process.cwd(), ".env") });

type Row = Record<string, string>;

type RunConfig = {
  run?: {
    name?: string;
    id_field?: string;
  };
  io?: {
    output_root?: string;
    copy_input_csv?: boolean;
  };
  normalize?: {
    trim_all_strings?: boolean;
    derive?: {
      domain_from?: string; // e.g. "website"
      profile_text_fields?: string[];
    };
  };
  steps: StepConfig[];
  finalize?: {
    output_csv?: string; // default "A3.csv"
    include_not_sure_in_a3?: boolean; // default true
    views?: Array<{ name: string; where: { field: string; equals: string } }>;
  };
};

type StepConfig = FilterStep | AiTextStep | WebAiStep | ApolloPeopleStep;

type StepInput = {
  source?: "normalized" | "prev_step";
  where?: { field: string; equals: string };
};

type FilterRule =
  | { type: "code_in"; field: string; values: string[] }
  | { type: "regex"; field: string; pattern: string; flags?: string }
  | { type: "contains_any"; field: string; values: string[]; case_insensitive?: boolean }
  | { type: "equals_any"; field: string; values: string[]; case_insensitive?: boolean }
  | { type: "not_empty"; field: string }
  | { type: "is_empty"; field: string };

type FilterStep = {
  id: string;
  type: "filter";
  input?: StepInput;
  rules: {
    keep_if_any?: FilterRule[];
    drop_if_any?: FilterRule[];
  };
};

type AiTextStep = {
  id: string;
  type: "ai_text";
  input?: StepInput;
  ai: {
    serverUrl?: string;
    model?: string;
    agent?: string;
    concurrency?: number;
    session_concurrency?: number;
    batch_size?: number;
    max_attempts?: number;
    retry_delay_ms?: number;
    continue_session_id?: string;
  };
  task: {
    criteria_name: string;
    read_fields: string[];
    instructions: string[];
    label_set?: string[]; // [yes,no,not_sure]
    confidence_set?: string[]; // [high,not_sure,low]
    decision_field: string; // e.g. "Decision-1"
    confidence_field: string; // e.g. "Confidence-1"
    reason_field?: string; // e.g. "Reason-1"
    evidence_field?: string; // e.g. "Evidence-1"
  };
  routing?: {
    finalize_if?: {
      all?: Array<{ field: string; equals: string }>;
    };
  };
};

type WebAiStep = {
  id: string;
  type: "web_ai";
  input?: StepInput;
  scrape: {
    enabled?: boolean;
    concurrency?: number;
    max_pages_per_domain?: number;
    url_paths?: string[];
    timeout_ms?: number;
    cache_by_domain?: boolean;
    user_agent?: string;
    max_chars_per_page?: number;
    max_total_chars?: number;
  };
  ai: {
    serverUrl?: string;
    model?: string;
    agent?: string;
    concurrency?: number;
    batch_size?: number;
    max_attempts?: number;
    retry_delay_ms?: number;
    browse_fallback_enabled?: boolean;
    browse_fallback_min_chars?: number;
    browse_fallback_batch_size?: number;
    browse_fallback_concurrency?: number;
    browse_fallback_agent?: string;
    browse_fallback_model?: string;
    browse_fallback_max_attempts?: number;
    browse_fallback_retry_delay_ms?: number;
    continue_session_id?: string;
  };
  task: {
    criteria_name: string;
    read_fields: string[];
    instructions: string[];
    label_set?: string[];
    confidence_set?: string[];
    decision_field: string; // e.g. "Decision-2"
    confidence_field: string; // e.g. "Confidence-2"
    reason_field?: string;
    evidence_field?: string;
  };
};

type SeniorityMin = "IC" | "Manager" | "Director" | "VP" | "CLevel";

type ApolloPeopleStep = {
  id: string;
  type: "apollo_people";
  from_final?: {
    output_csv?: string; // default from finalize.output_csv or A3.csv
    decision_field?: string; // default Decision-Final
    decision_equals?: string; // default yes
    confidence_field?: string; // default Confidence-Final
    confidence_equals?: string; // default high
  };
  company?: {
    id_field?: string; // default run.id_field or id
    name_field?: string; // default name
    domain_field?: string; // default domain
  };
  people: {
    per_page?: number; // default 25
    max_pages?: number; // default 1
    people_limit_per_company?: number; // default 50
    target_roles_or_titles: string[];
    seniority_min: SeniorityMin;
    person_locations?: string[];
    q_keywords?: string[];
    max_company_ids_per_request?: number; // default 200
  };
  rate_limit?: {
    request_delay_ms?: number; // default 350
    duplicate_company_delay_ms?: number; // default 1200
    max_attempts?: number; // default 4
    retry_base_delay_ms?: number; // default 800
  };
};

type CliArgs = {
  input: string;
  config: string;
  server?: string;
  resume?: string;
  forceFinalize?: boolean;
};

type ProviderModel = {
  providerID: string;
  modelID: string;
};

const DEFAULT_LABEL_SET = ["yes", "no", "not_sure"] as const;
const DEFAULT_CONFIDENCE_SET = ["high", "not_sure", "low"] as const;

function normalizeStringList(value: unknown, fallback: readonly string[]): string[] {
  if (!Array.isArray(value)) return [...fallback];
  const out = value
    .map((v) => String(v).trim())
    .filter((v) => v.length > 0);
  return out.length > 0 ? out : [...fallback];
}

function taskLabelSet(task: { label_set?: string[] }): string[] {
  return normalizeStringList(task.label_set, DEFAULT_LABEL_SET);
}

function taskConfidenceSet(task: { confidence_set?: string[] }): string[] {
  return normalizeStringList(task.confidence_set, DEFAULT_CONFIDENCE_SET);
}

function logInfo(message: string, details?: Record<string, unknown>): void {
  const ts = new Date().toISOString();
  if (!details || Object.keys(details).length === 0) {
    console.log(`[analyze-companies] ${ts} ${message}`);
    return;
  }
  console.log(`[analyze-companies] ${ts} ${message} ${JSON.stringify(details)}`);
}

function parseArgs(argv: string[]): CliArgs {
  const out: Partial<CliArgs> = {};
  for (let i = 2; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === "--input") out.input = argv[++i];
    else if (a === "--config") out.config = argv[++i];
    else if (a === "--server") out.server = argv[++i];
    else if (a === "--resume") out.resume = argv[++i];
    else if (a === "--force-finalize") out.forceFinalize = true;
    else if (a === "--help" || a === "-h") {
      console.log(
        "Usage: node ./dist/scripts/analyze-companies.js --input <csv> --config <json> [--server http://127.0.0.1:3000] [--resume <run_dir>] [--force-finalize]",
      );
      process.exit(0);
    }
  }
  if (!out.input || !out.config) {
    throw new Error("Missing required args: --input and --config");
  }
  return out as CliArgs;
}

// ---------- CSV helpers (no deps) ----------
function parseCsv(text: string): { headers: string[]; rows: Row[] } {
  // Minimal RFC4180-ish parser with quoted fields.
  const rows: string[][] = [];
  let cur: string[] = [];
  let field = "";
  let i = 0;
  let inQuotes = false;

  const pushField = (): void => {
    cur.push(field);
    field = "";
  };
  const pushRow = (): void => {
    // ignore trailing empty line
    if (cur.length === 1 && cur[0] === "" && rows.length === 0) return;
    rows.push(cur);
    cur = [];
  };

  while (i < text.length) {
    const ch = text[i];
    if (inQuotes) {
      if (ch === '"') {
        const next = text[i + 1];
        if (next === '"') {
          field += '"';
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

    if (ch === '"') {
      inQuotes = true;
      i += 1;
      continue;
    }
    if (ch === ",") {
      pushField();
      i += 1;
      continue;
    }
    if (ch === "\n") {
      pushField();
      pushRow();
      i += 1;
      continue;
    }
    if (ch === "\r") {
      // handle CRLF
      const next = text[i + 1];
      if (next === "\n") {
        pushField();
        pushRow();
        i += 2;
        continue;
      }
      pushField();
      pushRow();
      i += 1;
      continue;
    }
    field += ch;
    i += 1;
  }
  // last field
  pushField();
  if (cur.length > 1 || cur[0] !== "" || rows.length > 0) pushRow();

  if (rows.length > 1) {
    const last = rows[rows.length - 1] ?? [];
    const isBlankTail = last.every((v) => (v ?? "").trim().length === 0);
    if (isBlankTail) rows.pop();
  }

  if (rows.length === 0) return { headers: [], rows: [] };
  const headers = rows[0].map((h) => h.trim());
  const outRows: Row[] = [];
  for (let r = 1; r < rows.length; r += 1) {
    const arr = rows[r];
    const isEmptyRow = arr.every((v) => (v ?? "").trim().length === 0);
    if (isEmptyRow) continue;
    const obj: Row = {};
    for (let c = 0; c < headers.length; c += 1) {
      obj[headers[c]] = (arr[c] ?? "").toString();
    }
    outRows.push(obj);
  }
  return { headers, rows: outRows };
}

function csvEscape(value: string): string {
  if (value.includes('"') || value.includes(",") || value.includes("\n") || value.includes("\r")) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

function toCsv(headers: string[], rows: Row[]): string {
  const lines: string[] = [];
  lines.push(headers.map(csvEscape).join(","));
  for (const row of rows) {
    lines.push(headers.map((h) => csvEscape(row[h] ?? "")).join(","));
  }
  return lines.join("\n") + "\n";
}

async function ensureDir(p: string): Promise<void> {
  await fsp.mkdir(p, { recursive: true });
}

function utcTimestamp(): string {
  // YYYYMMDDTHHmmssZ
  const d = new Date();
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mi = String(d.getUTCMinutes()).padStart(2, "0");
  const ss = String(d.getUTCSeconds()).padStart(2, "0");
  return `${yyyy}${mm}${dd}T${hh}${mi}${ss}Z`;
}

function stableHash(input: string): string {
  return crypto.createHash("sha256").update(input).digest("hex").slice(0, 16);
}

function getBasenameNoExt(p: string): string {
  const base = path.basename(p);
  return base.replace(/\.[^.]+$/, "");
}

function asLower(s: string): string {
  return (s ?? "").toLowerCase();
}

function toSnakeCase(value: string): string {
  return (value ?? "")
    .trim()
    .replace(/[^0-9A-Za-z]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();
}

function parseModelSpec(model?: string): ProviderModel | undefined {
  if (!model) return undefined;
  const normalized = model.trim();
  const separator = normalized.indexOf("/");
  if (separator <= 0 || separator >= normalized.length - 1) {
    throw new Error(
      `Invalid model "${model}". Expected format "provider/model" (example: "opencode/gpt-5-nano").`,
    );
  }
  const providerID = normalized.slice(0, separator).trim();
  const modelID = normalized.slice(separator + 1).trim();
  if (!providerID || !modelID) {
    throw new Error(
      `Invalid model "${model}". Expected format "provider/model" (example: "opencode/gpt-5-nano").`,
    );
  }
  return {
    providerID,
    modelID,
  };
}

function safeGet(row: Row, field: string): string {
  return row[field] ?? "";
}

function deriveDomain(urlRaw: string): string {
  const url = (urlRaw ?? "").trim();
  if (!url) return "";
  try {
    const withProto = url.match(/^https?:\/\//i) ? url : `https://${url}`;
    const u = new URL(withProto);
    return u.hostname.replace(/^www\./i, "");
  } catch {
    // fallback
    return url.replace(/^https?:\/\//i, "").replace(/^www\./i, "").split("/")[0] ?? "";
  }
}

function splitCodes(value: string): string[] {
  const raw = (value ?? "").trim();
  if (!raw) return [];
  return raw
    .split(/[^0-9A-Za-z]+/g)
    .map((x) => x.trim())
    .filter((x) => x.length > 0);
}

// ---------- Step Input loading ----------
async function readCsvFile(filePath: string): Promise<{ headers: string[]; rows: Row[] }>
{
  const raw = await fsp.readFile(filePath, "utf8");
  return parseCsv(raw);
}

async function writeText(filePath: string, content: string): Promise<void> {
  await ensureDir(path.dirname(filePath));
  await fsp.writeFile(filePath, content, "utf8");
}

async function writeJson(filePath: string, obj: unknown): Promise<void> {
  await ensureDir(path.dirname(filePath));
  await fsp.writeFile(filePath, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

async function appendJsonl(filePath: string, obj: unknown): Promise<void> {
  await ensureDir(path.dirname(filePath));
  await fsp.appendFile(filePath, JSON.stringify(obj) + "\n", "utf8");
}

async function pathExists(filePath: string): Promise<boolean> {
  try {
    await fsp.access(filePath);
    return true;
  } catch {
    return false;
  }
}

// ---------- Normalization ----------
async function normalizeInput(
  cfg: RunConfig,
  headersIn: string[],
  rowsIn: Row[],
): Promise<{ headers: string[]; rows: Row[]; idField: string }>
{
  const trimAll = cfg.normalize?.trim_all_strings ?? true;
  const headers = [...headersIn];
  const aliasFor = new Map<string, string>();
  for (const h of headersIn) {
    const alias = toSnakeCase(h);
    if (!alias || alias === h) continue;
    aliasFor.set(h, alias);
    if (!headers.includes(alias)) headers.push(alias);
  }

  const configuredIdField = cfg.run?.id_field?.trim();
  const idField = configuredIdField && headers.includes(configuredIdField) ? configuredIdField : "__row_id";

  const domainFrom = cfg.normalize?.derive?.domain_from;
  const profileFields = cfg.normalize?.derive?.profile_text_fields ?? [];

  const needId = idField === "__row_id";
  if (needId && !headers.includes("__row_id")) headers.push("__row_id");
  if (domainFrom && !headers.includes("domain")) headers.push("domain");
  if (profileFields.length > 0 && !headers.includes("profile_text")) headers.push("profile_text");

  const rows: Row[] = rowsIn.map((r, idx) => {
    const out: Row = { ...r };
    for (const [src, alias] of aliasFor.entries()) {
      if (!(alias in out)) out[alias] = safeGet(out, src);
    }
    if (trimAll) {
      for (const k of Object.keys(out)) out[k] = (out[k] ?? "").trim();
    }
    if (needId) out.__row_id = String(idx + 1);
    if (domainFrom) out.domain = deriveDomain(safeGet(out, domainFrom));
    if (profileFields.length > 0) {
      out.profile_text = profileFields.map((f) => safeGet(out, f)).filter(Boolean).join(" | ");
    }
    return out;
  });

  return { headers, rows, idField };
}

// ---------- Filter step ----------
function evalRule(rule: FilterRule, row: Row): boolean {
  const v = safeGet(row, rule.field);
  switch (rule.type) {
    case "code_in": {
      const codes = new Set(splitCodes(v));
      return rule.values.some((x) => codes.has(x));
    }
    case "regex": {
      const re = new RegExp(rule.pattern, rule.flags ?? "");
      return re.test(v);
    }
    case "contains_any": {
      const hay = rule.case_insensitive ? asLower(v) : v;
      return rule.values.some((needle) => {
        const n = rule.case_insensitive ? asLower(needle) : needle;
        return hay.includes(n);
      });
    }
    case "equals_any": {
      const hay = rule.case_insensitive ? asLower(v) : v;
      return rule.values.some((needle) => {
        const n = rule.case_insensitive ? asLower(needle) : needle;
        return hay === n;
      });
    }
    case "not_empty":
      return v.trim().length > 0;
    case "is_empty":
      return v.trim().length === 0;
    default:
      return false;
  }
}

function ruleKey(rule: FilterRule): string {
  if (rule.type === "regex") return `regex:${rule.field}:${rule.pattern}`;
  if (rule.type === "code_in") return `code_in:${rule.field}:${rule.values.join("|")}`;
  if (rule.type === "contains_any") return `contains_any:${rule.field}:${rule.values.join("|")}`;
  if (rule.type === "equals_any") return `equals_any:${rule.field}:${rule.values.join("|")}`;
  return `${rule.type}:${(rule as any).field}`;
}

type RunContext = {
  cwd: string;
  runDir: string;
  outputRoot: string;
  idField: string;
  /** Optional CLI override for OpenCode server URL */
  serverOverride?: string;
  originalPath: string;
  normalizedPath: string;
  lastStepOutPath: string;
};

async function loadStepInput(ctx: RunContext, step: { input?: StepInput }): Promise<{ headers: string[]; rows: Row[] }>
{
  const src = step.input?.source ?? "prev_step";
  const file = src === "normalized" ? ctx.normalizedPath : ctx.lastStepOutPath;
  const data = await readCsvFile(file);
  if (step.input?.where) {
    const { field, equals } = step.input.where;
    const rows = data.rows.filter((r) => safeGet(r, field) === equals);
    return { headers: data.headers, rows };
  }
  return data;
}

async function runFilterStep(ctx: RunContext, step: FilterStep): Promise<string> {
  logInfo("filter step started", { step: step.id });
  const stepDir = path.resolve(ctx.runDir, "steps", step.id);
  await ensureDir(stepDir);
  await writeJson(path.resolve(stepDir, "config.json"), step);

  const input = await loadStepInput(ctx, step);
  logInfo("filter input loaded", { step: step.id, rows: input.rows.length });
  await writeText(path.resolve(stepDir, "in.csv"), toCsv(input.headers, input.rows));

  const keepRules = step.rules.keep_if_any ?? [];
  const dropRules = step.rules.drop_if_any ?? [];

  const keepHits: Record<string, number> = {};
  const dropHits: Record<string, number> = {};
  const kept: Row[] = [];
  const dropped: Row[] = [];

  for (const row of input.rows) {
    const keep = keepRules.length === 0 ? true : keepRules.some((r) => {
      const ok = evalRule(r, row);
      if (ok) keepHits[ruleKey(r)] = (keepHits[ruleKey(r)] ?? 0) + 1;
      return ok;
    });

    const drop = dropRules.some((r) => {
      const ok = evalRule(r, row);
      if (ok) dropHits[ruleKey(r)] = (dropHits[ruleKey(r)] ?? 0) + 1;
      return ok;
    });

    if (keep && !drop) kept.push(row);
    else dropped.push(row);
  }

  await writeText(path.resolve(stepDir, "out.csv"), toCsv(input.headers, kept));
  await writeText(path.resolve(stepDir, "out.ids.txt"), kept.map((r) => safeGet(r, ctx.idField)).join("\n") + "\n");
  await writeJson(path.resolve(stepDir, "stats.json"), {
    step: step.id,
    in_count: input.rows.length,
    kept_count: kept.length,
    dropped_count: dropped.length,
    keep_rule_hits: keepHits,
    drop_rule_hits: dropHits,
  });

  // Debug sample
  const sample = {
    kept: kept.slice(0, 5),
    dropped: dropped.slice(0, 5),
  };
  await writeJson(path.resolve(stepDir, "debug.samples.json"), sample);
  logInfo("filter step completed", {
    step: step.id,
    in_count: input.rows.length,
    kept_count: kept.length,
    dropped_count: dropped.length,
  });

  return path.resolve(stepDir, "out.csv");
}

// ---------- OpenCode client (simple) ----------

type OpenCodeClient = {
  serverUrl: string;
  model?: ProviderModel;
  agent?: string;
  sessionId: string;
};

type MessageResponse = {
  parts?: Array<Record<string, unknown>>;
};

async function createSession(serverUrl: string): Promise<string> {
  const base = serverBaseUrl(serverUrl);
  logInfo("creating opencode session", { server: base });
  const resp = await requestJson<{ id?: string }>(
    `${base}/session`,
    { method: "POST", body: JSON.stringify({ title: `analyze-companies ${new Date().toISOString()}` }) },
    process.env,
  );
  if (!resp.id) throw new Error(`Invalid /session response: ${JSON.stringify(resp)}`);
  logInfo("opencode session created", { server: base, session_id: String(resp.id) });
  return String(resp.id);
}

function extractTextsFromUnknown(value: unknown, out: string[]): void {
  if (typeof value === "string") {
    if (value.trim().length > 0) out.push(value);
    return;
  }
  if (!value || typeof value !== "object") return;
  if (Array.isArray(value)) {
    for (const item of value) extractTextsFromUnknown(item, out);
    return;
  }
  const rec = value as Record<string, unknown>;
  const directCandidates = [rec.text, rec.content, rec.message, rec.delta, rec.value];
  for (const candidate of directCandidates) extractTextsFromUnknown(candidate, out);
  if (Array.isArray(rec.parts)) extractTextsFromUnknown(rec.parts, out);
}

async function sendMessage(client: OpenCodeClient, prompt: string): Promise<{ text: string; response: MessageResponse }> {
  const base = serverBaseUrl(client.serverUrl);
  const body: Record<string, unknown> = {
    parts: [{ type: "text", text: prompt }],
  };
  if (client.agent) body.agent = client.agent;
  if (client.model) body.model = client.model;

  const resp = await requestJson<MessageResponse>(
    `${base}/session/${encodeURIComponent(client.sessionId)}/message`,
    { method: "POST", body: JSON.stringify(body) },
    process.env,
  );
  const texts: string[] = [];
  extractTextsFromUnknown(resp.parts ?? [], texts);
  return { text: texts.join("\n"), response: resp };
}

function extractJsonArray(text: string, requiredObjectKeys?: string[]): unknown {
  const matchesShape = (parsed: unknown[]): boolean => {
    if (!requiredObjectKeys || requiredObjectKeys.length === 0) return true;
    if (parsed.length === 0) return false;
    return parsed.every((item) => {
      if (!item || typeof item !== "object" || Array.isArray(item)) return false;
      const rec = item as Record<string, unknown>;
      return requiredObjectKeys.every((k) => Object.prototype.hasOwnProperty.call(rec, k));
    });
  };

  const tryParseArray = (raw: string): unknown[] | null => {
    try {
      const parsed = JSON.parse(raw);
      return Array.isArray(parsed) ? parsed : null;
    } catch {
      return null;
    }
  };

  let firstParsedArray: unknown[] | null = null;

  // First, try fenced code blocks (common LLM response format).
  const fenceRegex = /```(?:json)?\s*([\s\S]*?)```/gi;
  for (const match of text.matchAll(fenceRegex)) {
    const candidate = (match[1] ?? "").trim();
    const parsed = tryParseArray(candidate);
    if (parsed) {
      if (!firstParsedArray) firstParsedArray = parsed;
      if (matchesShape(parsed)) return parsed;
    }
  }

  // Fallback: scan for the first syntactically valid top-level JSON array.
  for (let start = 0; start < text.length; start += 1) {
    if (text[start] !== "[") continue;

    let depth = 0;
    let inString = false;
    let escaped = false;
    for (let i = start; i < text.length; i += 1) {
      const ch = text[i];
      if (inString) {
        if (escaped) {
          escaped = false;
        } else if (ch === "\\") {
          escaped = true;
        } else if (ch === "\"") {
          inString = false;
        }
        continue;
      }

      if (ch === "\"") {
        inString = true;
        continue;
      }
      if (ch === "[") {
        depth += 1;
        continue;
      }
      if (ch === "]") {
        depth -= 1;
        if (depth === 0) {
          const candidate = text.slice(start, i + 1);
          const parsed = tryParseArray(candidate);
          if (parsed) {
            if (!firstParsedArray) firstParsedArray = parsed;
            if (matchesShape(parsed)) return parsed;
          }
          break;
        }
      }
    }
  }

  if (firstParsedArray) {
    throw new Error("Model returned a JSON array, but not in expected result object shape");
  }
  throw new Error(`Model did not return a parseable JSON array. Got: ${text.slice(0, 300)}`);
}

async function sleepMs(ms: number): Promise<void> {
  if (ms <= 0) return;
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeCompanyName(value: string): string {
  return (value ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function mapSeniorityMinToApollo(value: SeniorityMin): PeopleQueryV1Normalized["person_seniorities"] {
  switch (value) {
    case "IC":
      return ["ic", "manager", "director", "head", "vp", "c_suite"];
    case "Manager":
      return ["manager", "director", "head", "vp", "c_suite"];
    case "Director":
      return ["director", "head", "vp", "c_suite"];
    case "VP":
      return ["vp", "c_suite"];
    case "CLevel":
      return ["c_suite"];
    default:
      throw new Error(`Unsupported seniority_min: ${String(value)}`);
  }
}

function chunkArray<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function parseHttpStatusFromErrorMessage(message: string): number | null {
  const match = message.match(/\bHTTP\s+(\d{3})\b/i);
  if (!match) return null;
  const code = Number.parseInt(match[1], 10);
  return Number.isInteger(code) ? code : null;
}

type ApolloPersonLite = {
  apollo_person_id: string;
  full_name: string;
  title: string;
  email: string;
  linkedin_url: string;
  location: string;
  organization_id: string;
  organization_name: string;
  organization_domain: string;
};

function normalizeApolloPerson(raw: Record<string, unknown>): ApolloPersonLite {
  const firstName = typeof raw.first_name === "string" ? raw.first_name : "";
  const lastName =
    (typeof raw.last_name === "string" ? raw.last_name : "")
    || (typeof raw.last_name_obfuscated === "string" ? raw.last_name_obfuscated : "");
  const orgObj = typeof raw.organization === "object" && raw.organization ? (raw.organization as Record<string, unknown>) : null;
  const apolloPersonId =
    (typeof raw.id === "string" ? raw.id : "")
    || (typeof raw.apollo_id === "string" ? raw.apollo_id : "")
    || "";
  const fullName =
    (typeof raw.name === "string" ? raw.name : "")
    || [firstName, lastName].filter((x) => x.length > 0).join(" ");
  return {
    apollo_person_id: apolloPersonId,
    full_name: fullName,
    title: typeof raw.title === "string" ? raw.title : "",
    email: typeof raw.email === "string" ? raw.email : "",
    linkedin_url: typeof raw.linkedin_url === "string" ? raw.linkedin_url : "",
    location: typeof raw.location === "string" ? raw.location : "",
    organization_id:
      (typeof raw.organization_id === "string" ? raw.organization_id : "")
      || (typeof orgObj?.id === "string" ? orgObj.id : ""),
    organization_name:
      (typeof raw.organization_name === "string" ? raw.organization_name : "")
      || (typeof orgObj?.name === "string" ? orgObj.name : ""),
    organization_domain:
      (typeof raw.organization_primary_domain === "string" ? raw.organization_primary_domain : "")
      || (typeof orgObj?.primary_domain === "string" ? orgObj.primary_domain : ""),
  };
}

type HighYesCompany = {
  company_id: string;
  company_name: string;
  company_domain: string;
  normalized_company_name: string;
};

type LeadRow = {
  company_id: string;
  company_name: string;
  company_domain: string;
  normalized_company_name: string;
  source_list: "A_unique_name_batch" | "B_duplicate_name_single";
  request_key: string;
  apollo_person_id: string;
  full_name: string;
  title: string;
  email: string;
  linkedin_url: string;
  location: string;
  apollo_organization_id: string;
  apollo_organization_name: string;
  apollo_organization_domain: string;
};

async function callPeopleSearchWithRetry(
  query: PeopleQueryV1Normalized,
  maxAttempts: number,
  retryBaseDelayMs: number,
): Promise<PeopleSearchResponse> {
  let lastError: unknown = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      return await callPeopleSearch(query, process.env);
    } catch (error) {
      lastError = error;
      const message = error instanceof Error ? error.message : String(error);
      const status = parseHttpStatusFromErrorMessage(message);
      const retriable = status === 429 || (status !== null && status >= 500);
      if (!retriable || attempt >= maxAttempts) break;
      await sleepMs(retryBaseDelayMs * attempt);
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

// ---------- AI Text step (A2) ----------

function buildTextBatchPrompt(step: AiTextStep, idField: string, rows: Row[]): string {
  const { criteria_name, read_fields, instructions } = step.task;
  const label_set = taskLabelSet(step.task);
  const confidence_set = taskConfidenceSet(step.task);
  const items = rows.map((r) => {
    const obj: Record<string, string> = {};
    for (const f of read_fields) obj[f] = safeGet(r, f);
    obj[idField] = safeGet(r, idField);
    return obj;
  });

  return [
    `You are classifying companies for criteria: ${criteria_name}.`,
    ...instructions.map((x) => `- ${x}`),
    "",
    `Allowed decision labels: ${label_set.join(", ")}`,
    `Allowed confidence labels: ${confidence_set.join(", ")}`,
    "",
    "Return ONLY a JSON array. Each element must be:",
    `{"id":"<${idField}>","decision":"${label_set.join("|")}","confidence":"${confidence_set.join("|")}","reason_codes":["..."],"evidence_snippets":["..."]}`,
    "",
    "Items:",
    JSON.stringify(items),
  ].join("\n");
}

type AiRowResult = {
  id: string;
  decision: string;
  confidence: string;
  reason_codes?: string[];
  evidence_snippets?: string[];
};

type BatchReviewRow = {
  id: string;
  input: Row;
  ai_result: {
    decision: string | null;
    confidence: string | null;
    reason_codes: string[];
    evidence_snippets: string[];
  };
  error: string | null;
};

type WebBatchReviewRow = {
  id: string;
  input: Row;
  ai_result: {
    decision: string | null;
    confidence: string | null;
    reason_codes: string[];
    evidence_snippets: string[];
    evidence_urls: string[];
  };
  error: string | null;
};

function dedupeRowForReview(row: Row): Row {
  const keysByCanonical = new Map<string, string[]>();
  for (const key of Object.keys(row)) {
    const canonical = toSnakeCase(key) || key;
    const arr = keysByCanonical.get(canonical) ?? [];
    arr.push(key);
    keysByCanonical.set(canonical, arr);
  }

  const out: Row = {};
  for (const [canonical, keys] of keysByCanonical.entries()) {
    if (keys.length === 1) {
      const key = keys[0];
      out[key] = row[key];
      continue;
    }

    // Prefer original non-snake key for readability in review artifacts.
    const preferred =
      keys.find((k) => k !== canonical) ??
      keys[0];
    out[preferred] = row[preferred];
  }
  return out;
}

function buildBatchReviewRows(
  batch: Row[],
  idField: string,
  byId: Map<string, AiRowResult>,
  errorMessage?: string,
): BatchReviewRow[] {
  return batch.map((row) => {
    const id = safeGet(row, idField);
    const result = byId.get(id);
    return {
      id,
      input: dedupeRowForReview(row),
      ai_result: {
        decision: result?.decision ?? null,
        confidence: result?.confidence ?? null,
        reason_codes: result?.reason_codes ?? [],
        evidence_snippets: result?.evidence_snippets ?? [],
      },
      error: errorMessage ?? null,
    };
  });
}

function buildWebBatchReviewRows(
  batch: Row[],
  idField: string,
  byId: Map<string, WebAiRowResult>,
  errorMessage?: string,
): WebBatchReviewRow[] {
  return batch.map((row) => {
    const id = safeGet(row, idField);
    const result = byId.get(id);
    return {
      id,
      input: dedupeRowForReview(row),
      ai_result: {
        decision: result?.decision ?? null,
        confidence: result?.confidence ?? null,
        reason_codes: result?.reason_codes ?? [],
        evidence_snippets: result?.evidence_snippets ?? [],
        evidence_urls: result?.evidence_urls ?? [],
      },
      error: errorMessage ?? null,
    };
  });
}

function validateAiRowResult(
  step: { task: { label_set?: string[]; confidence_set?: string[] } },
  obj: any,
): AiRowResult {
  if (!obj || typeof obj !== "object") throw new Error("AI result is not an object");
  const id = String(obj.id ?? "");
  const decision = String(obj.decision ?? "");
  const confidence = String(obj.confidence ?? "");
  if (!id) throw new Error("Missing id");
  if (!taskLabelSet(step.task).includes(decision)) throw new Error(`Invalid decision: ${decision}`);
  if (!taskConfidenceSet(step.task).includes(confidence)) throw new Error(`Invalid confidence: ${confidence}`);
  const reason_codes = Array.isArray(obj.reason_codes) ? obj.reason_codes.map(String) : [];
  const evidence_snippets = Array.isArray(obj.evidence_snippets) ? obj.evidence_snippets.map(String) : [];
  return { id, decision, confidence, reason_codes, evidence_snippets };
}

function shouldFinalize(step: AiTextStep, row: Row): boolean {
  const conds = step.routing?.finalize_if?.all ?? [];
  if (conds.length === 0) {
    // Default: Decision-1=yes and Confidence-1=high
    return safeGet(row, step.task.decision_field) === "yes" && safeGet(row, step.task.confidence_field) === "high";
  }
  return conds.every((c) => safeGet(row, c.field) === c.equals);
}

async function runAiTextStep(ctx: RunContext, step: AiTextStep): Promise<{ outPath: string; finalizedIds: Set<string> }>
{
  logInfo("ai_text step started", { step: step.id });
  const stepDir = path.resolve(ctx.runDir, "steps", step.id);
  const batchesDir = path.resolve(stepDir, "batches");
  await ensureDir(stepDir);
  await ensureDir(batchesDir);
  await writeJson(path.resolve(stepDir, "config.json"), step);

  const input = await loadStepInput(ctx, step);
  logInfo("ai_text input loaded", { step: step.id, rows: input.rows.length });
  await writeText(path.resolve(stepDir, "in.csv"), toCsv(input.headers, input.rows));

  const serverUrl = ctx.serverOverride ?? step.ai.serverUrl ?? "http://127.0.0.1:3000";
  const clientBase: Omit<OpenCodeClient, "sessionId"> = {
    serverUrl,
    model: parseModelSpec(step.ai.model),
    agent: step.ai.agent,
  };
  await writeJson(path.resolve(stepDir, "session.mode.json"), {
    mode: step.ai.continue_session_id ? "shared_continue_session" : "separate_session_per_batch",
    serverUrl: clientBase.serverUrl,
    model: clientBase.model ? `${clientBase.model.providerID}/${clientBase.model.modelID}` : null,
    agent: clientBase.agent ?? null,
    continue_session_id: step.ai.continue_session_id ?? null,
  });
  if (step.ai.continue_session_id) {
    await writeJson(path.resolve(stepDir, "session.json"), {
      serverUrl: clientBase.serverUrl,
      model: clientBase.model ? `${clientBase.model.providerID}/${clientBase.model.modelID}` : null,
      agent: clientBase.agent ?? null,
      sessionId: step.ai.continue_session_id,
      mode: "shared_continue_session",
    });
  }

  const batchSize = Math.max(1, step.ai.batch_size ?? 25);
  const configuredConcurrency = Math.max(1, step.ai.concurrency ?? 3);
  const perBatchSessions = !step.ai.continue_session_id;
  const sessionConcurrency = Math.max(1, step.ai.session_concurrency ?? 1);
  const concurrency = perBatchSessions ? Math.min(configuredConcurrency, sessionConcurrency) : configuredConcurrency;
  const maxAttempts = Math.max(1, step.ai.max_attempts ?? 3);
  const retryDelayMs = Math.max(0, step.ai.retry_delay_ms ?? 400);
  const limit = createLimiter(concurrency);

  const idToResult = new Map<string, AiRowResult>();
  const outJsonl = path.resolve(stepDir, "out.jsonl");
  const errJsonl = path.resolve(stepDir, "errors.jsonl");

  const batches: Array<Row[]> = [];
  for (let i = 0; i < input.rows.length; i += batchSize) {
    batches.push(input.rows.slice(i, i + batchSize));
  }
  logInfo("ai_text batching prepared", {
    step: step.id,
    batch_size: batchSize,
    batches: batches.length,
    concurrency,
    configured_concurrency: configuredConcurrency,
    session_concurrency: perBatchSessions ? sessionConcurrency : null,
    max_attempts: maxAttempts,
    retry_delay_ms: retryDelayMs,
    server: serverUrl,
  });

  let okBatches = 0;
  let failedBatches = 0;
  let resumedBatches = 0;
  await Promise.all(
    batches.map((batch, idx) =>
      limit(async () => {
        const batchNumber = String(idx + 1).padStart(4, "0");
        const batchDir = path.resolve(batchesDir, `batch-${batchNumber}`);
        await ensureDir(batchDir);
        await writeText(path.resolve(batchDir, "in.csv"), toCsv(input.headers, batch));

        const existingResultPath = path.resolve(batchDir, "result.json");
        const existingParsedPath = path.resolve(batchDir, "response.parsed.json");
        if (await pathExists(existingResultPath) && await pathExists(existingParsedPath)) {
          try {
            const existing = JSON.parse(await fsp.readFile(existingResultPath, "utf8")) as { status?: string };
            if (existing.status === "ok") {
              const arr = JSON.parse(await fsp.readFile(existingParsedPath, "utf8"));
              if (!Array.isArray(arr)) throw new Error("Cached parsed result is not an array");
              const batchById = new Map<string, AiRowResult>();
              for (const it of arr) {
                const r = validateAiRowResult(step, it);
                idToResult.set(r.id, r);
                batchById.set(r.id, r);
              }
              await writeJson(
                path.resolve(batchDir, "review.joined.json"),
                buildBatchReviewRows(batch, ctx.idField, batchById),
              );
              resumedBatches += 1;
              okBatches += 1;
              logInfo("ai_text batch resumed from cache", { step: step.id, batch_index: idx, batch_rows: batch.length });
              return;
            }
          } catch {
            // If cache is invalid, fall through to re-run this batch.
          }
        }

        const prompt = buildTextBatchPrompt(step, ctx.idField, batch);
        await writeText(path.resolve(batchDir, "prompt.txt"), prompt);
        try {
          const sessionId = step.ai.continue_session_id ?? (await createSession(serverUrl));
          const client: OpenCodeClient = {
            ...clientBase,
            sessionId,
          };
          await writeJson(path.resolve(batchDir, "session.json"), {
            serverUrl: client.serverUrl,
            model: client.model ? `${client.model.providerID}/${client.model.modelID}` : null,
            agent: client.agent ?? null,
            sessionId: client.sessionId,
            batch_index: idx,
            batch_rows: batch.length,
          });

          let arr: unknown[] | null = null;
          let lastError: unknown = null;
          let finalText = "";
          let finalResponse: MessageResponse | null = null;
          let usedAttempt = 0;

          for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
            usedAttempt = attempt;
            const reply = await sendMessage(client, prompt);
            finalText = reply.text;
            finalResponse = reply.response;
            await writeJson(path.resolve(batchDir, `attempt-${String(attempt).padStart(2, "0")}.response.json`), reply.response);
            await writeText(path.resolve(batchDir, `attempt-${String(attempt).padStart(2, "0")}.raw.txt`), reply.text);
            try {
              const parsed = extractJsonArray(reply.text, ["id", "decision", "confidence"]);
              if (!Array.isArray(parsed)) throw new Error("Expected JSON array");
              arr = parsed;
              break;
            } catch (e) {
              lastError = e;
              if (attempt < maxAttempts) {
                await sleepMs(retryDelayMs * attempt);
              }
            }
          }

          if (!arr) {
            throw lastError instanceof Error
              ? new Error(`${lastError.message} (attempts=${maxAttempts})`)
              : new Error(`Failed to parse JSON array after ${maxAttempts} attempts`);
          }

          await appendJsonl(outJsonl, { batch_index: idx, raw: finalText, attempts: usedAttempt });
          await writeText(path.resolve(batchDir, "response.raw.txt"), finalText);
          await writeJson(path.resolve(batchDir, "response.message.json"), finalResponse ?? {});
          await writeJson(path.resolve(batchDir, "response.parsed.json"), arr);
          const batchById = new Map<string, AiRowResult>();
          for (const it of arr) {
            const r = validateAiRowResult(step, it);
            idToResult.set(r.id, r);
            batchById.set(r.id, r);
          }
          await writeJson(
            path.resolve(batchDir, "review.joined.json"),
            buildBatchReviewRows(batch, ctx.idField, batchById),
          );
          await writeJson(path.resolve(batchDir, "result.json"), {
            status: "ok",
            batch_index: idx,
            batch_rows: batch.length,
            result_count: arr.length,
            attempts: usedAttempt,
          });
          okBatches += 1;
          logInfo("ai_text batch completed", {
            step: step.id,
            batch_index: idx,
            batch_rows: batch.length,
            attempts: usedAttempt,
          });
        } catch (e) {
          failedBatches += 1;
          await writeJson(path.resolve(batchDir, "result.json"), {
            status: "error",
            batch_index: idx,
            batch_rows: batch.length,
            error: e instanceof Error ? e.message : String(e),
          });
          await writeJson(
            path.resolve(batchDir, "review.joined.json"),
            buildBatchReviewRows(batch, ctx.idField, new Map<string, AiRowResult>(), e instanceof Error ? e.message : String(e)),
          );
          logInfo("ai_text batch failed", {
            step: step.id,
            batch_index: idx,
            error: e instanceof Error ? e.message : String(e),
          });
          await appendJsonl(errJsonl, {
            batch_index: idx,
            error: e instanceof Error ? e.message : String(e),
            ids: batch.map((r) => safeGet(r, ctx.idField)),
          });
        }
      }),
    ),
  );

  // Attach results back
  const headers = [...input.headers];
  const ensureHeader = (h: string): void => {
    if (!headers.includes(h)) headers.push(h);
  };
  ensureHeader(step.task.decision_field);
  ensureHeader(step.task.confidence_field);
  if (step.task.reason_field) ensureHeader(step.task.reason_field);
  if (step.task.evidence_field) ensureHeader(step.task.evidence_field);

  const outRows: Row[] = input.rows.map((row) => {
    const id = safeGet(row, ctx.idField);
    const res = idToResult.get(id);
    const out: Row = { ...row };
    out[step.task.decision_field] = res?.decision ?? "not_sure";
    out[step.task.confidence_field] = res?.confidence ?? "not_sure";
    if (step.task.reason_field) out[step.task.reason_field] = JSON.stringify(res?.reason_codes ?? []);
    if (step.task.evidence_field) out[step.task.evidence_field] = JSON.stringify(res?.evidence_snippets ?? []);
    return out;
  });

  const finalizedIds = new Set<string>();
  for (const r of outRows) {
    if (shouldFinalize(step, r)) finalizedIds.add(safeGet(r, ctx.idField));
  }

  const outPath = path.resolve(stepDir, "out.csv");
  await writeText(outPath, toCsv(headers, outRows));
  await writeText(path.resolve(stepDir, "out.ids.txt"), outRows.map((r) => safeGet(r, ctx.idField)).join("\n") + "\n");
  await writeJson(path.resolve(stepDir, "stats.json"), {
    step: step.id,
    in_count: input.rows.length,
    batches: batches.length,
    ok_batches: okBatches,
    failed_batches: failedBatches,
    resumed_batches: resumedBatches,
    finalized_count: finalizedIds.size,
    batch_artifacts_dir: batchesDir,
  });
  logInfo("ai_text step completed", {
    step: step.id,
    in_count: input.rows.length,
    ok_batches: okBatches,
    failed_batches: failedBatches,
    resumed_batches: resumedBatches,
    finalized_count: finalizedIds.size,
  });

  return { outPath, finalizedIds };
}

function createLimiter(concurrency: number): <T>(fn: () => Promise<T>) => Promise<T> {
  let active = 0;
  const queue: Array<() => void> = [];

  const next = (): void => {
    if (queue.length === 0) return;
    if (active >= concurrency) return;
    active += 1;
    const run = queue.shift();
    run?.();
  };

  return async <T>(fn: () => Promise<T>): Promise<T> => {
    return await new Promise<T>((resolve, reject) => {
      const task = (): void => {
        fn()
          .then(resolve)
          .catch(reject)
          .finally(() => {
            active -= 1;
            next();
          });
      };
      queue.push(task);
      next();
    });
  };
}

// ---------- Web scrape + AI (A3) ----------

function htmlToText(html: string): string {
  // Very lightweight tag stripper. Good enough for MVP.
  const noScript = html.replace(/<script[\s\S]*?<\/script>/gi, " ").replace(/<style[\s\S]*?<\/style>/gi, " ");
  const text = noScript
    .replace(/<br\s*\/?\s*>/gi, "\n")
    .replace(/<\/?p\b[^>]*>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\s+/g, " ")
    .trim();
  return text;
}

async function fetchWithTimeout(url: string, timeoutMs: number, userAgent?: string): Promise<string> {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeoutMs);
  try {
    const resp = await fetch(url, {
      method: "GET",
      headers: userAgent ? { "User-Agent": userAgent } : undefined,
      signal: ac.signal,
    });
    const html = await resp.text();
    return html;
  } finally {
    clearTimeout(t);
  }
}

function buildDomainUrls(domain: string, pathsList: string[], maxPages: number): string[] {
  const urls: string[] = [];
  for (const p of pathsList) {
    const pp = p.startsWith("/") ? p : `/${p}`;
    urls.push(`https://${domain}${pp}`);
  }
  // de-dupe while preserving order
  const seen = new Set<string>();
  const out: string[] = [];
  for (const u of urls) {
    if (seen.has(u)) continue;
    seen.add(u);
    out.push(u);
    if (out.length >= maxPages) break;
  }
  return out;
}

type ScrapeBundle = {
  domain: string;
  pages: Array<{ url: string; text: string; status: "ok" | "error"; error?: string }>;
  combinedText: string;
};

async function scrapeDomain(stepDir: string, domain: string, cfg: WebAiStep["scrape"]): Promise<ScrapeBundle> {
  const maxPages = Math.max(1, cfg.max_pages_per_domain ?? 5);
  const pathsList = cfg.url_paths ?? ["/", "/about", "/company", "/licensing", "/licenses", "/disclosures", "/legal", "/terms"];
  const timeoutMs = Math.max(2000, cfg.timeout_ms ?? 12000);
  const ua = cfg.user_agent ?? "Mozilla/5.0 (compatible; analyze-companies/0.1; +https://example.invalid)";
  const maxCharsPerPage = Math.max(1000, cfg.max_chars_per_page ?? 12000);
  const maxTotal = Math.max(2000, cfg.max_total_chars ?? 40000);

  const domainDir = path.resolve(stepDir, "scrape", stableHash(domain));
  await ensureDir(domainDir);

  const urls = buildDomainUrls(domain, pathsList, maxPages);
  await writeJson(path.resolve(domainDir, "urls.json"), { domain, urls });

  const pages: ScrapeBundle["pages"] = [];
  let combined = "";

  for (let i = 0; i < urls.length; i += 1) {
    const url = urls[i];
    try {
      const html = await fetchWithTimeout(url, timeoutMs, ua);
      const text = htmlToText(html).slice(0, maxCharsPerPage);
      pages.push({ url, text, status: "ok" });
      await writeText(path.resolve(domainDir, `page-${i + 1}.txt`), text + "\n");

      if (combined.length < maxTotal && text.length > 0) {
        const remain = maxTotal - combined.length;
        combined += `\n\n[URL] ${url}\n${text.slice(0, remain)}`;
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      pages.push({ url, text: "", status: "error", error: msg });
      await writeText(path.resolve(domainDir, `page-${i + 1}.error.txt`), msg + "\n");
    }
  }

  return { domain, pages, combinedText: combined.trim() };
}

function buildWebBatchPrompt(step: WebAiStep, idField: string, rows: Row[], bundlesByDomain: Map<string, ScrapeBundle>): string {
  const { criteria_name, read_fields, instructions } = step.task;
  const label_set = taskLabelSet(step.task);
  const confidence_set = taskConfidenceSet(step.task);
  const items = rows.map((r) => {
    const domain = safeGet(r, "domain") || deriveDomain(safeGet(r, "website"));
    const bundle = bundlesByDomain.get(domain);
    const obj: Record<string, unknown> = {};
    for (const f of read_fields) obj[f] = safeGet(r, f);
    obj[idField] = safeGet(r, idField);
    obj.domain = domain;
    obj.website_text = bundle?.combinedText ?? "";
    return obj;
  });

  return [
    `You are classifying companies for criteria: ${criteria_name}.`,
    ...instructions.map((x) => `- ${x}`),
    "",
    `Allowed decision labels: ${label_set.join(", ")}`,
    `Allowed confidence labels: ${confidence_set.join(", ")}`,
    "",
    "Use the provided website_text to decide. Return ONLY a JSON array.",
    `Each element must be {"id":"<${idField}>","decision":"${label_set.join("|")}","confidence":"${confidence_set.join("|")}","reason_codes":["..."],"evidence_snippets":["..."],"evidence_urls":["..."]}`,
    "",
    "Items:",
    JSON.stringify(items),
  ].join("\n");
}

function scrapeLooksLowQuality(bundle: ScrapeBundle | undefined, minChars: number): boolean {
  if (!bundle) return true;
  const text = (bundle.combinedText ?? "").trim();
  if (text.length < minChars) return true;
  const lower = text.toLowerCase();
  const weakSignals = [
    "page not found",
    "not found",
    "attention required",
    "enable cookies",
    "access denied",
    "invalid ssl",
  ];
  const weakHit = weakSignals.some((s) => lower.includes(s));
  if (weakHit && text.length < minChars * 2) return true;
  return false;
}

function buildWebBrowseFallbackPrompt(step: WebAiStep, idField: string, rows: Row[]): string {
  const { criteria_name, read_fields, instructions } = step.task;
  const label_set = taskLabelSet(step.task);
  const confidence_set = taskConfidenceSet(step.task);
  const items = rows.map((r) => {
    const obj: Record<string, unknown> = {};
    for (const f of read_fields) obj[f] = safeGet(r, f);
    obj[idField] = safeGet(r, idField);
    obj.website = safeGet(r, "website") || safeGet(r, "Website");
    obj.domain = safeGet(r, "domain");
    return obj;
  });

  return [
    `You are classifying companies for criteria: ${criteria_name}.`,
    ...instructions.map((x) => `- ${x}`),
    "- Scraped website text was low quality or insufficient.",
    "- Use your browsing capability to inspect each website URL/domain and gather evidence.",
    "- If the site is unreachable or still ambiguous, return not_sure.",
    "",
    `Allowed decision labels: ${label_set.join(", ")}`,
    `Allowed confidence labels: ${confidence_set.join(", ")}`,
    "",
    "Return ONLY a JSON array.",
    `Each element must be {"id":"<${idField}>","decision":"${label_set.join("|")}","confidence":"${confidence_set.join("|")}","reason_codes":["..."],"evidence_snippets":["..."],"evidence_urls":["..."]}`,
    "",
    "Items:",
    JSON.stringify(items),
  ].join("\n");
}

type WebAiRowResult = {
  id: string;
  decision: string;
  confidence: string;
  reason_codes?: string[];
  evidence_snippets?: string[];
  evidence_urls?: string[];
};

function validateWebAiRowResult(step: WebAiStep, obj: any): WebAiRowResult {
  if (!obj || typeof obj !== "object") throw new Error("AI result is not an object");
  const id = String(obj.id ?? "");
  const decision = String(obj.decision ?? "");
  const confidence = String(obj.confidence ?? "");
  if (!id) throw new Error("Missing id");
  if (!taskLabelSet(step.task).includes(decision)) throw new Error(`Invalid decision: ${decision}`);
  if (!taskConfidenceSet(step.task).includes(confidence)) throw new Error(`Invalid confidence: ${confidence}`);
  const reason_codes = Array.isArray(obj.reason_codes) ? obj.reason_codes.map(String) : [];
  const evidence_snippets = Array.isArray(obj.evidence_snippets) ? obj.evidence_snippets.map(String) : [];
  const evidence_urls = Array.isArray(obj.evidence_urls) ? obj.evidence_urls.map(String) : [];
  return { id, decision, confidence, reason_codes, evidence_snippets, evidence_urls };
}

async function runWebAiStep(ctx: RunContext, step: WebAiStep): Promise<string> {
  logInfo("web_ai step started", { step: step.id });
  const stepDir = path.resolve(ctx.runDir, "steps", step.id);
  const batchesDir = path.resolve(stepDir, "batches");
  await ensureDir(stepDir);
  await ensureDir(batchesDir);
  await writeJson(path.resolve(stepDir, "config.json"), step);

  const input = await loadStepInput(ctx, step);
  logInfo("web_ai input loaded", { step: step.id, rows: input.rows.length });
  await writeText(path.resolve(stepDir, "in.csv"), toCsv(input.headers, input.rows));

  const scrapeCfg = step.scrape ?? {};
  const doScrape = scrapeCfg.enabled ?? true;
  const scrapeConcurrency = Math.max(1, scrapeCfg.concurrency ?? 8);
  const scrapeLimit = createLimiter(scrapeConcurrency);

  // Domain bundles (with caching)
  const domains = new Set<string>();
  for (const r of input.rows) {
    const d = safeGet(r, "domain") || deriveDomain(safeGet(r, "website"));
    if (d) domains.add(d);
  }
  logInfo("web_ai domains identified", { step: step.id, domains: domains.size });
  const bundlesByDomain = new Map<string, ScrapeBundle>();

  if (doScrape) {
    const domainList = Array.from(domains);
    let okDomains = 0;
    let errDomains = 0;
    await Promise.all(
      domainList.map((d) =>
        scrapeLimit(async () => {
          try {
            const bundle = await scrapeDomain(stepDir, d, scrapeCfg);
            bundlesByDomain.set(d, bundle);
            okDomains += 1;
          } catch {
            errDomains += 1;
          }
        }),
      ),
    );
    await writeJson(path.resolve(stepDir, "scrape.stats.json"), {
      domains_total: domainList.length,
      domains_ok: okDomains,
      domains_err: errDomains,
    });
    logInfo("web_ai scraping completed", {
      step: step.id,
      domains_total: domainList.length,
      domains_ok: okDomains,
      domains_err: errDomains,
    });
  }

  // AI classify using scraped bundle
  const serverUrl = ctx.serverOverride ?? step.ai.serverUrl ?? "http://127.0.0.1:3000";
  const client: OpenCodeClient = {
    serverUrl,
    model: parseModelSpec(step.ai.model),
    agent: step.ai.agent,
    sessionId: step.ai.continue_session_id ?? (await createSession(serverUrl)),
  };
  await writeJson(path.resolve(stepDir, "session.json"), {
    serverUrl: client.serverUrl,
    model: client.model ? `${client.model.providerID}/${client.model.modelID}` : null,
    agent: client.agent ?? null,
    sessionId: client.sessionId,
  });

  const batchSize = Math.max(1, step.ai.batch_size ?? 10);
  const concurrency = Math.max(1, step.ai.concurrency ?? 2);
  const maxAttempts = Math.max(1, step.ai.max_attempts ?? 3);
  const retryDelayMs = Math.max(0, step.ai.retry_delay_ms ?? 400);
  const limit = createLimiter(concurrency);

  const idToResult = new Map<string, WebAiRowResult>();
  const outJsonl = path.resolve(stepDir, "out.jsonl");
  const errJsonl = path.resolve(stepDir, "errors.jsonl");

  const batches: Array<Row[]> = [];
  for (let i = 0; i < input.rows.length; i += batchSize) {
    batches.push(input.rows.slice(i, i + batchSize));
  }
  logInfo("web_ai batching prepared", {
    step: step.id,
    batch_size: batchSize,
    batches: batches.length,
    concurrency,
    max_attempts: maxAttempts,
    retry_delay_ms: retryDelayMs,
    server: serverUrl,
  });

  let okBatches = 0;
  let failedBatches = 0;
  await Promise.all(
    batches.map((batch, idx) =>
      limit(async () => {
        const batchNumber = String(idx + 1).padStart(4, "0");
        const batchDir = path.resolve(batchesDir, `batch-${batchNumber}`);
        await ensureDir(batchDir);
        await writeText(path.resolve(batchDir, "in.csv"), toCsv(input.headers, batch));

        const prompt = buildWebBatchPrompt(step, ctx.idField, batch, bundlesByDomain);
        await writeText(path.resolve(batchDir, "prompt.txt"), prompt);
        try {
          let arr: unknown[] | null = null;
          let finalReply: { text: string; response: MessageResponse } | null = null;
          let lastError: unknown = null;
          let usedAttempt = 0;

          for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
            usedAttempt = attempt;
            const reply = await sendMessage(client, prompt);
            finalReply = reply;
            await writeText(path.resolve(batchDir, `attempt-${String(attempt).padStart(2, "0")}.raw.txt`), reply.text);
            await writeJson(path.resolve(batchDir, `attempt-${String(attempt).padStart(2, "0")}.response.json`), reply.response);
            try {
              const parsed = extractJsonArray(reply.text, ["id", "decision", "confidence"]);
              if (!Array.isArray(parsed)) throw new Error("Expected JSON array");
              arr = parsed;
              break;
            } catch (e) {
              lastError = e;
              if (attempt < maxAttempts) await sleepMs(retryDelayMs * attempt);
            }
          }

          if (!arr || !finalReply) {
            throw lastError instanceof Error
              ? new Error(`${lastError.message} (attempts=${maxAttempts})`)
              : new Error(`Failed to parse JSON array after ${maxAttempts} attempts`);
          }

          await appendJsonl(outJsonl, { batch_index: idx, raw: finalReply.text, attempts: usedAttempt });
          await writeText(path.resolve(batchDir, "response.raw.txt"), finalReply.text);
          await writeJson(path.resolve(batchDir, "response.message.json"), finalReply.response);
          await writeJson(path.resolve(batchDir, "response.parsed.json"), arr);
          const batchById = new Map<string, WebAiRowResult>();
          for (const it of arr) {
            const r = validateWebAiRowResult(step, it);
            idToResult.set(r.id, r);
            batchById.set(r.id, r);
          }
          await writeJson(
            path.resolve(batchDir, "review.joined.json"),
            buildWebBatchReviewRows(batch, ctx.idField, batchById),
          );
          await writeJson(path.resolve(batchDir, "result.json"), {
            status: "ok",
            batch_index: idx,
            batch_rows: batch.length,
            result_count: arr.length,
            attempts: usedAttempt,
          });
          okBatches += 1;
          logInfo("web_ai batch completed", { step: step.id, batch_index: idx, batch_rows: batch.length });
        } catch (e) {
          failedBatches += 1;
          await writeJson(path.resolve(batchDir, "result.json"), {
            status: "error",
            batch_index: idx,
            batch_rows: batch.length,
            error: e instanceof Error ? e.message : String(e),
          });
          await writeJson(
            path.resolve(batchDir, "review.joined.json"),
            buildWebBatchReviewRows(batch, ctx.idField, new Map<string, WebAiRowResult>(), e instanceof Error ? e.message : String(e)),
          );
          logInfo("web_ai batch failed", {
            step: step.id,
            batch_index: idx,
            error: e instanceof Error ? e.message : String(e),
          });
          await appendJsonl(errJsonl, {
            batch_index: idx,
            error: e instanceof Error ? e.message : String(e),
            ids: batch.map((r) => safeGet(r, ctx.idField)),
          });
        }
      }),
    ),
  );

  // Optional fallback: for thin scrape text, let a browsing-capable agent inspect URLs directly.
  const fallbackEnabled = step.ai.browse_fallback_enabled ?? true;
  const fallbackMinChars = Math.max(0, step.ai.browse_fallback_min_chars ?? 350);
  const fallbackRows = fallbackEnabled
    ? input.rows.filter((row) => {
      const domain = safeGet(row, "domain") || deriveDomain(safeGet(row, "website"));
      const bundle = bundlesByDomain.get(domain);
      return scrapeLooksLowQuality(bundle, fallbackMinChars);
    })
    : [];

  let fallbackOkBatches = 0;
  let fallbackFailedBatches = 0;

  if (fallbackRows.length > 0) {
    const fallbackDir = path.resolve(stepDir, "browse-fallback");
    await ensureDir(fallbackDir);
    const fallbackClientBase: Omit<OpenCodeClient, "sessionId"> = {
      serverUrl,
      model: parseModelSpec(step.ai.browse_fallback_model) ?? client.model,
      agent: step.ai.browse_fallback_agent ?? client.agent,
    };
    await writeJson(path.resolve(fallbackDir, "session.mode.json"), {
      mode: "separate_session_per_batch_attempt",
      serverUrl: fallbackClientBase.serverUrl,
      model: fallbackClientBase.model ? `${fallbackClientBase.model.providerID}/${fallbackClientBase.model.modelID}` : null,
      agent: fallbackClientBase.agent ?? null,
    });

    const fallbackBatchSize = Math.max(1, step.ai.browse_fallback_batch_size ?? 8);
    const fallbackConcurrency = Math.max(1, step.ai.browse_fallback_concurrency ?? 1);
    const fallbackMaxAttempts = Math.max(1, step.ai.browse_fallback_max_attempts ?? maxAttempts);
    const fallbackRetryDelayMs = Math.max(0, step.ai.browse_fallback_retry_delay_ms ?? retryDelayMs);
    const fallbackLimit = createLimiter(fallbackConcurrency);
    const fallbackBatches: Array<Row[]> = [];
    for (let i = 0; i < fallbackRows.length; i += fallbackBatchSize) {
      fallbackBatches.push(fallbackRows.slice(i, i + fallbackBatchSize));
    }
    logInfo("web_ai browse fallback prepared", {
      step: step.id,
      rows: fallbackRows.length,
      batches: fallbackBatches.length,
      min_chars: fallbackMinChars,
      concurrency: fallbackConcurrency,
      max_attempts: fallbackMaxAttempts,
    });

    await Promise.all(
      fallbackBatches.map((batch, idx) =>
        fallbackLimit(async () => {
          const batchNumber = String(idx + 1).padStart(4, "0");
          const batchDir = path.resolve(fallbackDir, `batch-${batchNumber}`);
          await ensureDir(batchDir);
          await writeText(path.resolve(batchDir, "in.csv"), toCsv(input.headers, batch));

          const prompt = buildWebBrowseFallbackPrompt(step, ctx.idField, batch);
          await writeText(path.resolve(batchDir, "prompt.txt"), prompt);
          try {
            let arr: unknown[] | null = null;
            let finalReply: { text: string; response: MessageResponse } | null = null;
            let usedAttempt = 0;
            let lastError: unknown = null;

            for (let attempt = 1; attempt <= fallbackMaxAttempts; attempt += 1) {
              usedAttempt = attempt;
              const attemptSessionId = await createSession(serverUrl);
              const fallbackClient: OpenCodeClient = {
                ...fallbackClientBase,
                sessionId: attemptSessionId,
              };
              await writeJson(path.resolve(batchDir, `attempt-${String(attempt).padStart(2, "0")}.session.json`), {
                serverUrl: fallbackClient.serverUrl,
                model: fallbackClient.model ? `${fallbackClient.model.providerID}/${fallbackClient.model.modelID}` : null,
                agent: fallbackClient.agent ?? null,
                sessionId: fallbackClient.sessionId,
              });
              const reply = await sendMessage(fallbackClient, prompt);
              finalReply = reply;
              await writeText(path.resolve(batchDir, `attempt-${String(attempt).padStart(2, "0")}.raw.txt`), reply.text);
              await writeJson(path.resolve(batchDir, `attempt-${String(attempt).padStart(2, "0")}.response.json`), reply.response);
              try {
                const parsed = extractJsonArray(reply.text, ["id", "decision", "confidence"]);
                if (!Array.isArray(parsed)) throw new Error("Expected JSON array");
                arr = parsed;
                break;
              } catch (e) {
                lastError = e;
                if (attempt < fallbackMaxAttempts) await sleepMs(fallbackRetryDelayMs * attempt);
              }
            }

            if (!arr || !finalReply) {
              throw lastError instanceof Error
                ? new Error(`${lastError.message} (attempts=${fallbackMaxAttempts})`)
                : new Error(`Failed to parse JSON array after ${fallbackMaxAttempts} attempts`);
            }

            await writeText(path.resolve(batchDir, "response.raw.txt"), finalReply.text);
            await writeJson(path.resolve(batchDir, "response.message.json"), finalReply.response);
            await writeJson(path.resolve(batchDir, "response.parsed.json"), arr);
            const batchById = new Map<string, WebAiRowResult>();
            for (const it of arr) {
              const r = validateWebAiRowResult(step, it);
              idToResult.set(r.id, r); // fallback overwrites prior scraped-only result
              batchById.set(r.id, r);
            }
            await writeJson(
              path.resolve(batchDir, "review.joined.json"),
              buildWebBatchReviewRows(batch, ctx.idField, batchById),
            );
            await writeJson(path.resolve(batchDir, "result.json"), {
              status: "ok",
              batch_index: idx,
              batch_rows: batch.length,
              result_count: arr.length,
              attempts: usedAttempt,
            });
            fallbackOkBatches += 1;
            logInfo("web_ai browse fallback batch completed", {
              step: step.id,
              batch_index: idx,
              batch_rows: batch.length,
            });
          } catch (e) {
            fallbackFailedBatches += 1;
            await writeJson(path.resolve(batchDir, "result.json"), {
              status: "error",
              batch_index: idx,
              batch_rows: batch.length,
              error: e instanceof Error ? e.message : String(e),
            });
            await writeJson(
              path.resolve(batchDir, "review.joined.json"),
              buildWebBatchReviewRows(batch, ctx.idField, new Map<string, WebAiRowResult>(), e instanceof Error ? e.message : String(e)),
            );
            logInfo("web_ai browse fallback batch failed", {
              step: step.id,
              batch_index: idx,
              error: e instanceof Error ? e.message : String(e),
            });
          }
        }),
      ),
    );
  }

  // Attach results
  const headers = [...input.headers];
  const ensureHeader = (h: string): void => {
    if (!headers.includes(h)) headers.push(h);
  };
  ensureHeader(step.task.decision_field);
  ensureHeader(step.task.confidence_field);
  if (step.task.reason_field) ensureHeader(step.task.reason_field);
  if (step.task.evidence_field) ensureHeader(step.task.evidence_field);
  // Optional: store evidence urls in a separate column if not provided
  const evidenceUrlsField = step.task.evidence_field ? `${step.task.evidence_field}-urls` : "Evidence-urls";
  ensureHeader(evidenceUrlsField);

  const outRows: Row[] = input.rows.map((row) => {
    const id = safeGet(row, ctx.idField);
    const res = idToResult.get(id);
    const out: Row = { ...row };
    out[step.task.decision_field] = res?.decision ?? "not_sure";
    out[step.task.confidence_field] = res?.confidence ?? "not_sure";
    if (step.task.reason_field) out[step.task.reason_field] = JSON.stringify(res?.reason_codes ?? []);
    if (step.task.evidence_field) out[step.task.evidence_field] = JSON.stringify(res?.evidence_snippets ?? []);
    out[evidenceUrlsField] = JSON.stringify(res?.evidence_urls ?? []);
    return out;
  });

  const outPath = path.resolve(stepDir, "out.csv");
  await writeText(outPath, toCsv(headers, outRows));
  await writeText(path.resolve(stepDir, "out.ids.txt"), outRows.map((r) => safeGet(r, ctx.idField)).join("\n") + "\n");
  await writeJson(path.resolve(stepDir, "stats.json"), {
    step: step.id,
    in_count: input.rows.length,
    batches: batches.length,
    ok_batches: okBatches,
    failed_batches: failedBatches,
    browse_fallback_enabled: fallbackEnabled,
    browse_fallback_min_chars: fallbackMinChars,
    browse_fallback_rows: fallbackRows.length,
    browse_fallback_ok_batches: fallbackOkBatches,
    browse_fallback_failed_batches: fallbackFailedBatches,
    domains: Array.from(domains).length,
    batch_artifacts_dir: batchesDir,
  });
  logInfo("web_ai step completed", {
    step: step.id,
    in_count: input.rows.length,
    ok_batches: okBatches,
    failed_batches: failedBatches,
    browse_fallback_rows: fallbackRows.length,
    browse_fallback_ok_batches: fallbackOkBatches,
    browse_fallback_failed_batches: fallbackFailedBatches,
    domains: Array.from(domains).length,
  });

  return outPath;
}

// ---------- Apollo people enrichment (A4) ----------
async function runApolloPeopleStep(ctx: RunContext, cfg: RunConfig, step: ApolloPeopleStep): Promise<string> {
  logInfo("apollo_people step started", { step: step.id });
  const stepDir = path.resolve(ctx.runDir, "steps", step.id);
  await ensureDir(stepDir);
  await writeJson(path.resolve(stepDir, "config.json"), step);

  const fromFinalCsv = step.from_final?.output_csv ?? cfg.finalize?.output_csv ?? "A3.csv";
  const finalPath = path.resolve(ctx.runDir, "final", fromFinalCsv);
  if (!(await pathExists(finalPath))) {
    throw new Error(`Final output CSV not found for apollo_people step: ${finalPath}`);
  }
  const finalData = await readCsvFile(finalPath);
  await writeText(path.resolve(stepDir, "in.csv"), toCsv(finalData.headers, finalData.rows));

  const decisionField = step.from_final?.decision_field ?? "Decision-Final";
  const decisionEquals = step.from_final?.decision_equals ?? "yes";
  const confidenceField = step.from_final?.confidence_field ?? "Confidence-Final";
  const confidenceEquals = step.from_final?.confidence_equals ?? "high";
  const configuredIdField = step.company?.id_field ?? cfg.run?.id_field ?? "id";
  const configuredNameField = step.company?.name_field ?? "name";
  const configuredDomainField = step.company?.domain_field ?? "domain";
  const idFieldCandidates = Array.from(new Set([
    configuredIdField,
    "apollo_account_id",
    "apollo_id",
    "organization_id",
    "id",
    "__row_id",
  ]));
  const idField =
    idFieldCandidates.find((field) => finalData.rows.some((row) => safeGet(row, field).trim().length > 0))
    ?? configuredIdField;
  const nameFieldCandidates = Array.from(new Set([
    configuredNameField,
    "name",
    "company_name",
    "Company Name",
  ]));
  const nameField =
    nameFieldCandidates.find((field) => finalData.rows.some((row) => safeGet(row, field).trim().length > 0))
    ?? configuredNameField;
  const domainFieldCandidates = Array.from(new Set([
    configuredDomainField,
    "domain",
    "website",
    "Website",
    "company_domain",
  ]));
  const domainField =
    domainFieldCandidates.find((field) => finalData.rows.some((row) => safeGet(row, field).trim().length > 0))
    ?? configuredDomainField;
  logInfo("apollo_people id field resolved", {
    step: step.id,
    configured_id_field: configuredIdField,
    resolved_id_field: idField,
    configured_name_field: configuredNameField,
    resolved_name_field: nameField,
    configured_domain_field: configuredDomainField,
    resolved_domain_field: domainField,
  });

  const candidateRows = finalData.rows.filter(
    (r) => safeGet(r, decisionField) === decisionEquals && safeGet(r, confidenceField) === confidenceEquals,
  );
  const byCompanyId = new Map<string, HighYesCompany>();
  for (const row of candidateRows) {
    const companyId = safeGet(row, idField).trim();
    if (!companyId) continue;
    if (byCompanyId.has(companyId)) continue;
    const companyName = safeGet(row, nameField).trim();
    const rawDomain = safeGet(row, domainField).trim();
    const companyDomain = (domainField.toLowerCase().includes("website") ? deriveDomain(rawDomain) : rawDomain).toLowerCase();
    byCompanyId.set(companyId, {
      company_id: companyId,
      company_name: companyName,
      company_domain: companyDomain,
      normalized_company_name: normalizeCompanyName(companyName),
    });
  }
  const companies = Array.from(byCompanyId.values());

  const byNormalizedName = new Map<string, HighYesCompany[]>();
  for (const company of companies) {
    const key = company.normalized_company_name;
    const arr = byNormalizedName.get(key) ?? [];
    arr.push(company);
    byNormalizedName.set(key, arr);
  }
  const uniqueNameCompanies = companies.filter((c) => (byNormalizedName.get(c.normalized_company_name)?.length ?? 0) === 1);
  const duplicateNameCompanies = companies.filter((c) => (byNormalizedName.get(c.normalized_company_name)?.length ?? 0) > 1);
  await writeJson(path.resolve(stepDir, "companies.filtered.high-yes-high.json"), companies);
  await writeJson(path.resolve(stepDir, "companies.listA.unique-name.json"), uniqueNameCompanies);
  await writeJson(path.resolve(stepDir, "companies.listB.duplicate-name.json"), duplicateNameCompanies);

  const perPage = Math.max(1, Math.min(100, step.people.per_page ?? 25));
  const maxPages = Math.max(1, Math.min(500, step.people.max_pages ?? 1));
  const peopleLimitPerCompany = Math.max(1, step.people.people_limit_per_company ?? 50);
  const maxCompanyIdsPerRequest = Math.max(1, Math.min(50, step.people.max_company_ids_per_request ?? 50));
  const requestDelayMs = Math.max(0, step.rate_limit?.request_delay_ms ?? 350);
  const duplicateDelayMs = Math.max(0, step.rate_limit?.duplicate_company_delay_ms ?? 1200);
  const maxAttempts = Math.max(1, step.rate_limit?.max_attempts ?? 4);
  const retryBaseDelayMs = Math.max(100, step.rate_limit?.retry_base_delay_ms ?? 800);

  const titles = step.people.target_roles_or_titles
    .map((v) => v.trim())
    .filter((v) => v.length > 0);
  if (titles.length === 0) {
    throw new Error(`apollo_people step ${step.id} requires non-empty people.target_roles_or_titles`);
  }

  const personLocations = (step.people.person_locations ?? []).map((v) => v.trim()).filter((v) => v.length > 0);
  const keywords = (step.people.q_keywords ?? []).map((v) => v.trim()).filter((v) => v.length > 0);
  const baseQueryCommon: {
    per_page: number;
    person_titles?: string[];
    person_seniorities: PeopleQueryV1Normalized["person_seniorities"];
    person_locations?: string[];
    q_keywords?: string[];
  } = {
    per_page: perPage,
    person_titles: titles,
    person_seniorities: mapSeniorityMinToApollo(step.people.seniority_min),
  };
  if (personLocations.length > 0) baseQueryCommon.person_locations = personLocations;
  if (keywords.length > 0) baseQueryCommon.q_keywords = keywords;

  const companiesById = new Map(companies.map((c) => [c.company_id, c]));
  const uniqueNameToCompany = new Map<string, HighYesCompany>();
  for (const company of uniqueNameCompanies) {
    uniqueNameToCompany.set(company.normalized_company_name, company);
  }
  const companyByDomain = new Map<string, HighYesCompany>();
  for (const company of companies) {
    if (company.company_domain) companyByDomain.set(company.company_domain, company);
  }

  const leads: LeadRow[] = [];
  const unassignedPeople: ApolloPersonLite[] = [];

  const upsertLead = (company: HighYesCompany, person: ApolloPersonLite, sourceList: LeadRow["source_list"], requestKey: string): void => {
    leads.push({
      company_id: company.company_id,
      company_name: company.company_name,
      company_domain: company.company_domain,
      normalized_company_name: company.normalized_company_name,
      source_list: sourceList,
      request_key: requestKey,
      apollo_person_id: person.apollo_person_id,
      full_name: person.full_name,
      title: person.title,
      email: person.email,
      linkedin_url: person.linkedin_url,
      location: person.location,
      apollo_organization_id: person.organization_id,
      apollo_organization_name: person.organization_name,
      apollo_organization_domain: person.organization_domain,
    });
  };

  // List A: unique company names, batched by organization_ids.
  const listAChunks = chunkArray(uniqueNameCompanies, maxCompanyIdsPerRequest);
  let listARequests = 0;
  let listBRequests = 0;
  for (let chunkIndex = 0; chunkIndex < listAChunks.length; chunkIndex += 1) {
    const chunkCompanies = listAChunks[chunkIndex];
    const chunkCompanyIds = chunkCompanies.map((c) => c.company_id);
    const chunkDomains = chunkCompanies.map((c) => c.company_domain).filter((d) => d.length > 0);
    const requestKey = `listA-${String(chunkIndex + 1).padStart(4, "0")}`;
    const batchDir = path.resolve(stepDir, "listA-batches", requestKey);
    await ensureDir(batchDir);
    await writeJson(path.resolve(batchDir, "companies.json"), chunkCompanies);

    const seenKeys = new Set<string>();
    for (let page = 1; page <= maxPages; page += 1) {
      const query = assertValidPeopleQueryV1({
        ...baseQueryCommon,
        page,
        organization_ids: chunkCompanyIds,
      });
      await writeJson(path.resolve(batchDir, `request.page-${String(page).padStart(3, "0")}.json`), query);
      let response = await callPeopleSearchWithRetry(query, maxAttempts, retryBaseDelayMs);
      let queryMode = "strict";
      const strictPeople = Array.isArray(response.people) ? response.people : [];
      if (strictPeople.length === 0 && (baseQueryCommon.person_titles?.length ?? 0) > 0) {
        const relaxedInput: Record<string, unknown> = { ...baseQueryCommon, page, organization_ids: chunkCompanyIds };
        delete relaxedInput.person_titles;
        const relaxedQuery = assertValidPeopleQueryV1(relaxedInput);
        await writeJson(path.resolve(batchDir, `request.page-${String(page).padStart(3, "0")}.fallback-no-titles.json`), relaxedQuery);
        response = await callPeopleSearchWithRetry(relaxedQuery, maxAttempts, retryBaseDelayMs);
        queryMode = "fallback_no_titles";
        const relaxedPeople = Array.isArray(response.people) ? response.people : [];
        if (relaxedPeople.length === 0) {
          const minimalInput: Record<string, unknown> = {
            page,
            per_page: perPage,
            organization_ids: chunkCompanyIds,
          };
          const minimalQuery = assertValidPeopleQueryV1(minimalInput);
          await writeJson(path.resolve(batchDir, `request.page-${String(page).padStart(3, "0")}.fallback-minimal.json`), minimalQuery);
          response = await callPeopleSearchWithRetry(minimalQuery, maxAttempts, retryBaseDelayMs);
          queryMode = "fallback_minimal";
          const minimalPeople = Array.isArray(response.people) ? response.people : [];
          if (minimalPeople.length === 0 && chunkDomains.length > 0) {
            const domainOnlyQuery = assertValidPeopleQueryV1({
              page,
              per_page: perPage,
              q_organization_domains_list: chunkDomains,
            });
            await writeJson(path.resolve(batchDir, `request.page-${String(page).padStart(3, "0")}.fallback-domain-only.json`), domainOnlyQuery);
            response = await callPeopleSearchWithRetry(domainOnlyQuery, maxAttempts, retryBaseDelayMs);
            queryMode = "fallback_domain_only";
          }
        }
      }
      listARequests += 1;
      await writeJson(path.resolve(batchDir, `response.page-${String(page).padStart(3, "0")}.json`), { query_mode: queryMode, ...response });

      const peopleRaw = Array.isArray(response.people) ? response.people : [];
      if (peopleRaw.length === 0) break;
      for (const personRaw of peopleRaw) {
        if (!personRaw || typeof personRaw !== "object") continue;
        const person = normalizeApolloPerson(personRaw as Record<string, unknown>);
        const personKey = person.apollo_person_id || person.email || person.linkedin_url || `${person.full_name}|${person.organization_id}`;
        if (!personKey) continue;
        const dedupeKey = `${requestKey}|${personKey}`;
        if (seenKeys.has(dedupeKey)) continue;
        seenKeys.add(dedupeKey);

        const byOrgId = person.organization_id ? companiesById.get(person.organization_id) : undefined;
        const byOrgName = uniqueNameToCompany.get(normalizeCompanyName(person.organization_name));
        const byOrgDomain = person.organization_domain ? companyByDomain.get(person.organization_domain.toLowerCase()) : undefined;
        const matchedCompany = byOrgId ?? byOrgName ?? byOrgDomain;
        if (!matchedCompany) {
          unassignedPeople.push(person);
          continue;
        }
        upsertLead(matchedCompany, person, "A_unique_name_batch", requestKey);
      }

      const totalPages = response.pagination?.total_pages;
      if (typeof totalPages === "number" && page >= totalPages) break;
      if (peopleRaw.length < query.per_page) break;
      await sleepMs(requestDelayMs);
    }
  }

  // List B: duplicate company names, request one company id at a time.
  for (let idx = 0; idx < duplicateNameCompanies.length; idx += 1) {
    const company = duplicateNameCompanies[idx];
    const requestKey = `listB-${String(idx + 1).padStart(4, "0")}-${company.company_id}`;
    const companyDir = path.resolve(stepDir, "listB-single", requestKey);
    await ensureDir(companyDir);
    await writeJson(path.resolve(companyDir, "company.json"), company);

    const seenKeys = new Set<string>();
    for (let page = 1; page <= maxPages; page += 1) {
      const query = assertValidPeopleQueryV1({
        ...baseQueryCommon,
        page,
        organization_ids: [company.company_id],
      });
      await writeJson(path.resolve(companyDir, `request.page-${String(page).padStart(3, "0")}.json`), query);
      let response = await callPeopleSearchWithRetry(query, maxAttempts, retryBaseDelayMs);
      let queryMode = "strict";
      const strictPeople = Array.isArray(response.people) ? response.people : [];
      if (strictPeople.length === 0 && (baseQueryCommon.person_titles?.length ?? 0) > 0) {
        const relaxedInput: Record<string, unknown> = { ...baseQueryCommon, page, organization_ids: [company.company_id] };
        delete relaxedInput.person_titles;
        const relaxedQuery = assertValidPeopleQueryV1(relaxedInput);
        await writeJson(path.resolve(companyDir, `request.page-${String(page).padStart(3, "0")}.fallback-no-titles.json`), relaxedQuery);
        response = await callPeopleSearchWithRetry(relaxedQuery, maxAttempts, retryBaseDelayMs);
        queryMode = "fallback_no_titles";
        const relaxedPeople = Array.isArray(response.people) ? response.people : [];
        if (relaxedPeople.length === 0) {
          const minimalInput: Record<string, unknown> = {
            page,
            per_page: perPage,
            organization_ids: [company.company_id],
          };
          const minimalQuery = assertValidPeopleQueryV1(minimalInput);
          await writeJson(path.resolve(companyDir, `request.page-${String(page).padStart(3, "0")}.fallback-minimal.json`), minimalQuery);
          response = await callPeopleSearchWithRetry(minimalQuery, maxAttempts, retryBaseDelayMs);
          queryMode = "fallback_minimal";
          const minimalPeople = Array.isArray(response.people) ? response.people : [];
          if (minimalPeople.length === 0 && company.company_domain) {
            const domainOnlyQuery = assertValidPeopleQueryV1({
              page,
              per_page: perPage,
              q_organization_domains_list: [company.company_domain],
            });
            await writeJson(path.resolve(companyDir, `request.page-${String(page).padStart(3, "0")}.fallback-domain-only.json`), domainOnlyQuery);
            response = await callPeopleSearchWithRetry(domainOnlyQuery, maxAttempts, retryBaseDelayMs);
            queryMode = "fallback_domain_only";
          }
        }
      }
      listBRequests += 1;
      await writeJson(path.resolve(companyDir, `response.page-${String(page).padStart(3, "0")}.json`), { query_mode: queryMode, ...response });

      const peopleRaw = Array.isArray(response.people) ? response.people : [];
      if (peopleRaw.length === 0) break;
      for (const personRaw of peopleRaw) {
        if (!personRaw || typeof personRaw !== "object") continue;
        const person = normalizeApolloPerson(personRaw as Record<string, unknown>);
        const personKey = person.apollo_person_id || person.email || person.linkedin_url || `${person.full_name}|${person.organization_id}`;
        if (!personKey) continue;
        if (seenKeys.has(personKey)) continue;
        seenKeys.add(personKey);
        upsertLead(company, person, "B_duplicate_name_single", requestKey);
      }

      const totalPages = response.pagination?.total_pages;
      if (typeof totalPages === "number" && page >= totalPages) break;
      if (peopleRaw.length < query.per_page) break;
      await sleepMs(duplicateDelayMs);
    }
    await sleepMs(duplicateDelayMs);
  }

  // De-dupe same person/company tuple while preserving earliest record.
  const uniqueLeads = new Map<string, LeadRow>();
  for (const lead of leads) {
    const key =
      `${lead.company_id}|${lead.apollo_person_id || lead.email || lead.linkedin_url || lead.full_name}`;
    if (!uniqueLeads.has(key)) uniqueLeads.set(key, lead);
  }
  const leadRows = Array.from(uniqueLeads.values());

  const limitedByCompany = new Map<string, LeadRow[]>();
  for (const row of leadRows) {
    const arr = limitedByCompany.get(row.company_id) ?? [];
    if (arr.length < peopleLimitPerCompany) {
      arr.push(row);
      limitedByCompany.set(row.company_id, arr);
    }
  }
  const finalLeadRows = Array.from(limitedByCompany.values()).flat();
  const headers = [
    "company_id",
    "company_name",
    "company_domain",
    "normalized_company_name",
    "source_list",
    "request_key",
    "apollo_person_id",
    "full_name",
    "title",
    "email",
    "linkedin_url",
    "location",
    "apollo_organization_id",
    "apollo_organization_name",
    "apollo_organization_domain",
  ];
  await writeText(path.resolve(stepDir, "out.csv"), toCsv(headers, finalLeadRows as unknown as Row[]));
  await writeJson(path.resolve(stepDir, "out.json"), finalLeadRows);
  await writeJson(path.resolve(stepDir, "unassigned.people.sample.json"), unassignedPeople.slice(0, 200));

  const grouped = companies.map((company) => ({
    company_id: company.company_id,
    company_name: company.company_name,
    company_domain: company.company_domain,
    normalized_company_name: company.normalized_company_name,
    people: finalLeadRows.filter((lead) => lead.company_id === company.company_id),
  }));
  await writeJson(path.resolve(stepDir, "grouped.by-company.json"), grouped);
  await writeJson(path.resolve(stepDir, "apollo.rate-limit-notes.json"), {
    researched_at: new Date().toISOString(),
    references: [
      "https://docs.apollo.io/reference/get-rate-limits",
      "https://docs.apollo.io/reference/search-for-contacts",
    ],
    strategy: {
      request_delay_ms: requestDelayMs,
      duplicate_company_delay_ms: duplicateDelayMs,
      max_attempts: maxAttempts,
      retry_base_delay_ms: retryBaseDelayMs,
      max_company_ids_per_request: maxCompanyIdsPerRequest,
      note: "List B is always one company per request with explicit delay to avoid burst traffic.",
    },
  });
  await writeJson(path.resolve(stepDir, "stats.json"), {
    step: step.id,
    input_final_rows: finalData.rows.length,
    selected_high_yes_companies: companies.length,
    listA_unique_name_companies: uniqueNameCompanies.length,
    listB_duplicate_name_companies: duplicateNameCompanies.length,
    listA_requests: listARequests,
    listB_requests: listBRequests,
    leads_before_company_limit: leadRows.length,
    leads_after_company_limit: finalLeadRows.length,
    people_limit_per_company: peopleLimitPerCompany,
    unassigned_people_count: unassignedPeople.length,
  });
  logInfo("apollo_people step completed", {
    step: step.id,
    selected_high_yes_companies: companies.length,
    listA_unique_name_companies: uniqueNameCompanies.length,
    listB_duplicate_name_companies: duplicateNameCompanies.length,
    leads_after_company_limit: finalLeadRows.length,
  });
  return path.resolve(stepDir, "out.csv");
}

// ---------- Final merge ----------

async function finalizeRun(ctx: RunContext, cfg: RunConfig, a2Step: AiTextStep, a3Step?: WebAiStep): Promise<void> {
  logInfo("finalization started", {
    a2_step: a2Step.id,
    a3_step: a3Step?.id ?? null,
  });
  const finalDir = path.resolve(ctx.runDir, "final");
  await ensureDir(finalDir);

  // Load step outputs
  const a2Out = await readCsvFile(path.resolve(ctx.runDir, "steps", a2Step.id, "out.csv"));
  const a2ById = new Map<string, Row>();
  for (const r of a2Out.rows) a2ById.set(safeGet(r, ctx.idField), r);

  const a3ById = new Map<string, Row>();
  if (a3Step) {
    const a3OutPath = path.resolve(ctx.runDir, "steps", a3Step.id, "out.csv");
    try {
      const a3Out = await readCsvFile(a3OutPath);
      for (const r of a3Out.rows) a3ById.set(safeGet(r, ctx.idField), r);
    } catch {
      // ok: no a3
    }
  }

  const includeNotSure = cfg.finalize?.include_not_sure_in_a3 ?? true;
  const decision1 = a2Step.task.decision_field;
  const conf1 = a2Step.task.confidence_field;
  const decision2 = a3Step?.task.decision_field;
  const conf2 = a3Step?.task.confidence_field;

  const decisionFinalField = "Decision-Final";
  const confidenceFinalField = "Confidence-Final";
  const finalSourceField = "Final-Source";

  // Merge base headers from A2, then include A3 fields
  const headersSet = new Set<string>(a2Out.headers);
  headersSet.add(decisionFinalField);
  headersSet.add(confidenceFinalField);
  headersSet.add(finalSourceField);
  // Ensure both confidence columns exist in final output
  headersSet.add(conf1);
  if (conf2) headersSet.add(conf2);
  if (decision2) headersSet.add(decision2);

  const headers = Array.from(headersSet);

  const finalRows: Row[] = [];
  const rejectedRows: Row[] = [];

  for (const r of a2Out.rows) {
    const id = safeGet(r, ctx.idField);
    const d1 = safeGet(r, decision1);
    const c1 = safeGet(r, conf1);

    // Early finalize: yes + high => accept without web
    if (d1 === "yes" && c1 === "high") {
      const out: Row = { ...r };
      out[decisionFinalField] = "yes";
      out[confidenceFinalField] = "high";
      out[finalSourceField] = "text";
      if (decision2) out[decision2] = out[decision2] ?? "";
      if (conf2) out[conf2] = out[conf2] ?? "";
      finalRows.push(out);
      continue;
    }

    // If not sure => consult A3 if exists
    if (a3Step && d1 === "not_sure") {
      const r3 = a3ById.get(id);
      const out: Row = { ...r };
      if (r3) {
        // overlay A3 fields
        for (const [k, v] of Object.entries(r3)) {
          if (v !== undefined && v !== null && String(v).length > 0) out[k] = String(v);
        }
      }
      const dFinal = decision2 ? safeGet(out, decision2) : "not_sure";
      const cFinal = conf2 ? safeGet(out, conf2) : "not_sure";
      out[decisionFinalField] = dFinal || "not_sure";
      out[confidenceFinalField] = cFinal || "not_sure";
      out[finalSourceField] = "web";

      if (dFinal === "yes" || (includeNotSure && dFinal === "not_sure")) {
        finalRows.push(out);
      } else {
        rejectedRows.push(out);
      }
      continue;
    }

    // Otherwise Decision-1 governs (usually no)
    const out: Row = { ...r };
    out[decisionFinalField] = d1 || "not_sure";
    out[confidenceFinalField] = c1 || "not_sure";
    out[finalSourceField] = "text";

    if (d1 === "yes") {
      // you may keep yes but low/not_sure, or route to web in a future iteration
      finalRows.push(out);
    } else if (includeNotSure && d1 === "not_sure") {
      finalRows.push(out);
    } else {
      rejectedRows.push(out);
    }
  }

  const outputCsvName = cfg.finalize?.output_csv ?? "A3.csv";
  await writeText(path.resolve(finalDir, outputCsvName), toCsv(headers, finalRows));
  await writeText(path.resolve(finalDir, "A3-yes.csv"), toCsv(headers, finalRows.filter((r) => safeGet(r, decisionFinalField) === "yes")));
  await writeText(path.resolve(finalDir, "rejected.csv"), toCsv(headers, rejectedRows));

  // Optional views
  for (const view of cfg.finalize?.views ?? []) {
    const vrows = finalRows.filter((r) => safeGet(r, view.where.field) === view.where.equals);
    await writeText(path.resolve(finalDir, view.name), toCsv(headers, vrows));
  }

  await writeJson(path.resolve(finalDir, "stats.json"), {
    final_rows: finalRows.length,
    rejected_rows: rejectedRows.length,
  });
  logInfo("finalization completed", {
    final_rows: finalRows.length,
    rejected_rows: rejectedRows.length,
    output_dir: finalDir,
  });
}

async function buildForceFinalizeFromA2(
  ctx: RunContext,
  cfg: RunConfig,
  a2Step: AiTextStep,
): Promise<string> {
  const stepDir = path.resolve(ctx.runDir, "steps", a2Step.id);
  const a2OutPath = path.resolve(stepDir, "out.csv");
  const decision1 = a2Step.task.decision_field;
  const conf1 = a2Step.task.confidence_field;
  const decisionFinalField = "Decision-Final";
  const confidenceFinalField = "Confidence-Final";
  const finalSourceField = "Final-Source";

  let headers: string[] = [];
  let rows: Row[] = [];
  let source: "a2.out.csv" | "a2.batch-cache" = "a2.out.csv";

  if (await pathExists(a2OutPath)) {
    const a2Out = await readCsvFile(a2OutPath);
    headers = [...a2Out.headers];
    rows = a2Out.rows;
  } else {
    source = "a2.batch-cache";
    const a2InPath = path.resolve(stepDir, "in.csv");
    if (!(await pathExists(a2InPath))) {
      throw new Error(`Cannot force-finalize: missing ${a2OutPath} and ${a2InPath}`);
    }
    const a2In = await readCsvFile(a2InPath);
    headers = [...a2In.headers];
    const byId = new Map<string, Row>();
    for (const row of a2In.rows) byId.set(safeGet(row, ctx.idField), { ...row });

    const batchesDir = path.resolve(stepDir, "batches");
    if (!(await pathExists(batchesDir))) {
      throw new Error(`Cannot force-finalize: missing A2 batches directory: ${batchesDir}`);
    }

    const batchNames = (await fsp.readdir(batchesDir, { withFileTypes: true }))
      .filter((d) => d.isDirectory())
      .map((d) => d.name)
      .sort((a, b) => a.localeCompare(b));

    for (const batchName of batchNames) {
      const batchDir = path.resolve(batchesDir, batchName);
      const resultPath = path.resolve(batchDir, "result.json");
      const parsedPath = path.resolve(batchDir, "response.parsed.json");
      if (!(await pathExists(resultPath)) || !(await pathExists(parsedPath))) continue;
      const result = JSON.parse(await fsp.readFile(resultPath, "utf8")) as { status?: string };
      if (result.status !== "ok") continue;
      const parsed = JSON.parse(await fsp.readFile(parsedPath, "utf8"));
      if (!Array.isArray(parsed)) continue;
      for (const item of parsed) {
        const validated = validateAiRowResult(a2Step, item);
        const row = byId.get(validated.id);
        if (!row) continue;
        row[decision1] = validated.decision;
        row[conf1] = validated.confidence;
        if (a2Step.task.reason_field) row[a2Step.task.reason_field] = JSON.stringify(validated.reason_codes ?? []);
        if (a2Step.task.evidence_field) row[a2Step.task.evidence_field] = JSON.stringify(validated.evidence_snippets ?? []);
      }
    }
    rows = Array.from(byId.values());
  }

  const ensureHeader = (h: string): void => {
    if (!headers.includes(h)) headers.push(h);
  };
  ensureHeader(decision1);
  ensureHeader(conf1);
  ensureHeader(decisionFinalField);
  ensureHeader(confidenceFinalField);
  ensureHeader(finalSourceField);

  const yesHighRows = rows
    .filter((r) => safeGet(r, decision1) === "yes" && safeGet(r, conf1) === "high")
    .map((r) => ({
      ...r,
      [decisionFinalField]: "yes",
      [confidenceFinalField]: "high",
      [finalSourceField]: "text",
    }));

  const finalDir = path.resolve(ctx.runDir, "final");
  await ensureDir(finalDir);
  const outputCsvName = `${(cfg.finalize?.output_csv ?? "A3.csv").replace(/\.csv$/i, "")}.force-finalize.csv`;
  const outputPath = path.resolve(finalDir, outputCsvName);
  await writeText(outputPath, toCsv(headers, yesHighRows));
  await writeJson(path.resolve(finalDir, "force-finalize.stats.json"), {
    generated_at: new Date().toISOString(),
    source,
    output_csv: outputCsvName,
    decision_field: decision1,
    confidence_field: conf1,
    selected_yes_high_rows: yesHighRows.length,
  });
  logInfo("force-finalize completed", {
    run_dir: ctx.runDir,
    source,
    output_csv: outputCsvName,
    selected_yes_high_rows: yesHighRows.length,
  });
  return outputPath;
}

// ---------- Main ----------

async function loadConfig(configPath: string): Promise<RunConfig> {
  const raw = await fsp.readFile(configPath, "utf8");
  const cfg = JSON.parse(raw) as RunConfig;
  if (!cfg.steps || cfg.steps.length === 0) throw new Error("Config must include steps[]");
  for (const step of cfg.steps) {
    if (step.type === "ai_text" || step.type === "web_ai") {
      step.task.label_set = taskLabelSet(step.task);
      step.task.confidence_set = taskConfidenceSet(step.task);
    } else if (step.type === "apollo_people") {
      if (!Array.isArray(step.people?.target_roles_or_titles) || step.people.target_roles_or_titles.length === 0) {
        throw new Error(`Step ${step.id} (apollo_people) requires people.target_roles_or_titles`);
      }
    }
  }
  return cfg;
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv);
  logInfo("run started", {
    input: args.input,
    config: args.config,
    server: args.server ?? null,
    resume: args.resume ?? null,
    force_finalize: Boolean(args.forceFinalize),
  });
  if (args.forceFinalize && !args.resume) {
    throw new Error("--force-finalize requires --resume <run_dir>");
  }
  const cfg = await loadConfig(args.config);
  logInfo("config loaded", { steps: cfg.steps.length, run_name: cfg.run?.name ?? null });

  const cwd = process.cwd();
  const outputRoot = path.resolve(cwd, cfg.io?.output_root ?? "./output/analysis");
  const runDir = args.resume
    ? path.resolve(cwd, args.resume)
    : path.resolve(outputRoot, `${getBasenameNoExt(args.input)}-${utcTimestamp()}`);
  if (args.resume && !(await pathExists(runDir))) {
    throw new Error(`Resume run directory not found: ${runDir}`);
  }
  await ensureDir(runDir);
  await ensureDir(path.resolve(runDir, "input"));
  await ensureDir(path.resolve(runDir, "steps"));
  logInfo("run directories prepared", { run_dir: runDir });

  const ctx: RunContext = {
    cwd,
    runDir,
    outputRoot,
    idField: "__row_id",
    serverOverride: args.server ?? undefined,
    originalPath: path.resolve(runDir, "input", "original.csv"),
    normalizedPath: path.resolve(runDir, "input", "normalized.csv"),
    lastStepOutPath: path.resolve(runDir, "input", "normalized.csv"),
  };

  // Save run metadata
  await writeJson(path.resolve(runDir, "run.json"), {
    name: cfg.run?.name ?? null,
    started_at: new Date().toISOString(),
    resumed: Boolean(args.resume),
    input: path.resolve(cwd, args.input),
    config: path.resolve(cwd, args.config),
    server: args.server ?? null,
  });
  logInfo("run metadata written", { run_dir: runDir, resumed: Boolean(args.resume) });

  if (args.forceFinalize) {
    const a2Step = cfg.steps.find((s): s is AiTextStep => s.type === "ai_text");
    if (!a2Step) throw new Error("Cannot force-finalize: config has no ai_text step");
    const outputPath = await buildForceFinalizeFromA2(ctx, cfg, a2Step);
    process.stderr.write(`\nDone (force-finalize). Output: ${outputPath}\n`);
    return;
  }

  // Copy input
  if ((cfg.io?.copy_input_csv ?? true) && !(args.resume && await pathExists(ctx.originalPath))) {
    const raw = await fsp.readFile(args.input, "utf8");
    await writeText(ctx.originalPath, raw);
    logInfo("input copied", { destination: ctx.originalPath });
  }

  // Normalize
  if (args.resume && await pathExists(ctx.normalizedPath)) {
    const normalized = await readCsvFile(ctx.normalizedPath);
    ctx.idField = cfg.run?.id_field && normalized.headers.includes(cfg.run.id_field) ? cfg.run.id_field : "__row_id";
    ctx.lastStepOutPath = ctx.normalizedPath;
    logInfo("normalization reused from existing run", {
      id_field: ctx.idField,
      rows: normalized.rows.length,
      headers: normalized.headers.length,
    });
  } else {
    const csvRaw = await fsp.readFile(args.input, "utf8");
    const parsed = parseCsv(csvRaw);
    logInfo("input parsed", { headers: parsed.headers.length, rows: parsed.rows.length });
    const norm = await normalizeInput(cfg, parsed.headers, parsed.rows);
    ctx.idField = norm.idField;
    await writeText(ctx.normalizedPath, toCsv(norm.headers, norm.rows));
    ctx.lastStepOutPath = ctx.normalizedPath;
    logInfo("normalization completed", { id_field: ctx.idField, rows: norm.rows.length, headers: norm.headers.length });
  }

  // Execute steps
  let a2Step: AiTextStep | null = null;
  let a3Step: WebAiStep | null = null;
  const deferredApolloPeopleSteps: ApolloPeopleStep[] = [];

  for (const step of cfg.steps) {
    logInfo("executing step", { step: step.id, type: step.type });
    const stepOutPath = path.resolve(ctx.runDir, "steps", step.id, "out.csv");
    if (args.resume && await pathExists(stepOutPath)) {
      ctx.lastStepOutPath = stepOutPath;
      logInfo("step already complete; skipping", { step: step.id, out: stepOutPath });
      if (step.type === "ai_text") a2Step = step;
      if (step.type === "web_ai") a3Step = step;
      continue;
    }
    if (step.type === "filter") {
      ctx.lastStepOutPath = await runFilterStep(ctx, step);
    } else if (step.type === "ai_text") {
      a2Step = step;
      const res = await runAiTextStep(ctx, step);
      ctx.lastStepOutPath = res.outPath;
      await writeText(
        path.resolve(ctx.runDir, "steps", step.id, "finalized.ids.txt"),
        Array.from(res.finalizedIds).join("\n") + "\n",
      );
      logInfo("ai_text finalized ids written", { step: step.id, finalized_ids: res.finalizedIds.size });
    } else if (step.type === "web_ai") {
      a3Step = step;
      ctx.lastStepOutPath = await runWebAiStep(ctx, step);
    } else if (step.type === "apollo_people") {
      deferredApolloPeopleSteps.push(step);
      logInfo("apollo_people step deferred until finalization", { step: step.id });
    } else {
      throw new Error(`Unknown step type: ${(step as any).type}`);
    }
  }

  if (!a2Step) throw new Error("Config must include an ai_text step to produce Confidence-1");
  await finalizeRun(ctx, cfg, a2Step, a3Step ?? undefined);
  for (const step of deferredApolloPeopleSteps) {
    ctx.lastStepOutPath = await runApolloPeopleStep(ctx, cfg, step);
  }
  logInfo("run completed", { run_dir: ctx.runDir });

  process.stderr.write(`\nDone. Output: ${ctx.runDir}\n`);
}

if (require.main === module) {
  main().catch((err) => {
    process.stderr.write(`Fatal: ${err instanceof Error ? err.message : String(err)}\n`);
    process.exitCode = 1;
  });
}
