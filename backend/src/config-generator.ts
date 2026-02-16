import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { RunConfigSchema } from "./run-config";
import type { ZodError } from "zod";

type ProviderModel = {
  providerID: string;
  modelID: string;
};

type MessageResponse = {
  parts?: Array<Record<string, unknown>>;
};

export type ConfigGenerationJobStatus = "running" | "completed" | "failed";

export type ConfigGenerationJob = {
  id: string;
  status: ConfigGenerationJobStatus;
  percent: number;
  stage: string;
  attempt: number;
  maxAttempts: number;
  error: string | null;
  validationErrors: string[];
  configJson: string | null;
  updatedAt: string;
};

type ConfigGenerationInput = {
  prompt: string;
  csvHeaders: string[];
};

type CodeReferenceEntry = {
  key: string;
  label: string;
  keywords: string[];
  codes: string[];
};

type CodeReferenceCatalog = {
  entries: CodeReferenceEntry[];
};

type CodeSelection = {
  matchedNaicsLabels: string[];
  matchedSicLabels: string[];
  naicsCodes: string[];
  sicCodes: string[];
};

const DEFAULT_TIMEOUT_MS = 60000;
const DEFAULT_MODEL = "opencode/gpt-5-nano";
const NAICS_REFERENCE_CANDIDATE_PATHS = [
  path.resolve(process.cwd(), "data", "company_naics_codes.json"),
  path.resolve(process.cwd(), "..", "data", "company_naics_codes.json")
];
const SIC_REFERENCE_CANDIDATE_PATHS = [
  path.resolve(process.cwd(), "data", "company_sic_codes.json"),
  path.resolve(process.cwd(), "..", "data", "company_sic_codes.json")
];

function resolveFirstExistingPath(candidates: string[]): string {
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) return candidate;
  }
  return candidates[0] ?? "";
}

const NAICS_REFERENCE_PATH = resolveFirstExistingPath(NAICS_REFERENCE_CANDIDATE_PATHS);
const SIC_REFERENCE_PATH = resolveFirstExistingPath(SIC_REFERENCE_CANDIDATE_PATHS);

function nowIso(): string {
  return new Date().toISOString();
}

function normalize(value: unknown): string {
  return String(value ?? "").trim();
}

function toSnakeCase(value: string): string {
  return value
    .trim()
    .replace(/[^0-9A-Za-z]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();
}

function readCodeReferenceCatalog(filePath: string): CodeReferenceCatalog {
  try {
    const raw = fs.readFileSync(filePath, "utf8");
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return { entries: [] };
    }
    const entriesRaw = (parsed as Record<string, unknown>).entries;
    if (!Array.isArray(entriesRaw)) return { entries: [] };
    const entries = entriesRaw
      .filter((entry): entry is Record<string, unknown> => Boolean(entry && typeof entry === "object"))
      .map((entry) => ({
        key: normalize(entry.key),
        label: normalize(entry.label),
        keywords: Array.isArray(entry.keywords)
          ? entry.keywords.map((value) => normalize(value).toLowerCase()).filter((value) => value.length > 0)
          : [],
        codes: Array.isArray(entry.codes)
          ? entry.codes.map((value) => normalize(value)).filter((value) => value.length > 0)
          : []
      }))
      .filter((entry) => entry.key.length > 0 && entry.codes.length > 0);
    return { entries };
  } catch {
    return { entries: [] };
  }
}

function deriveCodesFromPrompt(prompt: string, naicsCatalog: CodeReferenceCatalog, sicCatalog: CodeReferenceCatalog): CodeSelection {
  const text = prompt.toLowerCase();
  const matchedNaicsLabels: string[] = [];
  const matchedSicLabels: string[] = [];
  const naicsCodes = new Set<string>();
  const sicCodes = new Set<string>();

  for (const entry of naicsCatalog.entries) {
    if (!entry.keywords.some((keyword) => text.includes(keyword))) continue;
    matchedNaicsLabels.push(entry.label || entry.key);
    for (const code of entry.codes) naicsCodes.add(code);
  }
  for (const entry of sicCatalog.entries) {
    if (!entry.keywords.some((keyword) => text.includes(keyword))) continue;
    matchedSicLabels.push(entry.label || entry.key);
    for (const code of entry.codes) sicCodes.add(code);
  }

  // Default fallback for mortgage-focused requests.
  if (naicsCodes.size === 0 && /(mortgage|lender|home loan|refinance|heloc)/i.test(prompt)) {
    naicsCodes.add("522292");
    naicsCodes.add("522310");
  }
  if (sicCodes.size === 0 && /(mortgage|lender|home loan|refinance|heloc)/i.test(prompt)) {
    sicCodes.add("6162");
    sicCodes.add("6163");
  }

  return {
    matchedNaicsLabels,
    matchedSicLabels,
    naicsCodes: Array.from(naicsCodes),
    sicCodes: Array.from(sicCodes)
  };
}

function parseProviderModel(model: string): ProviderModel {
  const trimmed = model.trim();
  const sep = trimmed.indexOf("/");
  if (sep <= 0 || sep === trimmed.length - 1) {
    throw new Error(`Invalid model "${model}". Expected provider/model format.`);
  }
  return {
    providerID: trimmed.slice(0, sep),
    modelID: trimmed.slice(sep + 1)
  };
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

function tryParseObject(raw: string): Record<string, unknown> | null {
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
    return parsed as Record<string, unknown>;
  } catch {
    return null;
  }
}

function extractJsonCandidates(text: string): Record<string, unknown>[] {
  const out: Record<string, unknown>[] = [];
  const seen = new Set<string>();
  const add = (obj: Record<string, unknown> | null): void => {
    if (!obj) return;
    const key = JSON.stringify(obj);
    if (seen.has(key)) return;
    seen.add(key);
    out.push(obj);
  };

  add(tryParseObject(text.trim()));

  const fenceRegex = /```(?:json)?\s*([\s\S]*?)```/gi;
  for (const match of text.matchAll(fenceRegex)) {
    add(tryParseObject((match[1] ?? "").trim()));
  }

  for (let start = 0; start < text.length; start += 1) {
    if (text[start] !== "{") continue;
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
      } else if (ch === "{") {
        depth += 1;
      } else if (ch === "}") {
        depth -= 1;
        if (depth === 0) {
          add(tryParseObject(text.slice(start, i + 1)));
          break;
        }
      }
    }
  }
  return out;
}

function unwrapCandidate(candidate: Record<string, unknown>): Record<string, unknown> {
  const wrapperKeys = ["config", "run_config", "json", "data"];
  for (const key of wrapperKeys) {
    const inner = candidate[key];
    if (inner && typeof inner === "object" && !Array.isArray(inner)) {
      return inner as Record<string, unknown>;
    }
  }
  return candidate;
}

function chooseReadFields(csvHeaders: string[]): string[] {
  const preferred = [
    "name",
    "industry",
    "keywords",
    "short_description",
    "naics_codes",
    "sic_codes",
    "website"
  ];
  const snakeToHeader = new Map<string, string>();
  for (const header of csvHeaders) {
    const snake = toSnakeCase(header);
    if (!snake) continue;
    if (!snakeToHeader.has(snake)) snakeToHeader.set(snake, header);
  }
  const out: string[] = [];
  for (const field of preferred) {
    const source = snakeToHeader.get(field);
    if (!source) continue;
    out.push(toSnakeCase(source));
  }
  if (out.length === 0) {
    return csvHeaders.slice(0, 6).map((header) => toSnakeCase(header)).filter((header) => header.length > 0);
  }
  return out;
}

function buildDefaultConfig(input: ConfigGenerationInput, selection: CodeSelection): Record<string, unknown> {
  const readFields = chooseReadFields(input.csvHeaders);
  const keepRules: Array<Record<string, unknown>> = [];
  if (selection.naicsCodes.length > 0) {
    keepRules.push({
      type: "code_in",
      field: "naics_codes",
      values: selection.naicsCodes
    });
  }
  if (selection.sicCodes.length > 0) {
    keepRules.push({
      type: "code_in",
      field: "sic_codes",
      values: selection.sicCodes
    });
  }
  if (keepRules.length === 0) {
    keepRules.push({
      type: "regex",
      field: "profile_text",
      pattern: "\\bmortgage\\b|\\bhome loan\\b|\\brefinance\\b|\\bheloc\\b",
      flags: "i"
    });
  }
  return {
    run: {
      name: "ai-generated-run",
      id_field: "id"
    },
    normalize: {
      trim_all_strings: true,
      derive: {
        domain_from: "website",
        profile_text_fields: readFields.slice(0, 4)
      }
    },
    steps: [
      {
        id: "01-filter-A1",
        type: "filter",
        input: { source: "normalized" },
        rules: {
          keep_if_any: keepRules,
          drop_if_any: [
            {
              type: "regex",
              field: "profile_text",
              pattern: "broker|servicer|property management|real estate agent|title insurance|escrow|appraisal|bank|credit union",
              flags: "i"
            }
          ]
        }
      },
      {
        id: "02-ai-text-A2",
        type: "ai_text",
        input: { source: "prev_step" },
        ai: {
          model: DEFAULT_MODEL,
          concurrency: 2,
          batch_size: 20
        },
        task: {
          criteria_name: "target_classification",
          read_fields: readFields.length > 0 ? readFields : ["name"],
          decision_field: "Decision-1",
          confidence_field: "Confidence-1",
          reason_field: "Reason-1",
          evidence_field: "Evidence-1",
          instructions: [
            input.prompt || "Classify whether the company matches the desired target profile.",
            "Return decision as one of: yes, no, not_sure.",
            "Return confidence as one of: high, not_sure, low."
          ]
        }
      }
    ],
    finalize: {
      output_csv: "A2.csv"
    }
  };
}

function getDefaultFilterStep(input: ConfigGenerationInput, selection: CodeSelection): Record<string, unknown> {
  const steps = (buildDefaultConfig(input, selection).steps ?? []) as Array<Record<string, unknown>>;
  const first = steps[0];
  return first && typeof first === "object"
    ? ({ ...first } as Record<string, unknown>)
    : {
        id: "01-filter-A1",
        type: "filter",
        input: { source: "normalized" },
        rules: { keep_if_any: [], drop_if_any: [] }
      };
}

function upsertCodeRule(
  rules: Record<string, unknown>,
  field: string,
  values: string[]
): void {
  if (values.length === 0) return;
  const keepRaw = Array.isArray(rules.keep_if_any) ? rules.keep_if_any : [];
  const keep = keepRaw
    .filter((rule): rule is Record<string, unknown> => Boolean(rule && typeof rule === "object"))
    .map((rule) => ({ ...rule }));
  const existingIndex = keep.findIndex(
    (rule) => normalize(rule.type) === "code_in" && normalize(rule.field) === field
  );
  const dedupedValues = Array.from(new Set(values.map((value) => normalize(value)).filter((value) => value.length > 0)));
  if (dedupedValues.length === 0) {
    rules.keep_if_any = keep;
    return;
  }
  if (existingIndex >= 0) {
    const existingValues = Array.isArray(keep[existingIndex].values)
      ? (keep[existingIndex].values as unknown[])
          .map((value) => normalize(value))
          .filter((value) => value.length > 0)
      : [];
    keep[existingIndex].values = Array.from(new Set([...existingValues, ...dedupedValues]));
    keep[existingIndex].type = "code_in";
    keep[existingIndex].field = field;
  } else {
    keep.push({ type: "code_in", field, values: dedupedValues });
  }
  rules.keep_if_any = keep;
}

function ensureStep01Filter(
  stepsInput: Array<Record<string, unknown>>,
  input: ConfigGenerationInput,
  selection: CodeSelection
): Array<Record<string, unknown>> {
  const defaultFilter = getDefaultFilterStep(input, selection);
  const steps = stepsInput.map((step) => ({ ...step }));
  let step01 = steps[0];

  if (!step01 || normalize(step01.type) !== "filter") {
    step01 = defaultFilter;
    steps.unshift(step01);
  }

  step01.id = "01-filter-A1";
  step01.type = "filter";
  if (!step01.input || typeof step01.input !== "object") {
    step01.input = { source: "normalized" };
  }
  const inputObj = step01.input as Record<string, unknown>;
  if (!normalize(inputObj.source)) inputObj.source = "normalized";

  if (!step01.rules || typeof step01.rules !== "object") {
    step01.rules = { keep_if_any: [], drop_if_any: [] };
  }
  const rules = step01.rules as Record<string, unknown>;
  if (!Array.isArray(rules.keep_if_any)) rules.keep_if_any = [];
  if (!Array.isArray(rules.drop_if_any)) rules.drop_if_any = [];

  upsertCodeRule(rules, "naics_codes", selection.naicsCodes);
  upsertCodeRule(rules, "sic_codes", selection.sicCodes);

  const keepRules = Array.isArray(rules.keep_if_any) ? rules.keep_if_any : [];
  if (keepRules.length === 0) {
    rules.keep_if_any = [
      {
        type: "regex",
        field: "profile_text",
        pattern: "\\bmortgage\\b|\\bhome loan\\b|\\brefinance\\b|\\bheloc\\b",
        flags: "i"
      }
    ];
  }
  step01.rules = rules;
  return steps;
}

function repairCandidate(candidateInput: Record<string, unknown>, input: ConfigGenerationInput, selection: CodeSelection): Record<string, unknown> {
  const defaultConfig = buildDefaultConfig(input, selection);
  const candidate = { ...candidateInput };

  if (!Array.isArray(candidate.steps)) {
    candidate.steps = defaultConfig.steps;
  }
  if (!candidate.finalize || typeof candidate.finalize !== "object") {
    candidate.finalize = defaultConfig.finalize;
  }

  const steps = Array.isArray(candidate.steps) ? candidate.steps : [];
  const repairedSteps = steps
    .map((step, index) => {
      if (!step || typeof step !== "object") return null;
      const row = { ...(step as Record<string, unknown>) };
      const stepType = normalize(row.type);
      if (!stepType) {
        row.type = "ai_text";
      }
      if (!normalize(row.id)) {
        row.id = `${String(index + 1).padStart(2, "0")}-${normalize(row.type) || "ai_text"}-auto`;
      }
      if (!row.input || typeof row.input !== "object") {
        row.input = { source: index === 0 ? "normalized" : "prev_step" };
      }

      if (row.type === "filter") {
        if (!row.rules || typeof row.rules !== "object") {
          row.rules = {
            keep_if_any: [],
            drop_if_any: []
          };
        }
      }

      if (row.type === "ai_text" || row.type === "web_ai") {
        if (!row.ai || typeof row.ai !== "object") row.ai = {};
        const ai = row.ai as Record<string, unknown>;
        if (!normalize(ai.model)) ai.model = DEFAULT_MODEL;
        if (!Number.isFinite(ai.concurrency)) ai.concurrency = 2;
        if (!Number.isFinite(ai.batch_size)) ai.batch_size = row.type === "web_ai" ? 10 : 20;

        if (row.type === "web_ai") {
          if (!row.scrape || typeof row.scrape !== "object") {
            row.scrape = {
              enabled: true,
              concurrency: 4,
              max_pages_per_domain: 3,
              timeout_ms: 12000,
              cache_by_domain: true,
              url_paths: ["/", "/about", "/company"]
            };
          }
        }

        if (!row.task || typeof row.task !== "object") {
          row.task = ((buildDefaultConfig(input, selection).steps as Array<Record<string, unknown>>)[1] ?? {}).task;
        }
        const task = row.task as Record<string, unknown>;
        if (!normalize(task.criteria_name)) task.criteria_name = "target_classification";
        if (!Array.isArray(task.read_fields) || task.read_fields.length === 0) {
          task.read_fields = chooseReadFields(input.csvHeaders);
        }
        if (!Array.isArray(task.instructions) || task.instructions.length === 0) {
          task.instructions = [
            input.prompt || "Classify company fit.",
            "Return decision as yes/no/not_sure.",
            "Return confidence as high/not_sure/low."
          ];
        }
        if (!normalize(task.decision_field)) task.decision_field = row.type === "web_ai" ? "Decision-2" : "Decision-1";
        if (!normalize(task.confidence_field)) task.confidence_field = row.type === "web_ai" ? "Confidence-2" : "Confidence-1";
        if (!normalize(task.reason_field)) task.reason_field = row.type === "web_ai" ? "Reason-2" : "Reason-1";
        if (!normalize(task.evidence_field)) task.evidence_field = row.type === "web_ai" ? "Evidence-2" : "Evidence-1";
      }

      if (row.type === "apollo_people") {
        if (!row.people || typeof row.people !== "object") row.people = {};
        const people = row.people as Record<string, unknown>;
        if (!Array.isArray(people.target_roles_or_titles) || people.target_roles_or_titles.length === 0) {
          people.target_roles_or_titles = ["Loan Officer", "Mortgage Loan Officer"];
        }
        if (!normalize(people.seniority_min)) {
          people.seniority_min = "Manager";
        }
      }

      return row;
    })
    .filter((step): step is Record<string, unknown> => Boolean(step));

  const repairedOrDefault = repairedSteps.length > 0
    ? repairedSteps
    : ((defaultConfig.steps ?? []) as Array<Record<string, unknown>>);
  candidate.steps = ensureStep01Filter(repairedOrDefault, input, selection);
  return candidate;
}

function formatValidationErrors(error: ZodError): string[] {
  return error.issues.map((issue) => {
    const path = issue.path.length > 0 ? issue.path.join(".") : "root";
    return `${path}: ${issue.message}`;
  });
}

function buildInitialPrompt(input: ConfigGenerationInput, selection: CodeSelection): string {
  const headers = input.csvHeaders.length > 0
    ? input.csvHeaders.join(", ")
    : "(unknown headers)";
  return [
    "You are generating a run config JSON for our pipeline.",
    "Return only a complete JSON object.",
    "Do not return markdown, comments, explanation, or partial patch.",
    "Hard requirements:",
    "1) top-level object with steps[] (required, non-empty).",
    "2) step[0] must be filter step id=01-filter-A1 with keep_if_any code_in rules on naics_codes and sic_codes when relevant.",
    "3) include at least one ai_text step after filter.",
    "4) each ai_text/web_ai step must include: id, type, input, ai, task.",
    "5) if type=web_ai, scrape object is mandatory.",
    "6) task requires: criteria_name, read_fields, instructions, decision_field, confidence_field.",
    "7) If apollo_people exists, people.target_roles_or_titles[] and people.seniority_min are mandatory.",
    "Use concise but valid defaults.",
    `Reference files available on backend: ${NAICS_REFERENCE_PATH} and ${SIC_REFERENCE_PATH}.`,
    `Suggested NAICS codes from prompt: ${selection.naicsCodes.join(", ") || "(none)"}`,
    `Suggested SIC codes from prompt: ${selection.sicCodes.join(", ") || "(none)"}`,
    `Matched NAICS labels: ${selection.matchedNaicsLabels.join(" | ") || "(none)"}`,
    `Matched SIC labels: ${selection.matchedSicLabels.join(" | ") || "(none)"}`,
    `CSV headers: ${headers}`,
    `User intent: ${input.prompt}`,
    "Template example (must be valid JSON object):",
    JSON.stringify(buildDefaultConfig(input, selection), null, 2)
  ].join("\n");
}

function buildFixPrompt(previousJson: string, errors: string[], input: ConfigGenerationInput, selection: CodeSelection): string {
  return [
    "Fix this config so it fully validates.",
    "Return only one complete JSON object, not a patch.",
    "Do not omit required nested objects.",
    "Validation errors:",
    ...errors.map((error) => `- ${error}`),
    "Current invalid JSON:",
    previousJson,
    `Use NAICS reference file: ${NAICS_REFERENCE_PATH}`,
    `Use SIC reference file: ${SIC_REFERENCE_PATH}`,
    `Suggested NAICS codes: ${selection.naicsCodes.join(", ") || "(none)"}`,
    `Suggested SIC codes: ${selection.sicCodes.join(", ") || "(none)"}`,
    "Remember: if step.type is web_ai, include scrape object.",
    "Remember: include step 01 filter with code_in rules (naics_codes/sic_codes) when possible.",
    "If missing, use this safe template and adapt:",
    JSON.stringify(buildDefaultConfig(input, selection), null, 2)
  ].join("\n");
}

export class ConfigGenerator {
  private readonly jobs = new Map<string, ConfigGenerationJob>();

  public constructor(
    private readonly serverUrl: string,
    private readonly model: string,
    private readonly maxAttempts: number
  ) {}

  public startJob(input: ConfigGenerationInput): ConfigGenerationJob {
    const prompt = normalize(input.prompt);
    if (!prompt) {
      throw new Error("prompt is required");
    }
    const csvHeaders = Array.isArray(input.csvHeaders)
      ? input.csvHeaders.map((value) => normalize(value)).filter((value) => value.length > 0)
      : [];
    const job: ConfigGenerationJob = {
      id: randomUUID(),
      status: "running",
      percent: 0,
      stage: "queued",
      attempt: 0,
      maxAttempts: this.maxAttempts,
      error: null,
      validationErrors: [],
      configJson: null,
      updatedAt: nowIso()
    };
    this.jobs.set(job.id, job);
    void this.runJob(job, { prompt, csvHeaders });
    return job;
  }

  public getJob(jobId: string): ConfigGenerationJob | null {
    return this.jobs.get(jobId) ?? null;
  }

  private updateJob(job: ConfigGenerationJob, patch: Partial<ConfigGenerationJob>): void {
    Object.assign(job, patch, { updatedAt: nowIso() });
  }

  private async runJob(job: ConfigGenerationJob, input: ConfigGenerationInput): Promise<void> {
    try {
      const naicsCatalog = readCodeReferenceCatalog(NAICS_REFERENCE_PATH);
      const sicCatalog = readCodeReferenceCatalog(SIC_REFERENCE_PATH);
      const selection = deriveCodesFromPrompt(input.prompt, naicsCatalog, sicCatalog);
      this.updateJob(job, { stage: "creating_session", percent: 5 });
      const sessionId = await this.createSession();

      let errors: string[] = [];
      let lastCandidate = JSON.stringify(buildDefaultConfig(input, selection), null, 2);

      for (let attempt = 1; attempt <= job.maxAttempts; attempt += 1) {
        this.updateJob(job, {
          attempt,
          stage: attempt === 1 ? "generating" : "fixing",
          percent: Math.min(85, 10 + Math.round((attempt / job.maxAttempts) * 65)),
          validationErrors: errors
        });

        const prompt = attempt === 1
          ? buildInitialPrompt(input, selection)
          : buildFixPrompt(lastCandidate, errors, input, selection);
        const responseText = await this.sendMessage(sessionId, prompt);
        this.updateJob(job, { stage: "validating", percent: Math.min(95, job.percent + 8) });

        const candidates = extractJsonCandidates(responseText)
          .map((candidate) => repairCandidate(unwrapCandidate(candidate), input, selection));
        if (candidates.length === 0) {
          errors = ["Response did not contain a valid JSON object."];
          continue;
        }

        let bestErrors: string[] = [];
        let bestCandidate: Record<string, unknown> | null = null;
        for (const candidate of candidates) {
          const validated = RunConfigSchema.safeParse(candidate);
          if (validated.success) {
            this.updateJob(job, {
              status: "completed",
              percent: 100,
              stage: "completed",
              configJson: JSON.stringify(validated.data, null, 2),
              error: null,
              validationErrors: []
            });
            return;
          }
          const nextErrors = formatValidationErrors(validated.error);
          if (bestErrors.length === 0 || nextErrors.length < bestErrors.length) {
            bestErrors = nextErrors;
            bestCandidate = candidate;
          }
        }
        errors = bestErrors.length > 0 ? bestErrors : ["Generated config did not pass schema validation."];
        if (bestCandidate) {
          lastCandidate = JSON.stringify(bestCandidate, null, 2);
        }
      }

      // Last-resort fallback: always return a schema-valid baseline config instead of failing hard.
      const fallbackValidated = RunConfigSchema.safeParse(repairCandidate(buildDefaultConfig(input, selection), input, selection));
      if (fallbackValidated.success) {
        this.updateJob(job, {
          status: "completed",
          stage: "completed",
          percent: 100,
          configJson: JSON.stringify(fallbackValidated.data, null, 2),
          error: null,
          validationErrors: []
        });
        return;
      }

      this.updateJob(job, {
        status: "failed",
        stage: "failed",
        percent: 100,
        error: "Unable to generate a schema-valid config after all retry attempts.",
        validationErrors: errors
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.updateJob(job, {
        status: "failed",
        stage: "failed",
        percent: 100,
        error: message
      });
    }
  }

  private async createSession(): Promise<string> {
    const base = this.serverUrl.replace(/\/+$/, "");
    const response = await this.requestJson<{ id?: string }>(
      `${base}/session`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ title: `config-generator ${new Date().toISOString()}` })
      }
    );
    if (!response.id) throw new Error("OpenCode session id missing");
    return String(response.id);
  }

  private async sendMessage(sessionId: string, prompt: string): Promise<string> {
    const base = this.serverUrl.replace(/\/+$/, "");
    let parsedModel: ProviderModel | null = null;
    try {
      parsedModel = parseProviderModel(this.model);
    } catch {
      parsedModel = null;
    }

    const body: Record<string, unknown> = {
      parts: [{ type: "text", text: prompt }]
    };
    if (parsedModel) body.model = parsedModel;

    const response = await this.requestJson<MessageResponse>(
      `${base}/session/${encodeURIComponent(sessionId)}/message`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body)
      }
    );
    const texts: string[] = [];
    extractTextsFromUnknown(response.parts ?? [], texts);
    return texts.join("\n").trim();
  }

  private async requestJson<T>(url: string, init: RequestInit): Promise<T> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), DEFAULT_TIMEOUT_MS);
    try {
      const response = await fetch(url, { ...init, signal: controller.signal });
      const text = await response.text();
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${text.slice(0, 400)}`);
      }
      return text.trim().length === 0 ? ({} as T) : (JSON.parse(text) as T);
    } catch (error) {
      if (error instanceof Error && error.name === "AbortError") {
        throw new Error(`timeout after ${DEFAULT_TIMEOUT_MS}ms`);
      }
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }
}
