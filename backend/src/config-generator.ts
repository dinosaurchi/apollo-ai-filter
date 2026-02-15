import { randomUUID } from "node:crypto";
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

const DEFAULT_TIMEOUT_MS = 60000;

function nowIso(): string {
  return new Date().toISOString();
}

function normalize(value: unknown): string {
  return String(value ?? "").trim();
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

function extractJsonObject(text: string): Record<string, unknown> | null {
  const tryParseObject = (raw: string): Record<string, unknown> | null => {
    try {
      const parsed = JSON.parse(raw) as unknown;
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
      return parsed as Record<string, unknown>;
    } catch {
      return null;
    }
  };

  const direct = tryParseObject(text.trim());
  if (direct) return direct;

  const fenceRegex = /```(?:json)?\s*([\s\S]*?)```/gi;
  for (const match of text.matchAll(fenceRegex)) {
    const parsed = tryParseObject((match[1] ?? "").trim());
    if (parsed) return parsed;
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
          const parsed = tryParseObject(text.slice(start, i + 1));
          if (parsed) return parsed;
          break;
        }
      }
    }
  }
  return null;
}

function formatValidationErrors(error: ZodError): string[] {
  return error.issues.map((issue) => {
    const path = issue.path.length > 0 ? issue.path.join(".") : "root";
    return `${path}: ${issue.message}`;
  });
}

function buildInitialPrompt(input: ConfigGenerationInput): string {
  const headers = input.csvHeaders.length > 0
    ? input.csvHeaders.join(", ")
    : "(unknown headers)";
  return [
    "You generate JSON config for our analyzer pipeline.",
    "Return exactly one JSON object and nothing else.",
    "The JSON must validate the following shape:",
    "- top-level object with keys: run?, io?, normalize?, steps (required), finalize?",
    "- steps is a non-empty array of step objects.",
    "- include at least one ai_text step.",
    "- supported step types: filter, ai_text, web_ai, apollo_people.",
    "- ai_text/web_ai steps require: id, type, input?, ai{}, task{}.",
    "- task requires: criteria_name, read_fields[], instructions[], decision_field, confidence_field.",
    "- apollo_people.people requires target_roles_or_titles[] and seniority_min.",
    "Use CSV headers only when needed for read_fields and filters.",
    `CSV headers: ${headers}`,
    `User intent: ${input.prompt}`,
    "Output strict JSON only."
  ].join("\n");
}

function buildFixPrompt(previousJson: string, errors: string[]): string {
  return [
    "Fix the JSON config so it validates.",
    "Return exactly one JSON object and nothing else.",
    "Validation errors:",
    ...errors.map((error) => `- ${error}`),
    "Previous JSON:",
    previousJson
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
      this.updateJob(job, { stage: "creating_session", percent: 5 });
      const sessionId = await this.createSession();

      let errors: string[] = [];
      let lastCandidate = "";

      for (let attempt = 1; attempt <= job.maxAttempts; attempt += 1) {
        this.updateJob(job, {
          attempt,
          stage: attempt === 1 ? "generating" : "fixing",
          percent: Math.min(80, 10 + Math.round((attempt / job.maxAttempts) * 60)),
          validationErrors: errors
        });

        const prompt = attempt === 1
          ? buildInitialPrompt(input)
          : buildFixPrompt(lastCandidate, errors);
        const responseText = await this.sendMessage(sessionId, prompt);

        this.updateJob(job, { stage: "validating", percent: Math.min(90, job.percent + 10) });

        const candidate = extractJsonObject(responseText);
        if (!candidate) {
          errors = ["Response did not contain a valid JSON object."];
          lastCandidate = responseText.trim().slice(0, 12000);
          continue;
        }
        lastCandidate = JSON.stringify(candidate, null, 2);
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
        errors = formatValidationErrors(validated.error);
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
