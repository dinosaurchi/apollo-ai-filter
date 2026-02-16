import { afterEach, describe, expect, it, vi } from "vitest";
import { ConfigGenerator, type ConfigGenerationJob } from "../src/config-generator";

function buildValidConfig(): Record<string, unknown> {
  return {
    run: { name: "generated-run", id_field: "id" },
    normalize: {
      trim_all_strings: true,
      derive: {
        domain_from: "website",
        profile_text_fields: ["name", "industry", "keywords", "short_description"]
      }
    },
    steps: [
      {
        id: "01-filter-A1",
        type: "filter",
        input: { source: "normalized" },
        rules: {
          keep_if_any: [{ type: "code_in", field: "naics_codes", values: ["522292"] }]
        }
      },
      {
        id: "02-ai-text-A2",
        type: "ai_text",
        input: { source: "prev_step" },
        ai: { model: "opencode/gpt-5-nano", concurrency: 1, batch_size: 10 },
        task: {
          criteria_name: "example",
          read_fields: ["name", "industry"],
          instructions: ["Classify fit."],
          decision_field: "Decision-1",
          confidence_field: "Confidence-1"
        }
      }
    ],
    finalize: { output_csv: "A1.csv" }
  };
}

async function waitForDone(generator: ConfigGenerator, jobId: string): Promise<ConfigGenerationJob> {
  for (let i = 0; i < 40; i += 1) {
    const job = generator.getJob(jobId);
    if (job && job.status !== "running") return job;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error("timed out waiting for job completion");
}

describe("ConfigGenerator", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("retries after invalid output and completes with schema-valid config", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch");
    const valid = buildValidConfig();
    let messageCall = 0;
    fetchMock.mockImplementation(async (input: RequestInfo | URL) => {
      const url = input.toString();
      if (url.endsWith("/session")) {
        return {
          ok: true,
          text: async () => JSON.stringify({ id: "session-1" })
        } as Response;
      }
      if (url.includes("/session/session-1/message")) {
        messageCall += 1;
        if (messageCall === 1) {
          return {
            ok: true,
            text: async () => JSON.stringify({ parts: [{ type: "text", text: "not valid json" }] })
          } as Response;
        }
        return {
          ok: true,
          text: async () => JSON.stringify({ parts: [{ type: "text", text: JSON.stringify(valid) }] })
        } as Response;
      }
      throw new Error(`Unexpected URL: ${url}`);
    });

    const generator = new ConfigGenerator("http://localhost:3000", "opencode/gpt-5-nano", 3);
    const job = generator.startJob({
      prompt: "Find mortgage lenders",
      csvHeaders: ["id", "name", "website", "industry"]
    });
    const done = await waitForDone(generator, job.id);

    expect(done.status).toBe("completed");
    expect(done.configJson).toBeTruthy();
    expect(messageCall).toBe(2);
  });

  it("auto-repairs simple invalid output into a valid config", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input: RequestInfo | URL) => {
      const url = input.toString();
      if (url.endsWith("/session")) {
        return {
          ok: true,
          text: async () => JSON.stringify({ id: "session-2" })
        } as Response;
      }
      if (url.includes("/session/session-2/message")) {
        return {
          ok: true,
          text: async () => JSON.stringify({ parts: [{ type: "text", text: "{\"steps\":[]}" }] })
        } as Response;
      }
      throw new Error(`Unexpected URL: ${url}`);
    });

    const generator = new ConfigGenerator("http://localhost:3000", "opencode/gpt-5-nano", 2);
    const job = generator.startJob({ prompt: "Generate config", csvHeaders: [] });
    const done = await waitForDone(generator, job.id);

    expect(done.status).toBe("completed");
    const parsed = JSON.parse(done.configJson ?? "{}") as { steps?: unknown[] };
    expect(Array.isArray(parsed.steps)).toBe(true);
    expect(parsed.steps?.length ?? 0).toBeGreaterThan(0);
  });

  it("unwraps nested config response and repairs missing required nested fields", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input: RequestInfo | URL) => {
      const url = input.toString();
      if (url.endsWith("/session")) {
        return {
          ok: true,
          text: async () => JSON.stringify({ id: "session-3" })
        } as Response;
      }
      if (url.includes("/session/session-3/message")) {
        return {
          ok: true,
          text: async () => JSON.stringify({
            parts: [
              {
                type: "text",
                text: JSON.stringify({
                  config: {
                    steps: [
                      {
                        id: "01-web-a3",
                        type: "web_ai",
                        ai: {},
                        task: {
                          criteria_name: "x",
                          read_fields: ["name"],
                          instructions: ["find"],
                          decision_field: "Decision-1",
                          confidence_field: "Confidence-1"
                        }
                      }
                    ]
                  }
                })
              }
            ]
          })
        } as Response;
      }
      throw new Error(`Unexpected URL: ${url}`);
    });

    const generator = new ConfigGenerator("http://localhost:3000", "opencode/gpt-5-nano", 3);
    const job = generator.startJob({ prompt: "deep web research", csvHeaders: ["name", "website"] });
    const done = await waitForDone(generator, job.id);

    expect(done.status).toBe("completed");
    const parsed = JSON.parse(done.configJson ?? "{}") as { steps?: Array<Record<string, unknown>> };
    const steps = Array.isArray(parsed.steps) ? parsed.steps : [];
    const webStep = steps.find((step) => step.type === "web_ai");
    expect(steps[0]?.type).toBe("filter");
    expect(webStep?.scrape).toBeTruthy();
  });

  it("enforces step 01 filter with NAICS and SIC code rules for mortgage prompts", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input: RequestInfo | URL) => {
      const url = input.toString();
      if (url.endsWith("/session")) {
        return {
          ok: true,
          text: async () => JSON.stringify({ id: "session-4" })
        } as Response;
      }
      if (url.includes("/session/session-4/message")) {
        return {
          ok: true,
          text: async () =>
            JSON.stringify({
              parts: [
                {
                  type: "text",
                  text: JSON.stringify({
                    steps: [
                      {
                        id: "01-ai-text-A1",
                        type: "ai_text",
                        input: { source: "normalized" },
                        ai: {},
                        task: {
                          criteria_name: "mortgage_lender",
                          read_fields: ["name", "industry"],
                          instructions: ["Classify fit"],
                          decision_field: "Decision-1",
                          confidence_field: "Confidence-1"
                        }
                      }
                    ]
                  })
                }
              ]
            })
        } as Response;
      }
      throw new Error(`Unexpected URL: ${url}`);
    });

    const generator = new ConfigGenerator("http://localhost:3000", "opencode/gpt-5-nano", 2);
    const job = generator.startJob({
      prompt: "Find non-bank mortgage lenders in the US and exclude brokers",
      csvHeaders: ["id", "name", "naics_codes", "sic_codes", "industry"]
    });
    const done = await waitForDone(generator, job.id);

    expect(done.status).toBe("completed");
    const parsed = JSON.parse(done.configJson ?? "{}") as { steps?: Array<Record<string, unknown>> };
    const firstStep = Array.isArray(parsed.steps) ? parsed.steps[0] : null;
    expect(firstStep?.id).toBe("01-filter-A1");
    expect(firstStep?.type).toBe("filter");
    const keepRules = (((firstStep?.rules as Record<string, unknown> | undefined)?.keep_if_any ?? []) as Array<Record<string, unknown>>);
    const naicsRule = keepRules.find((rule) => rule.type === "code_in" && rule.field === "naics_codes");
    const sicRule = keepRules.find((rule) => rule.type === "code_in" && rule.field === "sic_codes");
    expect(Array.isArray(naicsRule?.values)).toBe(true);
    expect(Array.isArray(sicRule?.values)).toBe(true);
    expect((naicsRule?.values as string[]).includes("522292")).toBe(true);
    expect((sicRule?.values as string[]).includes("6162")).toBe(true);
  });
});
