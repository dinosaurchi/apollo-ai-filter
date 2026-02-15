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
        id: "01-ai-text-A1",
        type: "ai_text",
        input: { source: "normalized" },
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

  it("fails after max attempts when output never validates", async () => {
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

    expect(done.status).toBe("failed");
    expect(done.error).toContain("Unable to generate a schema-valid config");
    expect(done.validationErrors.length).toBeGreaterThan(0);
  });
});

