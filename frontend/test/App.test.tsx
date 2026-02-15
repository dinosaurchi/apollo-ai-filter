import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { App } from "../src/App";

afterEach(() => {
  vi.restoreAllMocks();
  window.location.hash = "";
});

describe("App", () => {
  it("renders main heading", async () => {
    class EventSourceMock {
      public addEventListener(): void {}
      public close(): void {}
    }
    vi.stubGlobal("EventSource", EventSourceMock);
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input: RequestInfo | URL) => {
      const url = input.toString();
      if (url.endsWith("/api/health")) {
        return {
          ok: true,
          json: async () => ({ ok: true })
        } as Response;
      }
      if (url.endsWith("/api/runs")) {
        return {
          ok: true,
          json: async () => ({ ok: true, runs: [] })
        } as Response;
      }
      return {
        ok: true,
        json: async () => ({ ok: true })
      } as Response;
    });

    render(<App />);

    expect(screen.getByRole("heading", { name: "Apollo Filter App" })).toBeInTheDocument();
    expect(await screen.findByText("Backend is healthy")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Run Monitor" })).toBeInTheDocument();
  });

  it("generates config via AI mode and fills config JSON", async () => {
    class EventSourceMock {
      public addEventListener(): void {}
      public close(): void {}
    }
    vi.stubGlobal("EventSource", EventSourceMock);
    let progressCalls = 0;

    vi.spyOn(globalThis, "fetch").mockImplementation(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = input.toString();
      if (url.endsWith("/api/health")) {
        return { ok: true, json: async () => ({ ok: true }) } as Response;
      }
      if (url.endsWith("/api/runs")) {
        return { ok: true, json: async () => ({ ok: true, runs: [] }) } as Response;
      }
      if (url.endsWith("/api/config/generate/start") && (init?.method ?? "GET") === "POST") {
        return { ok: true, json: async () => ({ ok: true, jobId: "cfg-1" }) } as Response;
      }
      if (url.includes("/api/config/generate/progress?jobId=cfg-1")) {
        progressCalls += 1;
        if (progressCalls === 1) {
          return {
            ok: true,
            json: async () => ({
              ok: true,
              job: {
                id: "cfg-1",
                status: "running",
                percent: 40,
                stage: "generating",
                attempt: 1,
                maxAttempts: 6,
                error: null,
                validationErrors: []
              }
            })
          } as Response;
        }
        return {
          ok: true,
          json: async () => ({
            ok: true,
            job: {
              id: "cfg-1",
              status: "completed",
              percent: 100,
              stage: "completed",
              attempt: 2,
              maxAttempts: 6,
              error: null,
              validationErrors: []
            }
          })
        } as Response;
      }
      if (url.includes("/api/config/generate/result?jobId=cfg-1")) {
        return {
          ok: true,
          json: async () => ({
            ok: true,
            ready: true,
            configJson: JSON.stringify({
              steps: [
                {
                  id: "01-ai-text-A1",
                  type: "ai_text",
                  input: { source: "normalized" },
                  ai: {},
                  task: {
                    criteria_name: "x",
                    read_fields: ["name"],
                    instructions: ["y"],
                    decision_field: "Decision-1",
                    confidence_field: "Confidence-1"
                  }
                }
              ]
            })
          })
        } as Response;
      }
      return { ok: true, json: async () => ({ ok: true }) } as Response;
    });

    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Generate with AI" }));
    fireEvent.change(screen.getByPlaceholderText("Describe your filtering pipeline idea in plain English..."), {
      target: { value: "Find mortgage lenders and enrich decision confidence." }
    });
    fireEvent.click(screen.getByRole("button", { name: "Generate Config" }));

    expect(await screen.findByText(/Generator status:/)).toBeInTheDocument();
    const configArea = await screen.findByPlaceholderText("Generated config will appear here. You can edit it before submitting.");
    await waitFor(() => {
      expect((configArea as HTMLTextAreaElement).value).toContain("\"steps\"");
    });
  });

  it("shows step input/output row counts in run overview detail", async () => {
    class EventSourceMock {
      public addEventListener(): void {}
      public close(): void {}
    }
    vi.stubGlobal("EventSource", EventSourceMock);
    window.location.hash = "#/runs";
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = input.toString();
      if (url.endsWith("/api/health")) {
        return {
          ok: true,
          json: async () => ({ ok: true })
        } as Response;
      }
      if (url.endsWith("/api/runs")) {
        return {
          ok: true,
          json: async () => ({
            ok: true,
            runs: [
              {
                id: "run-1",
                status: "completed",
                createdAt: "2026-02-14T20:00:00.000Z",
                updatedAt: "2026-02-14T20:01:00.000Z",
                inputFileName: "example.csv",
                progress: {
                  totalSteps: 5,
                  completedSteps: 5,
                  currentStep: null,
                  message: "Completed"
                },
                error: null,
                pid: null
              }
            ]
          })
        } as Response;
      }
      if (url.endsWith("/api/runs/run-1/overview")) {
        return {
          ok: true,
          json: async () => ({
            ok: true,
            run: {
              id: "run-1",
              status: "completed",
              inputFileName: "example.csv",
              createdAt: "2026-02-14T20:00:00.000Z",
              updatedAt: "2026-02-14T20:01:00.000Z",
              error: null,
              progress: {
                totalSteps: 5,
                completedSteps: 5,
                currentStep: null,
                message: "Completed"
              },
              logs: [],
              analysisRunDir: "/tmp/output/run-1",
              inputConfigJson: "{}",
              stepSummaries: []
            }
          })
        } as Response;
      }
      if (url.endsWith("/api/runs/run-1/heavy/step-summaries/start") && (init?.method ?? "GET") === "POST") {
        return {
          ok: true,
          json: async () => ({ ok: true, jobId: "step-job-1" })
        } as Response;
      }
      if (url.includes("/api/runs/run-1/heavy/step-summaries/progress?jobId=step-job-1")) {
        return {
          ok: true,
          json: async () => ({
            ok: true,
            job: {
              status: "completed",
              percent: 100,
              steps: []
            }
          })
        } as Response;
      }
      if (url.includes("/api/runs/run-1/heavy/step-summaries/result?jobId=step-job-1")) {
        return {
          ok: true,
          json: async () => ({
            ok: true,
            ready: true,
            stepSummaries: [
              {
                id: "02-ai-text-A2",
                type: "ai_text",
                title: "02-ai-text-A2",
                inputRows: 10,
                outputRows: 8,
                progressValue: "8/10",
                status: "finished"
              }
            ]
          })
        } as Response;
      }
      if (url.endsWith("/api/runs/run-1/heavy/logs/start") && (init?.method ?? "GET") === "POST") {
        return {
          ok: true,
          json: async () => ({ ok: true, jobId: "logs-job-1" })
        } as Response;
      }
      if (url.includes("/api/runs/run-1/heavy/logs/progress?jobId=logs-job-1")) {
        return {
          ok: true,
          json: async () => ({ ok: true, job: { status: "completed", percent: 100 } })
        } as Response;
      }
      if (url.includes("/api/runs/run-1/heavy/logs/result?jobId=logs-job-1")) {
        return {
          ok: true,
          json: async () => ({ ok: true, ready: true, logs: [] })
        } as Response;
      }
      return {
        ok: true,
        json: async () => ({ ok: true })
      } as Response;
    });

    render(<App />);

    const detailButton = await screen.findByRole("button", { name: "Detail" });
    fireEvent.click(detailButton);

    expect(await screen.findByText("Input Rows")).toBeInTheDocument();
    expect(screen.getByText("02-ai-text-A2")).toBeInTheDocument();
    expect(screen.getByText("8/10")).toBeInTheDocument();
    expect(screen.getByText("Finished")).toBeInTheDocument();
    expect(screen.getByText("10")).toBeInTheDocument();
    expect(screen.getByText("8")).toBeInTheDocument();
  });

  it("paginates run monitor table with default page size 20", async () => {
    class EventSourceMock {
      public addEventListener(): void {}
      public close(): void {}
    }
    vi.stubGlobal("EventSource", EventSourceMock);
    window.location.hash = "#/runs";

    const runs = Array.from({ length: 25 }, (_, idx) => {
      const n = idx + 1;
      return {
        id: `run-${String(n).padStart(2, "0")}`,
        status: "completed",
        createdAt: `2026-02-14T20:${String(idx).padStart(2, "0")}:00.000Z`,
        updatedAt: `2026-02-14T20:${String(idx).padStart(2, "0")}:30.000Z`,
        inputFileName: "example.csv",
        progress: {
          totalSteps: 5,
          completedSteps: 5,
          currentStep: null,
          message: "Completed"
        },
        error: null,
        pid: null
      };
    });

    vi.spyOn(globalThis, "fetch").mockImplementation(async (input: RequestInfo | URL) => {
      const url = input.toString();
      if (url.endsWith("/api/health")) {
        return {
          ok: true,
          json: async () => ({ ok: true })
        } as Response;
      }
      if (url.endsWith("/api/runs")) {
        return {
          ok: true,
          json: async () => ({ ok: true, runs })
        } as Response;
      }
      return {
        ok: true,
        json: async () => ({ ok: true })
      } as Response;
    });

    render(<App />);

    expect(await screen.findByText("1-20 of 25")).toBeInTheDocument();
    expect(screen.getByText("run-01")).toBeInTheDocument();
    expect(screen.queryByText("run-21")).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Next" }));

    expect(await screen.findByText("21-25 of 25")).toBeInTheDocument();
    expect(screen.getByText("run-21")).toBeInTheDocument();
  });

  it("loads run companies only when companies tab is opened", async () => {
    class EventSourceMock {
      public addEventListener(): void {}
      public close(): void {}
    }
    vi.stubGlobal("EventSource", EventSourceMock);
    window.location.hash = "#/runs";
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = input.toString();
      if (url.endsWith("/api/health")) {
        return {
          ok: true,
          json: async () => ({ ok: true })
        } as Response;
      }
      if (url.endsWith("/api/runs")) {
        return {
          ok: true,
          json: async () => ({
            ok: true,
            runs: [
              {
                id: "run-1",
                status: "completed",
                createdAt: "2026-02-14T20:00:00.000Z",
                updatedAt: "2026-02-14T20:01:00.000Z",
                inputFileName: "example.csv",
                progress: {
                  totalSteps: 5,
                  completedSteps: 5,
                  currentStep: null,
                  message: "Completed"
                },
                error: null,
                pid: null
              }
            ]
          })
        } as Response;
      }
      if (url.endsWith("/api/runs/run-1/overview")) {
        return {
          ok: true,
          json: async () => ({
            ok: true,
            run: {
              id: "run-1",
              status: "completed",
              inputFileName: "example.csv",
              createdAt: "2026-02-14T20:00:00.000Z",
              updatedAt: "2026-02-14T20:01:00.000Z",
              error: null,
              progress: {
                totalSteps: 5,
                completedSteps: 5,
                currentStep: null,
                message: "Completed"
              },
              logs: [],
              analysisRunDir: "/tmp/output/run-1",
              inputConfigJson: "{}",
              stepSummaries: []
            }
          })
        } as Response;
      }
      if (url.endsWith("/api/runs/run-1/heavy/step-summaries/start") && (init?.method ?? "GET") === "POST") {
        return {
          ok: true,
          json: async () => ({ ok: true, jobId: "step-job-1" })
        } as Response;
      }
      if (url.includes("/api/runs/run-1/heavy/step-summaries/progress?jobId=step-job-1")) {
        return {
          ok: true,
          json: async () => ({ ok: true, job: { status: "completed", percent: 100, steps: [] } })
        } as Response;
      }
      if (url.includes("/api/runs/run-1/heavy/step-summaries/result?jobId=step-job-1")) {
        return {
          ok: true,
          json: async () => ({ ok: true, ready: true, stepSummaries: [] })
        } as Response;
      }
      if (url.endsWith("/api/runs/run-1/heavy/logs/start") && (init?.method ?? "GET") === "POST") {
        return {
          ok: true,
          json: async () => ({ ok: true, jobId: "logs-job-1" })
        } as Response;
      }
      if (url.includes("/api/runs/run-1/heavy/logs/progress?jobId=logs-job-1")) {
        return {
          ok: true,
          json: async () => ({ ok: true, job: { status: "completed", percent: 100 } })
        } as Response;
      }
      if (url.includes("/api/runs/run-1/heavy/logs/result?jobId=logs-job-1")) {
        return {
          ok: true,
          json: async () => ({ ok: true, ready: true, logs: [] })
        } as Response;
      }
      if (url.endsWith("/api/runs/run-1/companies")) {
        return {
          ok: true,
          json: async () => ({
            ok: true,
            companies: [
              {
                run_id: "run-1",
                company_id: "cmp-1",
                company_name: "Acme Lending",
                company_domain: "acme.test",
                decision: "yes",
                confidence: "high",
                evidence: "evidence"
              }
            ]
          })
        } as Response;
      }
      return {
        ok: true,
        json: async () => ({ ok: true })
      } as Response;
    });

    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: "Detail" }));
    expect(await screen.findByText("Run Detail: run-1")).toBeInTheDocument();
    expect(fetchMock.mock.calls.some((call) => call[0].toString().endsWith("/api/runs/run-1/companies"))).toBe(false);

    const dialog = screen.getByRole("dialog");
    fireEvent.click(within(dialog).getByRole("button", { name: "Companies" }));

    expect(await screen.findByText("Acme Lending")).toBeInTheDocument();
    expect(fetchMock.mock.calls.some((call) => call[0].toString().endsWith("/api/runs/run-1/companies"))).toBe(true);
  });
});
