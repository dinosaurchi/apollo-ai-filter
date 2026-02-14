import { fireEvent, render, screen } from "@testing-library/react";
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

  it("shows step input/output row counts in run overview detail", async () => {
    class EventSourceMock {
      public addEventListener(): void {}
      public close(): void {}
    }
    vi.stubGlobal("EventSource", EventSourceMock);
    window.location.hash = "#/runs";
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
      if (url.endsWith("/api/runs/run-1")) {
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
              stepSummaries: [
                {
                  id: "02-ai-text-A2",
                  type: "ai_text",
                  title: "02-ai-text-A2",
                  inputRows: 10,
                  outputRows: 8,
                  progressText: "Completed",
                  status: "finished"
                }
              ]
            }
          })
        } as Response;
      }
      if (url.endsWith("/api/runs/run-1/companies")) {
        return {
          ok: true,
          json: async () => ({ ok: true, companies: [] })
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
    expect(screen.getByText("Completed")).toBeInTheDocument();
    expect(screen.getByText("Finished")).toBeInTheDocument();
    expect(screen.getByText("10")).toBeInTheDocument();
    expect(screen.getByText("8")).toBeInTheDocument();
  });
});
