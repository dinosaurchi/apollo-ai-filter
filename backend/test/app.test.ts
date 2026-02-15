import request from "supertest";
import { afterEach, describe, expect, it, vi } from "vitest";
import { app, runManager } from "../src/app";
import * as entityStore from "../src/entity-store";
import * as apolloEnrich from "../src/apollo-enrich";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("GET /health", () => {
  it("returns ok", async () => {
    const response = await request(app).get("/health");
    expect(response.status).toBe(200);
    expect(response.body).toEqual({ ok: true });
  });
});

describe("GET /runs/:id", () => {
  it("returns run detail with step summaries", async () => {
    vi.spyOn(runManager, "getRun").mockReturnValue(({
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
      runRootDir: "/tmp/output/runs/run-1"
    } as unknown) as ReturnType<typeof runManager.getRun>);
    vi.spyOn(runManager, "getRunStepSummaries").mockResolvedValue([
      {
        id: "02-ai-text-A2",
        type: "ai_text",
        title: "02-ai-text-A2",
        inputRows: 10,
        outputRows: 8,
        progressValue: "8/10",
        status: "finished"
      }
    ]);

    const response = await request(app).get("/runs/run-1");

    expect(response.status).toBe(200);
    expect(response.body.ok).toBe(true);
    expect(response.body.run.id).toBe("run-1");
    expect(response.body.run.stepSummaries).toEqual([
      {
        id: "02-ai-text-A2",
        type: "ai_text",
        title: "02-ai-text-A2",
        inputRows: 10,
        outputRows: 8,
        progressValue: "8/10",
        status: "finished"
      }
    ]);
  });
});

describe("POST /people/:personId/enrich", () => {
  it("enriches a person and updates DB fields", async () => {
    vi.spyOn(runManager, "listRuns").mockReturnValue([]);
    vi.spyOn(entityStore, "getPersonByIdFromDb")
      .mockResolvedValueOnce({
        person_id: "person-1",
        full_name: "Old Name",
        email: "",
        linkedin_url: "https://linkedin.com/in/person-1",
        company_domain: "example.com",
        company_id: "company-1",
        company_name: "Example Co",
        title: "",
        location: "",
        run_id: "run-1"
      })
      .mockResolvedValueOnce({
        person_id: "person-1",
        full_name: "New Name",
        email: "new@example.com",
        linkedin_url: "https://linkedin.com/in/person-1",
        company_domain: "example.com",
        company_id: "company-1",
        company_name: "Example Co",
        title: "VP Sales",
        location: "USA",
        run_id: "run-1"
      });
    const updateSpy = vi.spyOn(entityStore, "updatePersonFromEnrichment").mockResolvedValue();
    vi.spyOn(apolloEnrich, "enrichPersonFromApollo").mockResolvedValue({
      full_name: "New Name",
      title: "VP Sales",
      email: "new@example.com",
      linkedin_url: "https://linkedin.com/in/person-1",
      location: "USA"
    });

    const response = await request(app).post("/people/person-1/enrich");

    expect(response.status).toBe(200);
    expect(response.body.ok).toBe(true);
    expect(updateSpy).toHaveBeenCalledWith("person-1", {
      full_name: "New Name",
      title: "VP Sales",
      email: "new@example.com",
      linkedin_url: "https://linkedin.com/in/person-1",
      location: "USA"
    });
    expect(response.body.person.full_name).toBe("New Name");
  });
});
