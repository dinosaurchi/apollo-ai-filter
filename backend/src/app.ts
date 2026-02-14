import express from "express";
import { checkDbConnection } from "./db";
import multer from "multer";
import { env } from "./config";
import { RunManager } from "./run-manager";
import fsp from "node:fs/promises";
import path from "node:path";
import {
  getRunIngestion,
  ingestCompanies,
  ingestPeople,
  listCompaniesFromDb,
  listCompanyReviews,
  listRunIngestionRecords,
  getRunIngestionSummary,
  listPeopleByCompanyFromDb,
  listPeopleFromDb,
  listPeopleReviews,
  markRunIngestionCompleted,
  markRunIngestionFailed,
  markRunIngestionInProgress,
  markRunIngestionPending,
  resolveCompanyReview,
  resolvePeopleReview
} from "./entity-store";

export const app = express();
export const runManager = new RunManager(
  env.OUTPUT_ROOT,
  env.ANALYZE_SCRIPT_DIR,
  env.OPENCODE_SERVER_URL,
  env.ANALYZER_NODE_OPTIONS
);
export const runManagerReady = runManager.init();

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 50 * 1024 * 1024
  }
});

app.use(express.json({ limit: "5mb" }));

async function syncSingleRunToDb(run: ReturnType<RunManager["listRuns"]>[number], force = false): Promise<void> {
  const existing = await getRunIngestion(run.id);
  if (!force && existing?.status === "completed") return;
  if (run.status === "queued" || run.status === "running") return;
  await markRunIngestionInProgress(run.id);
  try {
    const companies = await runManager.getRunCompanies(run.id, { includeRaw: true });
    const people = await runManager.getRunPeople(run.id);
    if (run.status === "completed" && companies.length === 0) {
      throw new Error("Completed run has no company result rows to ingest");
    }
    const companyRows = await ingestCompanies(run.id, companies);
    const peopleRows = await ingestPeople(run.id, people);
    const terminal = run.status === "completed" || run.status === "failed" || run.status === "cancelled";
    if (terminal) {
      await markRunIngestionCompleted(run.id, true, true, companyRows, peopleRows);
      return;
    }
    await markRunIngestionPending(run.id, companyRows > 0, peopleRows > 0);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await markRunIngestionFailed(run.id, message);
    if (force) throw error;
  }
}

async function syncRunsToDb(options?: { runIds?: string[]; force?: boolean; onlyFailed?: boolean }): Promise<void> {
  const runIds = new Set(options?.runIds ?? []);
  const runs = runManager.listRuns().filter((run) => runIds.size === 0 || runIds.has(run.id));
  for (const run of runs) {
    if (options?.onlyFailed) {
      const existing = await getRunIngestion(run.id);
      if (existing?.status !== "failed") continue;
    }
    await syncSingleRunToDb(run, Boolean(options?.force));
  }
}

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

app.get("/db/health", async (_req, res, next) => {
  try {
    const ok = await checkDbConnection();
    res.json({ ok });
  } catch (error) {
    next(error);
  }
});

app.post("/runs/validate", upload.single("csvFile"), (req, res) => {
  try {
    const configJson = String(req.body.configJson ?? "");
    if (!req.file) {
      res.status(400).json({ ok: false, error: "csvFile is required" });
      return;
    }
    if (!configJson) {
      res.status(400).json({ ok: false, error: "configJson is required" });
      return;
    }
    const result = runManager.validateInput({
      configJson,
      csvFileName: req.file.originalname || "input.csv",
      csvBuffer: req.file.buffer
    });
    res.json({
      ok: true,
      csvHeaders: result.csvHeaders,
      missingRequiredColumns: result.missingRequiredColumns
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Validation failed";
    res.status(400).json({ ok: false, error: message });
  }
});

app.post("/runs", upload.single("csvFile"), async (req, res, next) => {
  try {
    const configJson = String(req.body.configJson ?? "");
    if (!req.file) {
      res.status(400).json({ ok: false, error: "csvFile is required" });
      return;
    }
    if (!configJson) {
      res.status(400).json({ ok: false, error: "configJson is required" });
      return;
    }
    const run = await runManager.createRun({
      configJson,
      csvFileName: req.file.originalname || "input.csv",
      csvBuffer: req.file.buffer
    });
    res.status(201).json({ ok: true, run });
  } catch (error) {
    next(error);
  }
});

app.get("/runs", (_req, res) => {
  res.json({ ok: true, runs: runManager.listRuns() });
});

app.get("/runs/stream", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  const send = (event: string, payload: unknown): void => {
    res.write(`event: ${event}\n`);
    res.write(`data: ${JSON.stringify(payload)}\n\n`);
  };

  send("snapshot", { runs: runManager.listRuns() });

  const onUpdate = (run: unknown): void => {
    send("run_update", run);
  };
  runManager.on("run_updated", onUpdate);

  const ping = setInterval(() => {
    res.write(": ping\n\n");
  }, 10000);

  req.on("close", () => {
    clearInterval(ping);
    runManager.off("run_updated", onUpdate);
    res.end();
  });
});

app.get("/runs/:id", (req, res) => {
  void (async () => {
    const run = runManager.getRun(req.params.id);
    if (!run) {
      res.status(404).json({ ok: false, error: "Run not found" });
      return;
    }
    let inputConfigJson = "";
    const inputConfigPath = path.resolve(run.runRootDir, "input", "config.original.json");
    try {
      inputConfigJson = await fsp.readFile(inputConfigPath, "utf8");
    } catch {
      // Keep empty string when original config is unavailable.
    }
    res.json({ ok: true, run: { ...run, inputConfigJson } });
  })().catch((error) => {
    const message = error instanceof Error ? error.message : "Failed to load run";
    res.status(500).json({ ok: false, error: message });
  });
});

app.post("/runs/:id/cancel", async (req, res, next) => {
  try {
    const run = await runManager.cancelRun(req.params.id);
    res.json({ ok: true, run });
  } catch (error) {
    next(error);
  }
});

app.get("/runs/:id/companies", async (req, res, next) => {
  try {
    const companies = await runManager.getRunCompanies(req.params.id);
    res.json({ ok: true, companies });
  } catch (error) {
    next(error);
  }
});

app.get("/runs/:id/people", async (req, res, next) => {
  try {
    const people = await runManager.getRunPeople(req.params.id);
    res.json({ ok: true, people });
  } catch (error) {
    next(error);
  }
});

app.get("/companies", async (_req, res, next) => {
  try {
    await syncRunsToDb();
    const companies = await listCompaniesFromDb();
    res.json({ ok: true, companies });
  } catch (error) {
    next(error);
  }
});

app.get("/companies/:companyId/people", async (req, res, next) => {
  try {
    await syncRunsToDb();
    const people = await listPeopleByCompanyFromDb(req.params.companyId);
    res.json({ ok: true, people });
  } catch (error) {
    next(error);
  }
});

app.get("/people", async (_req, res, next) => {
  try {
    await syncRunsToDb();
    const people = await listPeopleFromDb();
    res.json({ ok: true, people });
  } catch (error) {
    next(error);
  }
});

app.get("/reviews/companies", async (_req, res, next) => {
  try {
    await syncRunsToDb();
    const reviews = await listCompanyReviews();
    res.json({ ok: true, reviews });
  } catch (error) {
    next(error);
  }
});

app.get("/reviews/people", async (_req, res, next) => {
  try {
    await syncRunsToDb();
    const reviews = await listPeopleReviews();
    res.json({ ok: true, reviews });
  } catch (error) {
    next(error);
  }
});

app.get("/admin/ingestion/status", async (req, res, next) => {
  try {
    const limitRaw = Number.parseInt(String(req.query.limit ?? "100"), 10);
    const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 500) : 100;
    const [summary, records] = await Promise.all([
      getRunIngestionSummary(),
      listRunIngestionRecords(limit)
    ]);
    res.json({ ok: true, summary, records });
  } catch (error) {
    next(error);
  }
});

app.post("/admin/ingestion/runs/:runId/retry", async (req, res, next) => {
  try {
    const run = runManager.getRun(req.params.runId);
    if (!run) {
      res.status(404).json({ ok: false, error: "Run not found" });
      return;
    }
    await syncSingleRunToDb(run, true);
    const status = await getRunIngestion(run.id);
    res.json({ ok: true, status });
  } catch (error) {
    next(error);
  }
});

app.post("/admin/ingestion/backfill", async (req, res, next) => {
  try {
    const body = (req.body ?? {}) as Record<string, unknown>;
    const runIds = Array.isArray(body.runIds)
      ? body.runIds.filter((value): value is string => typeof value === "string" && value.trim().length > 0)
      : undefined;
    const force = body.force === true;
    const onlyFailed = body.onlyFailed !== false;
    await syncRunsToDb({ runIds, force, onlyFailed });
    const [summary, records] = await Promise.all([
      getRunIngestionSummary(),
      listRunIngestionRecords(100)
    ]);
    res.json({ ok: true, summary, records });
  } catch (error) {
    next(error);
  }
});

app.post("/reviews/companies/:id/resolve", async (req, res, next) => {
  try {
    const decision = String(req.body?.decision ?? "");
    if (decision !== "keep_old" && decision !== "keep_new") {
      res.status(400).json({ ok: false, error: "decision must be keep_old or keep_new" });
      return;
    }
    await resolveCompanyReview(req.params.id, decision);
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

app.post("/reviews/people/:id/resolve", async (req, res, next) => {
  try {
    const decision = String(req.body?.decision ?? "");
    if (decision !== "keep_old" && decision !== "keep_new") {
      res.status(400).json({ ok: false, error: "decision must be keep_old or keep_new" });
      return;
    }
    await resolvePeopleReview(req.params.id, decision);
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

app.use(
  (err: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
    console.error(err);
    const message = err instanceof Error ? err.message : "Internal Server Error";
    res.status(500).json({ ok: false, error: message });
  }
);
