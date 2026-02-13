import express from "express";
import { checkDbConnection } from "./db";
import multer from "multer";
import { env } from "./config";
import { RunManager } from "./run-manager";

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
  const run = runManager.getRun(req.params.id);
  if (!run) {
    res.status(404).json({ ok: false, error: "Run not found" });
    return;
  }
  res.json({ ok: true, run });
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
    const companies = await runManager.getAllCompanies();
    res.json({ ok: true, companies });
  } catch (error) {
    next(error);
  }
});

app.get("/companies/:runId/:companyId/people", async (req, res, next) => {
  try {
    const people = await runManager.getRunPeople(req.params.runId);
    const filtered = people.filter((p) => (p.company_id ?? "") === req.params.companyId);
    res.json({ ok: true, people: filtered });
  } catch (error) {
    next(error);
  }
});

app.get("/people", async (_req, res, next) => {
  try {
    const people = await runManager.getAllPeople();
    res.json({ ok: true, people });
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
