import express from "express";
import { checkDbConnection } from "./db";

export const app = express();

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

app.use(
  (err: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
    console.error(err);
    res.status(500).json({ ok: false, error: "Internal Server Error" });
  }
);
