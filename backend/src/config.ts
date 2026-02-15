import dotenv from "dotenv";
import path from "node:path";
import { z } from "zod";

dotenv.config();

const EnvSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  PORT: z.coerce.number().int().positive().default(3000),
  DATABASE_URL: z
    .string()
    .min(1, "DATABASE_URL is required")
    .default("postgres://app:app@localhost:5432/app"),
  OUTPUT_ROOT: z
    .string()
    .min(1)
    .default(path.resolve(process.cwd(), "output")),
  ANALYZE_SCRIPT_DIR: z
    .string()
    .min(1)
    .default(path.resolve(process.cwd(), "analyzer-bot")),
  OPENCODE_SERVER_URL: z.string().min(1).default("http://127.0.0.1:3000"),
  ANALYZER_NODE_OPTIONS: z.string().min(1).default("--max-old-space-size=4096"),
  AI_CONFIG_GENERATOR_MODEL: z.string().min(1).default("opencode/gpt-5-nano"),
  AI_CONFIG_GENERATOR_MAX_ATTEMPTS: z.coerce.number().int().positive().max(12).default(10)
});

export const env = EnvSchema.parse(process.env);
