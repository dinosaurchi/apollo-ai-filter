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
  ANALYZER_IMAGE: z.string().min(1).default("apollo-filter-ai-sandbox:latest"),
  ANALYZER_DOCKERFILE: z
    .string()
    .min(1)
    .default(path.resolve(process.cwd(), "dockerfiles/ai.Dockerfile")),
  OPENCODE_AUTH_FILE: z
    .string()
    .min(1)
    .default(path.resolve(process.env.HOME ?? "", ".local/share/opencode/auth.json")),
  OPENCODE_CONFIG_DIR: z
    .string()
    .min(1)
    .default(path.resolve(process.cwd(), ".sandbox/opencode/config")),
  OPENCODE_DATA_DIR: z
    .string()
    .min(1)
    .default(path.resolve(process.cwd(), ".sandbox/opencode/data"))
});

export const env = EnvSchema.parse(process.env);
