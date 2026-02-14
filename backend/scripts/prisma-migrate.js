"use strict";

const { execFileSync } = require("node:child_process");
const { Client } = require("pg");

const BASELINE_MIGRATION = process.env.PRISMA_BASELINE_MIGRATION || "20260214120000_init";

function runPrisma(args) {
  execFileSync("pnpm", ["exec", "prisma", ...args], {
    stdio: "inherit",
    env: process.env
  });
}

async function requiresBaseline(client) {
  const migrationTable = await client.query("SELECT to_regclass('public._prisma_migrations')::text AS table_name");
  const migrationTableExists = migrationTable.rows[0] && migrationTable.rows[0].table_name !== null;
  if (migrationTableExists) return false;

  const tableCountRows = await client.query(`
    SELECT COUNT(*)::int AS count
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_type = 'BASE TABLE'
      AND table_name <> '_prisma_migrations'
  `);

  return Number(tableCountRows.rows[0]?.count ?? 0) > 0;
}

async function main() {
  const client = new Client({
    connectionString: process.env.DATABASE_URL
  });
  try {
    await client.connect();
    const baselineNeeded = await requiresBaseline(client);
    if (baselineNeeded) {
      console.log(`Existing schema detected without Prisma migration history. Marking ${BASELINE_MIGRATION} as applied.`);
      runPrisma(["migrate", "resolve", "--applied", BASELINE_MIGRATION]);
    }
  } finally {
    await client.end();
  }

  runPrisma(["migrate", "deploy"]);
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Prisma migration bootstrap failed: ${message}`);
  process.exit(1);
});
