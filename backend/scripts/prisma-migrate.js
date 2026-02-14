"use strict";

const { execFileSync } = require("node:child_process");
const { PrismaClient } = require("@prisma/client");

const BASELINE_MIGRATION = process.env.PRISMA_BASELINE_MIGRATION || "20260214120000_init";

function runPrisma(args) {
  execFileSync("pnpm", ["exec", "prisma", ...args], {
    stdio: "inherit",
    env: process.env
  });
}

async function requiresBaseline(prisma) {
  const migrationTable = await prisma.$queryRaw`
    SELECT to_regclass('public._prisma_migrations')::text AS table_name
  `;
  const migrationTableExists = migrationTable[0] && migrationTable[0].table_name !== null;
  if (migrationTableExists) return false;

  const tableCountRows = await prisma.$queryRaw`
    SELECT COUNT(*)::int AS count
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_type = 'BASE TABLE'
      AND table_name <> '_prisma_migrations'
  `;

  return Number(tableCountRows[0]?.count ?? 0) > 0;
}

async function main() {
  const prisma = new PrismaClient();
  try {
    const baselineNeeded = await requiresBaseline(prisma);
    if (baselineNeeded) {
      console.log(`Existing schema detected without Prisma migration history. Marking ${BASELINE_MIGRATION} as applied.`);
      runPrisma(["migrate", "resolve", "--applied", BASELINE_MIGRATION]);
    }
  } finally {
    await prisma.$disconnect();
  }

  runPrisma(["migrate", "deploy"]);
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Prisma migration bootstrap failed: ${message}`);
  process.exit(1);
});
