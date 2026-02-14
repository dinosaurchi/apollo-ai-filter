import { env } from "./config";
import { PrismaClient } from "@prisma/client";
import { PrismaPg } from "@prisma/adapter-pg";
import { Pool } from "pg";

const pool = new Pool({
  connectionString: env.DATABASE_URL
});

const adapter = new PrismaPg(pool);

export const prisma = new PrismaClient({
  adapter
});

export async function checkDbConnection(): Promise<boolean> {
  await prisma.$queryRaw`SELECT 1`;
  return true;
}

export async function queryRows<T extends Record<string, unknown>>(sql: string, values: unknown[] = []): Promise<T[]> {
  return prisma.$queryRawUnsafe<T[]>(sql, ...values);
}

export async function execSql(sql: string, values: unknown[] = []): Promise<number> {
  return prisma.$executeRawUnsafe(sql, ...values);
}

export async function closeDb(): Promise<void> {
  await prisma.$disconnect();
  await pool.end();
}
