import { env } from "./config";
import { PrismaClient } from "@prisma/client";

export const prisma = new PrismaClient({
  datasources: {
    db: {
      url: env.DATABASE_URL
    }
  }
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
