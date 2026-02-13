import { Pool } from "pg";
import { env } from "./config";

export const pool = new Pool({
  connectionString: env.DATABASE_URL
});

export async function checkDbConnection(): Promise<boolean> {
  const client = await pool.connect();
  try {
    await client.query("SELECT 1");
    return true;
  } finally {
    client.release();
  }
}
