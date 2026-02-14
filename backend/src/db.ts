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

export async function initDbSchema(): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS companies (
        company_id TEXT PRIMARY KEY,
        company_name TEXT NOT NULL DEFAULT '',
        company_domain TEXT NOT NULL DEFAULT '',
        decision TEXT NOT NULL DEFAULT '',
        confidence TEXT NOT NULL DEFAULT '',
        evidence TEXT NOT NULL DEFAULT '',
        raw TEXT NOT NULL DEFAULT '',
        source_run_id TEXT NOT NULL DEFAULT '',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    await client.query(`
      CREATE TABLE IF NOT EXISTS people (
        person_id TEXT PRIMARY KEY,
        company_id TEXT NOT NULL DEFAULT '',
        full_name TEXT NOT NULL DEFAULT '',
        title TEXT NOT NULL DEFAULT '',
        email TEXT NOT NULL DEFAULT '',
        linkedin_url TEXT NOT NULL DEFAULT '',
        location TEXT NOT NULL DEFAULT '',
        raw TEXT NOT NULL DEFAULT '',
        source_run_id TEXT NOT NULL DEFAULT '',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    await client.query(`
      CREATE TABLE IF NOT EXISTS review_company (
        id TEXT PRIMARY KEY,
        company_id TEXT NOT NULL,
        field_name TEXT NOT NULL,
        old_value TEXT NOT NULL DEFAULT '',
        new_value TEXT NOT NULL DEFAULT '',
        source_run_id TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'pending',
        resolution TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        resolved_at TIMESTAMPTZ
      );
    `);
    await client.query(`
      CREATE TABLE IF NOT EXISTS review_people (
        id TEXT PRIMARY KEY,
        person_id TEXT NOT NULL,
        field_name TEXT NOT NULL,
        old_value TEXT NOT NULL DEFAULT '',
        new_value TEXT NOT NULL DEFAULT '',
        source_run_id TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'pending',
        resolution TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        resolved_at TIMESTAMPTZ
      );
    `);
    await client.query(`
      CREATE TABLE IF NOT EXISTS run_ingestion (
        run_id TEXT PRIMARY KEY,
        status TEXT NOT NULL DEFAULT 'pending',
        attempt_count INTEGER NOT NULL DEFAULT 0,
        last_error TEXT NOT NULL DEFAULT '',
        last_attempt_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        company_rows_written INTEGER NOT NULL DEFAULT 0,
        people_rows_written INTEGER NOT NULL DEFAULT 0,
        companies_ingested BOOLEAN NOT NULL DEFAULT FALSE,
        people_ingested BOOLEAN NOT NULL DEFAULT FALSE,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    await client.query(`
      ALTER TABLE run_ingestion
      ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'pending';
    `);
    await client.query(`
      ALTER TABLE run_ingestion
      ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0;
    `);
    await client.query(`
      ALTER TABLE run_ingestion
      ADD COLUMN IF NOT EXISTS last_error TEXT NOT NULL DEFAULT '';
    `);
    await client.query(`
      ALTER TABLE run_ingestion
      ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ;
    `);
    await client.query(`
      ALTER TABLE run_ingestion
      ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
    `);
    await client.query(`
      ALTER TABLE run_ingestion
      ADD COLUMN IF NOT EXISTS company_rows_written INTEGER NOT NULL DEFAULT 0;
    `);
    await client.query(`
      ALTER TABLE run_ingestion
      ADD COLUMN IF NOT EXISTS people_rows_written INTEGER NOT NULL DEFAULT 0;
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_review_company_pending
      ON review_company (status, created_at DESC);
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_review_people_pending
      ON review_people (status, created_at DESC);
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_people_company_id
      ON people (company_id);
    `);
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_run_ingestion_status
      ON run_ingestion (status, updated_at DESC);
    `);
  } finally {
    client.release();
  }
}
