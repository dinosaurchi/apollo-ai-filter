CREATE TABLE IF NOT EXISTS companies (
  company_id TEXT PRIMARY KEY,
  company_name TEXT NOT NULL DEFAULT '',
  company_domain TEXT NOT NULL DEFAULT '',
  company_name_for_emails TEXT NOT NULL DEFAULT '',
  employees TEXT NOT NULL DEFAULT '',
  industry TEXT NOT NULL DEFAULT '',
  website TEXT NOT NULL DEFAULT '',
  company_linkedin_url TEXT NOT NULL DEFAULT '',
  facebook_url TEXT NOT NULL DEFAULT '',
  twitter_url TEXT NOT NULL DEFAULT '',
  company_city TEXT NOT NULL DEFAULT '',
  company_state TEXT NOT NULL DEFAULT '',
  company_country TEXT NOT NULL DEFAULT '',
  keywords TEXT NOT NULL DEFAULT '',
  company_phone TEXT NOT NULL DEFAULT '',
  total_funding TEXT NOT NULL DEFAULT '',
  latest_funding TEXT NOT NULL DEFAULT '',
  latest_funding_amount TEXT NOT NULL DEFAULT '',
  last_raised_at TEXT NOT NULL DEFAULT '',
  annual_revenue TEXT NOT NULL DEFAULT '',
  apollo_account_id TEXT NOT NULL DEFAULT '',
  sic_codes TEXT NOT NULL DEFAULT '',
  naics_codes TEXT NOT NULL DEFAULT '',
  short_description TEXT NOT NULL DEFAULT '',
  founded_year TEXT NOT NULL DEFAULT '',
  subsidiary_of TEXT NOT NULL DEFAULT '',
  stablecoin_prospect_9854 TEXT NOT NULL DEFAULT '',
  qualify_account TEXT NOT NULL DEFAULT '',
  prerequisite_determine_research_guidelines TEXT NOT NULL DEFAULT '',
  prerequisite_research_target_company TEXT NOT NULL DEFAULT '',
  source_run_id TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS people (
  person_id TEXT PRIMARY KEY,
  company_id TEXT NOT NULL DEFAULT '',
  full_name TEXT NOT NULL DEFAULT '',
  title TEXT NOT NULL DEFAULT '',
  email TEXT NOT NULL DEFAULT '',
  linkedin_url TEXT NOT NULL DEFAULT '',
  location TEXT NOT NULL DEFAULT '',
  source_run_id TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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

ALTER TABLE run_ingestion
ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'pending';

ALTER TABLE run_ingestion
ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE run_ingestion
ADD COLUMN IF NOT EXISTS last_error TEXT NOT NULL DEFAULT '';

ALTER TABLE run_ingestion
ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ;

ALTER TABLE run_ingestion
ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;

ALTER TABLE run_ingestion
ADD COLUMN IF NOT EXISTS company_rows_written INTEGER NOT NULL DEFAULT 0;

ALTER TABLE run_ingestion
ADD COLUMN IF NOT EXISTS people_rows_written INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_review_company_pending ON review_company (status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_review_people_pending ON review_people (status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_people_company_id ON people (company_id);
CREATE INDEX IF NOT EXISTS idx_run_ingestion_status ON run_ingestion (status, updated_at DESC);
