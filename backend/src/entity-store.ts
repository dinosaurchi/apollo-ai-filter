import { randomUUID } from "node:crypto";
import { execSql, queryRows } from "./db";
import { toSnakeCase } from "./csv";

type CompanyRecord = {
  run_id?: string;
  company_id?: string;
  company_name?: string;
  company_domain?: string;
  decision?: string;
  confidence?: string;
  evidence?: string;
  raw?: string;
};

type PersonRecord = Record<string, string>;
type IngestionState = "pending" | "ingesting" | "completed" | "failed";

const COMPANY_CSV_FIELDS = [
  "company_name_for_emails",
  "employees",
  "industry",
  "website",
  "company_linkedin_url",
  "facebook_url",
  "twitter_url",
  "company_city",
  "company_state",
  "company_country",
  "keywords",
  "company_phone",
  "total_funding",
  "latest_funding",
  "latest_funding_amount",
  "last_raised_at",
  "annual_revenue",
  "apollo_account_id",
  "sic_codes",
  "naics_codes",
  "short_description",
  "founded_year",
  "subsidiary_of",
  "stablecoin_prospect_9854",
  "qualify_account",
  "prerequisite_determine_research_guidelines",
  "prerequisite_research_target_company"
] as const;

type CompanyCsvField = typeof COMPANY_CSV_FIELDS[number];

const COMPANY_MUTABLE_FIELDS = new Set([
  "company_name",
  "company_domain"
]);

const PEOPLE_MUTABLE_FIELDS = new Set([
  "company_id",
  "full_name",
  "title",
  "email",
  "linkedin_url",
  "location"
]);

export type RunIngestionRecord = {
  run_id: string;
  status: IngestionState;
  attempt_count: number;
  last_error: string;
  last_attempt_at: string;
  completed_at: string;
  company_rows_written: number;
  people_rows_written: number;
  companies_ingested: boolean;
  people_ingested: boolean;
  updated_at: string;
};

function norm(value: string | undefined): string {
  return (value ?? "").trim();
}

function hasValue(value: string | undefined): boolean {
  return norm(value).length > 0;
}

function parseRawCompanyObject(raw: string | undefined): Record<string, string> {
  const trimmed = norm(raw);
  if (!trimmed) return {};
  try {
    const parsed = JSON.parse(trimmed) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    const out: Record<string, string> = {};
    for (const [key, value] of Object.entries(parsed as Record<string, unknown>)) {
      out[toSnakeCase(key)] = String(value ?? "").trim();
    }
    return out;
  } catch {
    return {};
  }
}

function companyFieldFromRaw(rawObj: Record<string, string>, key: string): string {
  return norm(rawObj[toSnakeCase(key)]);
}

export function extractCompanyCsvValues(row: CompanyRecord): Record<CompanyCsvField, string> {
  const rawObj = parseRawCompanyObject(row.raw);
  return {
    company_name_for_emails: companyFieldFromRaw(rawObj, "Company Name for Emails"),
    employees: companyFieldFromRaw(rawObj, "# Employees"),
    industry: companyFieldFromRaw(rawObj, "Industry"),
    website: companyFieldFromRaw(rawObj, "Website"),
    company_linkedin_url: companyFieldFromRaw(rawObj, "Company Linkedin Url"),
    facebook_url: companyFieldFromRaw(rawObj, "Facebook Url"),
    twitter_url: companyFieldFromRaw(rawObj, "Twitter Url"),
    company_city: companyFieldFromRaw(rawObj, "Company City"),
    company_state: companyFieldFromRaw(rawObj, "Company State"),
    company_country: companyFieldFromRaw(rawObj, "Company Country"),
    keywords: companyFieldFromRaw(rawObj, "Keywords"),
    company_phone: companyFieldFromRaw(rawObj, "Company Phone"),
    total_funding: companyFieldFromRaw(rawObj, "Total Funding"),
    latest_funding: companyFieldFromRaw(rawObj, "Latest Funding"),
    latest_funding_amount: companyFieldFromRaw(rawObj, "Latest Funding Amount"),
    last_raised_at: companyFieldFromRaw(rawObj, "Last Raised At"),
    annual_revenue: companyFieldFromRaw(rawObj, "Annual Revenue"),
    apollo_account_id: companyFieldFromRaw(rawObj, "Apollo Account Id"),
    sic_codes: companyFieldFromRaw(rawObj, "SIC Codes"),
    naics_codes: companyFieldFromRaw(rawObj, "NAICS Codes"),
    short_description: companyFieldFromRaw(rawObj, "Short Description"),
    founded_year: companyFieldFromRaw(rawObj, "Founded Year"),
    subsidiary_of: companyFieldFromRaw(rawObj, "Subsidiary of"),
    stablecoin_prospect_9854: companyFieldFromRaw(rawObj, "Stablecoin Prospect 9854"),
    qualify_account: companyFieldFromRaw(rawObj, "Qualify Account"),
    prerequisite_determine_research_guidelines: companyFieldFromRaw(rawObj, "Prerequisite: Determine Research Guidelines"),
    prerequisite_research_target_company: companyFieldFromRaw(rawObj, "Prerequisite: Research Target Company")
  };
}

function buildPersonId(row: PersonRecord): string {
  const id = norm(row.apollo_person_id);
  if (id) return id;
  const linkedin = norm(row.linkedin_url);
  if (linkedin) return `linkedin:${linkedin}`;
  const email = norm(row.email);
  if (email) return `email:${email}`;
  const name = norm(row.full_name);
  const companyId = norm(row.company_id);
  if (name || companyId) return `fallback:${companyId}:${name}`;
  return `generated:${randomUUID()}`;
}

export function assertAllowedFieldName(field: string, type: "company" | "people"): string {
  const normalized = field.trim();
  const allowed = type === "company" ? COMPANY_MUTABLE_FIELDS : PEOPLE_MUTABLE_FIELDS;
  if (!allowed.has(normalized)) {
    throw new Error(`Unsupported ${type} field for dynamic update: ${field}`);
  }
  return normalized;
}

async function ensureNoDuplicatePendingReview(
  table: "review_company" | "review_people",
  entityCol: "company_id" | "person_id",
  entityId: string,
  fieldName: string,
  oldValue: string,
  newValue: string
): Promise<boolean> {
  const existing = await queryRows<{ id: string }>(
    `SELECT id FROM ${table} WHERE ${entityCol} = $1 AND field_name = $2 AND old_value = $3 AND new_value = $4 AND status = 'pending' LIMIT 1`,
    [entityId, fieldName, oldValue, newValue]
  );
  return existing.length > 0;
}

export async function ingestCompanies(runId: string, rows: CompanyRecord[]): Promise<number> {
  let processed = 0;
  const fields: Array<keyof CompanyRecord> = [
    "company_name",
    "company_domain"
  ];
  for (const row of rows) {
    const companyId = norm(row.company_id);
    if (!companyId) continue;
    processed += 1;
    const csvValues = extractCompanyCsvValues(row);
    const existing = await queryRows<Record<string, unknown>>("SELECT * FROM companies WHERE company_id = $1", [companyId]);
    if (existing.length === 0) {
      await execSql(
        `INSERT INTO companies (
          company_id, company_name, company_domain,
          company_name_for_emails, employees, industry, website, company_linkedin_url, facebook_url, twitter_url,
          company_city, company_state, company_country, keywords, company_phone, total_funding, latest_funding,
          latest_funding_amount, last_raised_at, annual_revenue, apollo_account_id, sic_codes, naics_codes,
          short_description, founded_year, subsidiary_of, stablecoin_prospect_9854, qualify_account,
          prerequisite_determine_research_guidelines, prerequisite_research_target_company,
          source_run_id
        ) VALUES (
          $1,$2,$3,
          $4,$5,$6,$7,$8,$9,$10,
          $11,$12,$13,$14,$15,$16,$17,
          $18,$19,$20,$21,$22,$23,
          $24,$25,$26,$27,$28,
          $29,$30,
          $31
        )`,
        [
          companyId,
          norm(row.company_name),
          norm(row.company_domain),
          csvValues.company_name_for_emails,
          csvValues.employees,
          csvValues.industry,
          csvValues.website,
          csvValues.company_linkedin_url,
          csvValues.facebook_url,
          csvValues.twitter_url,
          csvValues.company_city,
          csvValues.company_state,
          csvValues.company_country,
          csvValues.keywords,
          csvValues.company_phone,
          csvValues.total_funding,
          csvValues.latest_funding,
          csvValues.latest_funding_amount,
          csvValues.last_raised_at,
          csvValues.annual_revenue,
          csvValues.apollo_account_id,
          csvValues.sic_codes,
          csvValues.naics_codes,
          csvValues.short_description,
          csvValues.founded_year,
          csvValues.subsidiary_of,
          csvValues.stablecoin_prospect_9854,
          csvValues.qualify_account,
          csvValues.prerequisite_determine_research_guidelines,
          csvValues.prerequisite_research_target_company,
          runId
        ]
      );
      continue;
    }
    const current = existing[0] as Record<string, string>;
    const updates: string[] = [];
    const values: string[] = [];
    for (const field of fields) {
      const oldValue = norm(current[field as string]);
      const newValue = norm(row[field] as string | undefined);
      if (!hasValue(newValue) || oldValue === newValue) continue;
      if (!hasValue(oldValue)) {
        updates.push(`${String(field)} = $${values.length + 1}`);
        values.push(newValue);
      } else {
        const hasPending = await ensureNoDuplicatePendingReview(
          "review_company",
          "company_id",
          companyId,
          String(field),
          oldValue,
          newValue
        );
        if (!hasPending) {
          await execSql(
            `INSERT INTO review_company (
              id, company_id, field_name, old_value, new_value, source_run_id, status
            ) VALUES ($1,$2,$3,$4,$5,$6,'pending')`,
            [randomUUID(), companyId, String(field), oldValue, newValue, runId]
          );
        }
      }
    }
    if (updates.length > 0) {
      await execSql(
        `UPDATE companies SET ${updates.join(", ")}, source_run_id = $${values.length + 1}, updated_at = NOW() WHERE company_id = $${values.length + 2}`,
        [...values, runId, companyId]
      );
    }
    const csvUpdates: string[] = [];
    const csvUpdateValues: string[] = [];
    for (const field of COMPANY_CSV_FIELDS) {
      const oldValue = norm(current[field]);
      const newValue = norm(csvValues[field]);
      if (!hasValue(newValue) || newValue === oldValue) continue;
      csvUpdates.push(`${field} = $${csvUpdateValues.length + 1}`);
      csvUpdateValues.push(newValue);
    }
    if (csvUpdates.length > 0) {
      await execSql(
        `UPDATE companies SET ${csvUpdates.join(", ")}, source_run_id = $${csvUpdateValues.length + 1}, updated_at = NOW() WHERE company_id = $${csvUpdateValues.length + 2}`,
        [...csvUpdateValues, runId, companyId]
      );
    }
  }
  return processed;
}

export async function ingestPeople(runId: string, rows: PersonRecord[]): Promise<number> {
  let processed = 0;
  const fields = ["company_id", "full_name", "title", "email", "linkedin_url", "location"] as const;
  for (const row of rows) {
    const personId = buildPersonId(row);
    processed += 1;
    const candidate: Record<string, string> = {
      company_id: norm(row.company_id) || norm(row.apollo_account_id) || norm(row.account_id),
      full_name: norm(row.full_name),
      title: norm(row.title),
      email: norm(row.email),
      linkedin_url: norm(row.linkedin_url),
      location: norm(row.location)
    };
    const existing = await queryRows<Record<string, unknown>>("SELECT * FROM people WHERE person_id = $1", [personId]);
    if (existing.length === 0) {
      await execSql(
        `INSERT INTO people (
          person_id, company_id, full_name, title, email, linkedin_url, location, source_run_id
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
        [
          personId,
          candidate.company_id,
          candidate.full_name,
          candidate.title,
          candidate.email,
          candidate.linkedin_url,
          candidate.location,
          runId
        ]
      );
      continue;
    }
    const current = existing[0] as Record<string, string>;
    const updates: string[] = [];
    const values: string[] = [];
    for (const field of fields) {
      const oldValue = norm(current[field]);
      const newValue = norm(candidate[field]);
      if (!hasValue(newValue) || oldValue === newValue) continue;
      if (!hasValue(oldValue)) {
        updates.push(`${field} = $${values.length + 1}`);
        values.push(newValue);
      } else {
        const hasPending = await ensureNoDuplicatePendingReview(
          "review_people",
          "person_id",
          personId,
          field,
          oldValue,
          newValue
        );
        if (!hasPending) {
          await execSql(
            `INSERT INTO review_people (
              id, person_id, field_name, old_value, new_value, source_run_id, status
            ) VALUES ($1,$2,$3,$4,$5,$6,'pending')`,
            [randomUUID(), personId, field, oldValue, newValue, runId]
          );
        }
      }
    }
    if (updates.length > 0) {
      await execSql(
        `UPDATE people SET ${updates.join(", ")}, source_run_id = $${values.length + 1}, updated_at = NOW() WHERE person_id = $${values.length + 2}`,
        [...values, runId, personId]
      );
    }
  }
  return processed;
}

export async function markRunIngestionInProgress(runId: string): Promise<void> {
  await execSql(
    `INSERT INTO run_ingestion (
      run_id, status, attempt_count, last_error, last_attempt_at, updated_at
    ) VALUES ($1, 'ingesting', 1, '', NOW(), NOW())
    ON CONFLICT (run_id) DO UPDATE SET
      status = 'ingesting',
      attempt_count = run_ingestion.attempt_count + 1,
      last_error = '',
      last_attempt_at = NOW(),
      updated_at = NOW()`,
    [runId]
  );
}

export async function markRunIngestionPending(runId: string, companiesDone: boolean, peopleDone: boolean): Promise<void> {
  await execSql(
    `INSERT INTO run_ingestion (
      run_id, status, companies_ingested, people_ingested, updated_at
    ) VALUES ($1,'pending',$2,$3,NOW())
     ON CONFLICT (run_id) DO UPDATE SET
       status = 'pending',
       companies_ingested = EXCLUDED.companies_ingested,
       people_ingested = EXCLUDED.people_ingested,
       updated_at = NOW()`,
    [runId, companiesDone, peopleDone]
  );
}

export async function markRunIngestionCompleted(
  runId: string,
  companiesDone: boolean,
  peopleDone: boolean,
  companyRows: number,
  peopleRows: number
): Promise<void> {
  await execSql(
    `INSERT INTO run_ingestion (
      run_id, status, companies_ingested, people_ingested, company_rows_written, people_rows_written, completed_at, updated_at
    ) VALUES ($1, 'completed', $2, $3, $4, $5, NOW(), NOW())
    ON CONFLICT (run_id) DO UPDATE SET
      status = 'completed',
      companies_ingested = EXCLUDED.companies_ingested,
      people_ingested = EXCLUDED.people_ingested,
      company_rows_written = EXCLUDED.company_rows_written,
      people_rows_written = EXCLUDED.people_rows_written,
      completed_at = NOW(),
      last_error = '',
      updated_at = NOW()`,
    [runId, companiesDone, peopleDone, companyRows, peopleRows]
  );
}

export async function markRunIngestionFailed(runId: string, errorMessage: string): Promise<void> {
  await execSql(
    `INSERT INTO run_ingestion (
      run_id, status, last_error, updated_at
    ) VALUES ($1, 'failed', $2, NOW())
    ON CONFLICT (run_id) DO UPDATE SET
      status = 'failed',
      last_error = $2,
      updated_at = NOW()`,
    [runId, errorMessage]
  );
}

export async function getRunIngestion(runId: string): Promise<RunIngestionRecord | null> {
  const result = await queryRows<Record<string, unknown>>(
    `SELECT
      run_id,
      status,
      attempt_count,
      last_error,
      COALESCE(last_attempt_at::text, '') AS last_attempt_at,
      COALESCE(completed_at::text, '') AS completed_at,
      company_rows_written,
      people_rows_written,
      companies_ingested,
      people_ingested,
      updated_at::text AS updated_at
    FROM run_ingestion
    WHERE run_id = $1`,
    [runId]
  );
  const row = result[0] as Record<string, unknown> | undefined;
  if (!row) return null;
  return {
    run_id: String(row.run_id ?? ""),
    status: (String(row.status ?? "pending") as IngestionState),
    attempt_count: Number(row.attempt_count ?? 0),
    last_error: String(row.last_error ?? ""),
    last_attempt_at: String(row.last_attempt_at ?? ""),
    completed_at: String(row.completed_at ?? ""),
    company_rows_written: Number(row.company_rows_written ?? 0),
    people_rows_written: Number(row.people_rows_written ?? 0),
    companies_ingested: Boolean(row.companies_ingested),
    people_ingested: Boolean(row.people_ingested),
    updated_at: String(row.updated_at ?? "")
  };
}

export async function listRunIngestionRecords(limit = 200): Promise<RunIngestionRecord[]> {
  const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.min(limit, 1000) : 200;
  const result = await queryRows<Record<string, unknown>>(
    `SELECT
      run_id,
      status,
      attempt_count,
      last_error,
      COALESCE(last_attempt_at::text, '') AS last_attempt_at,
      COALESCE(completed_at::text, '') AS completed_at,
      company_rows_written,
      people_rows_written,
      companies_ingested,
      people_ingested,
      updated_at::text AS updated_at
    FROM run_ingestion
    ORDER BY updated_at DESC
    LIMIT $1`,
    [safeLimit]
  );
  return result.map((row) => ({
    run_id: String(row.run_id ?? ""),
    status: (String(row.status ?? "pending") as IngestionState),
    attempt_count: Number(row.attempt_count ?? 0),
    last_error: String(row.last_error ?? ""),
    last_attempt_at: String(row.last_attempt_at ?? ""),
    completed_at: String(row.completed_at ?? ""),
    company_rows_written: Number(row.company_rows_written ?? 0),
    people_rows_written: Number(row.people_rows_written ?? 0),
    companies_ingested: Boolean(row.companies_ingested),
    people_ingested: Boolean(row.people_ingested),
    updated_at: String(row.updated_at ?? "")
  }));
}

export async function getRunIngestionSummary(): Promise<Record<string, number>> {
  const result = await queryRows<Record<string, unknown>>(
    `SELECT status, COUNT(*)::int AS count
     FROM run_ingestion
     GROUP BY status`
  );
  const summary: Record<string, number> = {
    pending: 0,
    ingesting: 0,
    completed: 0,
    failed: 0,
    total: 0
  };
  for (const row of result) {
    const status = String(row.status ?? "");
    const count = Number(row.count ?? 0);
    summary[status] = count;
    summary.total += count;
  }
  return summary;
}

export async function listCompaniesFromDb(): Promise<Array<Record<string, string>>> {
  const result = await queryRows<Record<string, unknown>>(
    `SELECT
        c.*,
        COALESCE(pc.people_count, 0)::int AS people_count
     FROM companies c
     LEFT JOIN (
       SELECT company_id, COUNT(*)::int AS people_count
       FROM people
       GROUP BY company_id
     ) pc ON pc.company_id = c.company_id
     ORDER BY c.updated_at DESC, c.company_name ASC`
  );
  return result.map((row) => {
    const allValues = Object.fromEntries(Object.entries(row).map(([key, value]) => [key, String(value ?? "")]));
    return {
      ...allValues,
      run_id: String(row.source_run_id ?? ""),
      evidence: "",
      raw: ""
    };
  });
}

export async function getPersonByIdFromDb(personId: string): Promise<Record<string, string> | null> {
  const result = await queryRows<Record<string, unknown>>(
    `SELECT
        p.person_id,
        p.company_id,
        COALESCE(c.company_name, '') AS company_name,
        COALESCE(c.company_domain, '') AS company_domain,
        p.full_name,
        p.title,
        p.email,
        p.linkedin_url,
        p.location,
        p.source_run_id
     FROM people p
     LEFT JOIN companies c ON c.company_id = p.company_id
     WHERE p.person_id = $1
     LIMIT 1`,
    [personId]
  );
  const row = result[0];
  if (!row) return null;
  return {
    run_id: String(row.source_run_id ?? ""),
    person_id: String(row.person_id ?? ""),
    company_id: String(row.company_id ?? ""),
    company_name: String(row.company_name ?? ""),
    company_domain: String(row.company_domain ?? ""),
    full_name: String(row.full_name ?? ""),
    title: String(row.title ?? ""),
    email: String(row.email ?? ""),
    linkedin_url: String(row.linkedin_url ?? ""),
    location: String(row.location ?? "")
  };
}

export async function updatePersonFromEnrichment(
  personId: string,
  patch: Partial<Record<"full_name" | "title" | "email" | "linkedin_url" | "location", string>>
): Promise<void> {
  const updates: string[] = [];
  const values: string[] = [];
  const fields: Array<keyof typeof patch> = ["full_name", "title", "email", "linkedin_url", "location"];
  for (const field of fields) {
    const value = norm(patch[field]);
    if (!value) continue;
    updates.push(`${field} = $${values.length + 1}`);
    values.push(value);
  }
  if (updates.length === 0) return;
  await execSql(
    `UPDATE people
     SET ${updates.join(", ")}, updated_at = NOW()
     WHERE person_id = $${values.length + 1}`,
    [...values, personId]
  );
}

export async function listPeopleFromDb(): Promise<Array<Record<string, string>>> {
  const result = await queryRows<Record<string, unknown>>(
    `SELECT
        p.person_id,
        p.company_id,
        COALESCE(c.company_name, '') AS company_name,
        p.full_name,
        p.title,
        p.email,
        p.linkedin_url,
        p.location,
        p.source_run_id
     FROM people p
     LEFT JOIN companies c ON c.company_id = p.company_id
     ORDER BY p.updated_at DESC, p.full_name ASC`
  );
  return result.map((row) => ({
    run_id: String(row.source_run_id ?? ""),
    person_id: String(row.person_id ?? ""),
    company_id: String(row.company_id ?? ""),
    company_name: String(row.company_name ?? ""),
    full_name: String(row.full_name ?? ""),
    title: String(row.title ?? ""),
    email: String(row.email ?? ""),
    linkedin_url: String(row.linkedin_url ?? ""),
    location: String(row.location ?? "")
  }));
}

export async function listPeopleByCompanyFromDb(companyId: string): Promise<Array<Record<string, string>>> {
  const result = await queryRows<Record<string, unknown>>(
    `SELECT
      p.person_id,
      p.company_id,
      COALESCE(c.company_name, '') AS company_name,
      p.full_name,
      p.title,
      p.email,
      p.linkedin_url,
      p.location,
      p.source_run_id
     FROM people p
     LEFT JOIN companies c ON c.company_id = p.company_id
     WHERE p.company_id = $1
     ORDER BY p.updated_at DESC, p.full_name ASC`,
    [companyId]
  );
  return result.map((row) => ({
    run_id: String(row.source_run_id ?? ""),
    person_id: String(row.person_id ?? ""),
    company_id: String(row.company_id ?? ""),
    company_name: String(row.company_name ?? ""),
    full_name: String(row.full_name ?? ""),
    title: String(row.title ?? ""),
    email: String(row.email ?? ""),
    linkedin_url: String(row.linkedin_url ?? ""),
    location: String(row.location ?? "")
  }));
}

export async function listCompanyReviews(): Promise<Array<Record<string, string>>> {
  const result = await queryRows<Record<string, unknown>>(
    `SELECT id, company_id, field_name, old_value, new_value, source_run_id, status, COALESCE(resolution, '') AS resolution, created_at
     FROM review_company
     WHERE status = 'pending' AND field_name IN ('company_name', 'company_domain')
     ORDER BY created_at DESC`
  );
  return result.map((row) => Object.fromEntries(Object.entries(row).map(([k, v]) => [k, String(v ?? "")])));
}

export async function listPeopleReviews(): Promise<Array<Record<string, string>>> {
  const result = await queryRows<Record<string, unknown>>(
    `SELECT id, person_id, field_name, old_value, new_value, source_run_id, status, COALESCE(resolution, '') AS resolution, created_at
     FROM review_people
     WHERE status = 'pending' AND field_name IN ('company_id', 'full_name', 'title', 'email', 'linkedin_url', 'location')
     ORDER BY created_at DESC`
  );
  return result.map((row) => Object.fromEntries(Object.entries(row).map(([k, v]) => [k, String(v ?? "")])));
}

export async function resolveCompanyReview(id: string, decision: "keep_old" | "keep_new"): Promise<void> {
  const result = await queryRows<Record<string, unknown>>("SELECT * FROM review_company WHERE id = $1 AND status = 'pending' LIMIT 1", [id]);
  if (result.length === 0) return;
  const review = result[0] as Record<string, string>;
  if (decision === "keep_new") {
    const fieldName = assertAllowedFieldName(String(review.field_name ?? ""), "company");
    await execSql(
      `UPDATE companies SET ${fieldName} = $1, updated_at = NOW() WHERE company_id = $2`,
      [review.new_value, review.company_id]
    );
  }
  await execSql(
    `UPDATE review_company SET status = 'resolved', resolution = $1, resolved_at = NOW() WHERE id = $2`,
    [decision, id]
  );
}

export async function resolvePeopleReview(id: string, decision: "keep_old" | "keep_new"): Promise<void> {
  const result = await queryRows<Record<string, unknown>>("SELECT * FROM review_people WHERE id = $1 AND status = 'pending' LIMIT 1", [id]);
  if (result.length === 0) return;
  const review = result[0] as Record<string, string>;
  if (decision === "keep_new") {
    const fieldName = assertAllowedFieldName(String(review.field_name ?? ""), "people");
    await execSql(
      `UPDATE people SET ${fieldName} = $1, updated_at = NOW() WHERE person_id = $2`,
      [review.new_value, review.person_id]
    );
  }
  await execSql(
    `UPDATE review_people SET status = 'resolved', resolution = $1, resolved_at = NOW() WHERE id = $2`,
    [decision, id]
  );
}
