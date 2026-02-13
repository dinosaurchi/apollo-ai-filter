import type { PeopleQueryV1, PeopleQueryV1Normalized } from "../people_query/types";
import { assertValidPeopleQueryV1 } from "../people_query/validate";

const DEFAULT_APOLLO_BASE_URL = "https://api.apollo.io";
const PEOPLE_SEARCH_PATH = "/api/v1/mixed_people/api_search";

const ARRAY_KEYS: Array<keyof PeopleQueryV1Normalized> = [
  "organization_ids",
  "q_organization_domains_list",
  "person_titles",
  "person_seniorities",
  "person_locations",
  "q_keywords",
];

export type PeopleSearchResponse = {
  people?: Array<Record<string, unknown>>;
  pagination?: {
    page?: number;
    per_page?: number;
    total_pages?: number;
    [key: string]: unknown;
  };
  [key: string]: unknown;
};

function resolveApiKey(env: NodeJS.ProcessEnv): string {
  const canonical = env.APOLLO_API_KEY;
  if (canonical && canonical.trim().length > 0) return canonical;

  const misspelled = env.APPOLO_API_KEY;
  if (misspelled && misspelled.trim().length > 0) {
    throw new Error("Missing APOLLO_API_KEY. Found APPOLO_API_KEY; rename it to APOLLO_API_KEY.");
  }

  throw new Error("Missing APOLLO_API_KEY environment variable");
}

function resolveBaseUrl(env: NodeJS.ProcessEnv): string {
  return (env.APOLLO_API_BASE_URL ?? DEFAULT_APOLLO_BASE_URL).replace(/\/+$/, "");
}

export function buildPeopleSearchQueryParams(query: PeopleQueryV1): URLSearchParams {
  const normalized = assertValidPeopleQueryV1(query);
  const params = new URLSearchParams();

  for (const key of ARRAY_KEYS) {
    const values = normalized[key];
    if (!Array.isArray(values)) continue;
    for (const value of values) {
      params.append(`${String(key)}[]`, value);
    }
  }
  params.append("page", String(normalized.page));
  params.append("per_page", String(normalized.per_page));
  return params;
}

export async function callPeopleSearch(
  query: PeopleQueryV1 | PeopleQueryV1Normalized,
  env: NodeJS.ProcessEnv = process.env,
): Promise<PeopleSearchResponse> {
  const apiKey = resolveApiKey(env);
  const params = buildPeopleSearchQueryParams(query);
  const url = `${resolveBaseUrl(env)}${PEOPLE_SEARCH_PATH}?${params.toString()}`;

  let response: Response;
  try {
    response = await fetch(url, {
      method: "POST",
      headers: {
        accept: "application/json",
        "x-api-key": apiKey,
      },
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`People API Search request failed: ${message}`);
  }

  const raw = await response.text();
  if (!response.ok) {
    throw new Error(`People API Search HTTP ${response.status}: ${raw}`);
  }

  try {
    return JSON.parse(raw) as PeopleSearchResponse;
  } catch {
    throw new Error("People API Search response is not valid JSON");
  }
}
