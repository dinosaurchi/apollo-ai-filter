const DEFAULT_APOLLO_BASE_URL = "https://api.apollo.io";

export type PersonEnrichInput = {
  person_id: string;
  full_name: string;
  email: string;
  linkedin_url: string;
  company_domain: string;
};

export type PersonEnrichResult = {
  full_name: string;
  title: string;
  email: string;
  linkedin_url: string;
  location: string;
};

function normalize(value: unknown): string {
  return String(value ?? "").trim();
}

function firstNonEmpty(values: unknown[]): string {
  for (const value of values) {
    const next = normalize(value);
    if (next) return next;
  }
  return "";
}

function resolveApiKey(env: NodeJS.ProcessEnv): string {
  const apiKey = normalize(env.APOLLO_API_KEY);
  if (!apiKey) throw new Error("Missing APOLLO_API_KEY environment variable");
  return apiKey;
}

function resolveBaseUrl(env: NodeJS.ProcessEnv): string {
  return (env.APOLLO_API_BASE_URL ?? DEFAULT_APOLLO_BASE_URL).replace(/\/+$/, "");
}

async function fetchApollo(
  path: string,
  init: RequestInit,
  env: NodeJS.ProcessEnv
): Promise<Record<string, unknown> | null> {
  const response = await fetch(`${resolveBaseUrl(env)}${path}`, {
    ...init,
    headers: {
      accept: "application/json",
      "x-api-key": resolveApiKey(env),
      ...(init.headers ?? {})
    }
  });
  const text = await response.text();
  if (!response.ok) {
    return null;
  }
  try {
    return JSON.parse(text) as Record<string, unknown>;
  } catch {
    return null;
  }
}

function shapeEnrichedPerson(payload: Record<string, unknown>): PersonEnrichResult {
  const rawPerson = (payload.person && typeof payload.person === "object")
    ? (payload.person as Record<string, unknown>)
    : payload;

  const location = firstNonEmpty([
    rawPerson.location,
    [normalize(rawPerson.city), normalize(rawPerson.state), normalize(rawPerson.country)]
      .filter((part) => part.length > 0)
      .join(", ")
  ]);

  return {
    full_name: firstNonEmpty([
      rawPerson.name,
      [normalize(rawPerson.first_name), normalize(rawPerson.last_name)]
        .filter((part) => part.length > 0)
        .join(" ")
    ]),
    title: firstNonEmpty([rawPerson.title, rawPerson.headline]),
    email: firstNonEmpty([rawPerson.email, rawPerson.work_email]),
    linkedin_url: firstNonEmpty([rawPerson.linkedin_url]),
    location
  };
}

export async function enrichPersonFromApollo(
  person: PersonEnrichInput,
  env: NodeJS.ProcessEnv = process.env
): Promise<PersonEnrichResult> {
  const personId = normalize(person.person_id);
  const email = normalize(person.email);
  const linkedinUrl = normalize(person.linkedin_url);
  const fullName = normalize(person.full_name);
  const companyDomain = normalize(person.company_domain);

  let payload: Record<string, unknown> | null = null;

  // Apollo person ids in this app are usually raw Apollo IDs.
  if (personId && !personId.startsWith("linkedin:") && !personId.startsWith("email:") && !personId.startsWith("fallback:")) {
    payload = await fetchApollo(`/api/v1/people/${encodeURIComponent(personId)}`, { method: "GET" }, env);
  }

  if (!payload) {
    const matchBody: Record<string, unknown> = {};
    if (email) matchBody.email = email;
    if (linkedinUrl) matchBody.linkedin_url = linkedinUrl;
    if (fullName) matchBody.name = fullName;
    if (companyDomain) matchBody.organization_domain = companyDomain;
    if (Object.keys(matchBody).length === 0) {
      throw new Error("Cannot enrich person: missing email/linkedin/name/domain identifiers");
    }
    payload = await fetchApollo(
      "/api/v1/people/match",
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(matchBody)
      },
      env
    );
  }

  if (!payload) {
    throw new Error("Apollo enrichment failed or returned non-JSON response");
  }

  const enriched = shapeEnrichedPerson(payload);
  if (!enriched.full_name && !enriched.title && !enriched.email && !enriched.linkedin_url && !enriched.location) {
    throw new Error("Apollo enrichment returned no usable contact fields");
  }
  return enriched;
}
