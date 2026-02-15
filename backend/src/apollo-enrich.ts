const DEFAULT_APOLLO_BASE_URL = "https://api.apollo.io";
const LOCKED_EMAIL_VALUES = new Set(["email_not_unlocked@domain.com", "email_not_unlocked"]);

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

function normalizeEmail(value: unknown): string {
  const email = normalize(value).toLowerCase();
  if (!email) return "";
  if (LOCKED_EMAIL_VALUES.has(email)) return "";
  return email;
}

function hasRealApolloId(personId: string): boolean {
  return Boolean(personId && !personId.startsWith("linkedin:") && !personId.startsWith("email:") && !personId.startsWith("fallback:"));
}

function firstEmailFromArray(value: unknown): string {
  if (!Array.isArray(value)) return "";
  for (const item of value) {
    if (typeof item === "string") {
      const email = normalizeEmail(item);
      if (email) return email;
      continue;
    }
    if (item && typeof item === "object") {
      const candidate = normalizeEmail((item as Record<string, unknown>).email);
      if (candidate) return candidate;
    }
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

function resolvePeopleMatchPath(env: NodeJS.ProcessEnv): string {
  const params = new URLSearchParams();
  params.set("reveal_personal_emails", env.APOLLO_REVEAL_PERSONAL_EMAILS === "false" ? "false" : "true");
  params.set("reveal_phone_number", env.APOLLO_REVEAL_PHONE_NUMBER === "true" ? "true" : "false");
  if (env.APOLLO_RUN_WATERFALL_EMAIL === "true") {
    params.set("run_waterfall_email", "true");
  }
  const webhookUrl = normalize(env.APOLLO_ENRICH_WEBHOOK_URL);
  if (webhookUrl) {
    params.set("webhook_url", webhookUrl);
  }
  return `/api/v1/people/match?${params.toString()}`;
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
    email: firstNonEmpty([
      normalizeEmail(rawPerson.email),
      normalizeEmail(rawPerson.work_email),
      normalizeEmail(rawPerson.personal_email),
      firstEmailFromArray(rawPerson.emails),
      firstEmailFromArray(rawPerson.work_emails),
      firstEmailFromArray(rawPerson.personal_emails)
    ]),
    linkedin_url: firstNonEmpty([
      rawPerson.linkedin_url,
      rawPerson.linkedin_profile_url,
      rawPerson.linkedin
    ]),
    location
  };
}

export async function enrichPersonFromApollo(
  person: PersonEnrichInput,
  env: NodeJS.ProcessEnv = process.env
): Promise<PersonEnrichResult> {
  const personId = normalize(person.person_id);
  const email = normalizeEmail(person.email);
  const linkedinUrl = normalize(person.linkedin_url);
  const fullName = normalize(person.full_name);
  const companyDomain = normalize(person.company_domain);

  let payload: Record<string, unknown> | null = null;
  const matchBody: Record<string, unknown> = {};
  if (hasRealApolloId(personId)) matchBody.id = personId;
  if (email) matchBody.email = email;
  if (linkedinUrl) matchBody.linkedin_url = linkedinUrl;
  if (fullName) matchBody.name = fullName;
  if (companyDomain) matchBody.organization_domain = companyDomain;
  if (Object.keys(matchBody).length === 0) {
    throw new Error("Cannot enrich person: missing email/linkedin/name/domain identifiers");
  }
  payload = await fetchApollo(
    resolvePeopleMatchPath(env),
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(matchBody)
    },
    env
  );

  if (!payload && hasRealApolloId(personId)) {
    payload = await fetchApollo(`/api/v1/people/${encodeURIComponent(personId)}`, { method: "GET" }, env);
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
