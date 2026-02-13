import { createSchemaValidator } from "../ai/schema_utils";
import type { PeopleQueryV1, PeopleQueryV1Normalized } from "./types";

const DEFAULT_PAGE = 1;
const DEFAULT_PER_PAGE = 100;

const validatePeopleQueryInner = createSchemaValidator<PeopleQueryV1>(
  "contracts/people.query.v1.schema.json",
);
const TITLE_ALL_SENTINELS = new Set(["all", "any", "*", "everyone"]);

function normalizeQuery(query: PeopleQueryV1): PeopleQueryV1Normalized {
  return {
    page: query.page ?? DEFAULT_PAGE,
    per_page: query.per_page ?? DEFAULT_PER_PAGE,
    organization_ids: query.organization_ids,
    q_organization_domains_list: query.q_organization_domains_list,
    person_titles: query.person_titles,
    person_seniorities: query.person_seniorities,
    person_locations: query.person_locations,
    q_keywords: query.q_keywords,
  };
}

function hasOrganizationRestriction(query: PeopleQueryV1): boolean {
  const ids = query.organization_ids ?? [];
  const domains = query.q_organization_domains_list ?? [];
  return ids.length > 0 || domains.length > 0;
}

export function validatePeopleQueryV1(obj: unknown):
  | { ok: true; value: PeopleQueryV1Normalized }
  | { ok: false; errors: string[] } {
  const base = validatePeopleQueryInner(obj);
  if (!base.ok) return base;

  if (!hasOrganizationRestriction(base.value)) {
    return { ok: false, errors: ["no company restriction provided"] };
  }

  if ((base.value.person_titles ?? []).some((value) => TITLE_ALL_SENTINELS.has(value.trim().toLowerCase()))) {
    return {
      ok: false,
      errors: ["person_titles cannot contain All/any/*/everyone; omit person_titles for no title filter"],
    };
  }

  return { ok: true, value: normalizeQuery(base.value) };
}

export function assertValidPeopleQueryV1(obj: unknown): PeopleQueryV1Normalized {
  const result = validatePeopleQueryV1(obj);
  if (!result.ok) {
    throw new Error(`Invalid PeopleQueryV1 input: ${result.errors.join("; ")}`);
  }
  return result.value;
}
