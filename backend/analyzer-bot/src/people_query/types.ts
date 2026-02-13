export type PeopleQueryV1 = {
  page?: number;
  per_page?: number;
  organization_ids?: string[];
  q_organization_domains_list?: string[];
  person_titles?: string[];
  person_seniorities?: Array<"ic" | "manager" | "director" | "vp" | "c_suite" | "head">;
  person_locations?: string[];
  q_keywords?: string[];
};

export type PeopleQueryV1Normalized = Omit<PeopleQueryV1, "page" | "per_page"> & {
  page: number;
  per_page: number;
};

export type OrgLite = {
  id?: string;
  organization_id?: string;
  apollo_id?: string;
  domain?: string;
  primary_domain?: string;
  name?: string;
  [key: string]: unknown;
};

export type PeopleQueryBuildResult = {
  query: PeopleQueryV1;
  warnings: string[];
};
