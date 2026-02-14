import { describe, expect, it } from "vitest";
import { assertAllowedFieldName, extractCompanyCsvValues } from "../src/entity-store";

describe("assertAllowedFieldName", () => {
  it("allows known company fields", () => {
    expect(assertAllowedFieldName("company_name", "company")).toBe("company_name");
    expect(assertAllowedFieldName("company_domain", "company")).toBe("company_domain");
  });

  it("allows known people fields", () => {
    expect(assertAllowedFieldName("full_name", "people")).toBe("full_name");
    expect(assertAllowedFieldName("linkedin_url", "people")).toBe("linkedin_url");
  });

  it("rejects unknown and run-scoped fields", () => {
    expect(() => assertAllowedFieldName("status", "company")).toThrow(/Unsupported company field/);
    expect(() => assertAllowedFieldName("decision", "company")).toThrow(/Unsupported company field/);
    expect(() => assertAllowedFieldName("confidence", "company")).toThrow(/Unsupported company field/);
    expect(() => assertAllowedFieldName("evidence", "company")).toThrow(/Unsupported company field/);
    expect(() => assertAllowedFieldName("raw", "company")).toThrow(/Unsupported company field/);
    expect(() => assertAllowedFieldName("raw", "people")).toThrow(/Unsupported people field/);
    expect(() => assertAllowedFieldName("drop table people", "people")).toThrow(/Unsupported people field/);
  });
});

describe("extractCompanyCsvValues", () => {
  it("extracts known fields from raw csv payload", () => {
    const values = extractCompanyCsvValues({
      raw: JSON.stringify({
        "Company Name for Emails": "Acme Co",
        "# Employees": "55",
        "Apollo Account Id": "abc-123",
        "Prerequisite: Research Target Company": "done"
      })
    });
    expect(values.company_name_for_emails).toBe("Acme Co");
    expect(values.employees).toBe("55");
    expect(values.apollo_account_id).toBe("abc-123");
    expect(values.prerequisite_research_target_company).toBe("done");
  });

  it("returns empty values when raw payload is missing or invalid", () => {
    const empty = extractCompanyCsvValues({ raw: "" });
    expect(empty.website).toBe("");
    const invalid = extractCompanyCsvValues({ raw: "{bad-json" });
    expect(invalid.company_country).toBe("");
  });
});
