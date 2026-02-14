import { describe, expect, it } from "vitest";
import { assertAllowedFieldName } from "../src/entity-store";

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
