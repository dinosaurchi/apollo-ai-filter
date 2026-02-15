import { afterEach, describe, expect, it, vi } from "vitest";
import { enrichPersonFromApollo } from "../src/apollo-enrich";

describe("enrichPersonFromApollo", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("returns normalized fields from Apollo payload", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue({
      ok: true,
      text: async () =>
        JSON.stringify({
          person: {
            name: "Jane Doe",
            title: "Director",
            email: "jane@example.com",
            linkedin_url: "https://linkedin.com/in/jane",
            city: "Austin",
            state: "TX",
            country: "US"
          }
        })
    } as Response);

    const result = await enrichPersonFromApollo(
      {
        person_id: "apollo-123",
        full_name: "",
        email: "",
        linkedin_url: "",
        company_domain: "example.com"
      },
      { APOLLO_API_KEY: "test-key" } as NodeJS.ProcessEnv
    );

    expect(fetchMock).toHaveBeenCalled();
    expect(result).toEqual({
      full_name: "Jane Doe",
      title: "Director",
      email: "jane@example.com",
      linkedin_url: "https://linkedin.com/in/jane",
      location: "Austin, TX, US"
    });
  });
});
