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

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, requestInit] = fetchMock.mock.calls[0] ?? [];
    expect(String(url)).toContain("/api/v1/people/match?");
    expect(String(url)).toContain("reveal_personal_emails=true");
    expect(requestInit).toMatchObject({ method: "POST" });
    expect(result).toEqual({
      full_name: "Jane Doe",
      title: "Director",
      email: "jane@example.com",
      linkedin_url: "https://linkedin.com/in/jane",
      location: "Austin, TX, US"
    });
  });

  it("drops locked placeholder emails and extracts linkedin fallback fields", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue({
      ok: true,
      text: async () =>
        JSON.stringify({
          person: {
            name: "John Doe",
            title: "Manager",
            email: "email_not_unlocked@domain.com",
            work_email: "email_not_unlocked@domain.com",
            personal_emails: [{ email: "john.doe@example.com" }],
            linkedin_profile_url: "https://linkedin.com/in/john-doe",
            location: "Seattle, WA"
          }
        })
    } as Response);

    const result = await enrichPersonFromApollo(
      {
        person_id: "apollo-456",
        full_name: "",
        email: "",
        linkedin_url: "",
        company_domain: "example.com"
      },
      { APOLLO_API_KEY: "test-key" } as NodeJS.ProcessEnv
    );

    expect(result).toEqual({
      full_name: "John Doe",
      title: "Manager",
      email: "john.doe@example.com",
      linkedin_url: "https://linkedin.com/in/john-doe",
      location: "Seattle, WA"
    });
  });
});
