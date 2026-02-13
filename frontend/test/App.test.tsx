import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { App } from "../src/App";

describe("App", () => {
  it("renders main heading", async () => {
    class EventSourceMock {
      public addEventListener(): void {}
      public close(): void {}
    }
    vi.stubGlobal("EventSource", EventSourceMock);
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input: RequestInfo | URL) => {
      const url = input.toString();
      if (url.endsWith("/api/health")) {
        return {
          ok: true,
          json: async () => ({ ok: true })
        } as Response;
      }
      if (url.endsWith("/api/runs")) {
        return {
          ok: true,
          json: async () => ({ ok: true, runs: [] })
        } as Response;
      }
      return {
        ok: true,
        json: async () => ({ ok: true })
      } as Response;
    });

    render(<App />);

    expect(screen.getByRole("heading", { name: "Apollo Filter App" })).toBeInTheDocument();
    expect(await screen.findByText("Backend is healthy")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Run Monitor" })).toBeInTheDocument();
  });
});
