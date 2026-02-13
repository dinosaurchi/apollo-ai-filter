import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { App } from "../src/App";

describe("App", () => {
  it("renders main heading", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue({
      json: async () => ({ ok: true })
    } as Response);

    render(<App />);

    expect(screen.getByRole("heading", { name: "Apollo Filter App" })).toBeInTheDocument();
    expect(await screen.findByText("Backend is healthy")).toBeInTheDocument();
  });
});
