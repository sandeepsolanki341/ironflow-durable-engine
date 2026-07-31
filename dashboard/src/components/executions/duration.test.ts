import { describe, it, expect } from "vitest";
import { formatDuration } from "./duration";

describe("formatDuration", () => {
  const start = "2026-01-01T00:00:00.000Z";

  it("formats seconds for a short terminal execution", () => {
    expect(formatDuration(start, "2026-01-01T00:00:45.000Z", "COMPLETED")).toBe("45s");
  });

  it("formats minutes and seconds", () => {
    expect(formatDuration(start, "2026-01-01T00:03:20.000Z", "COMPLETED")).toBe("3m 20s");
  });

  it("formats a multi-day terminal execution", () => {
    expect(formatDuration(start, "2026-01-04T06:00:00.000Z", "FAILED_COMPENSATED")).toBe(
      "3d 6h",
    );
  });

  it("ticks live from start to now for a non-terminal execution", () => {
    const now = new Date("2026-01-01T00:00:10.000Z").getTime();
    expect(formatDuration(start, null, "RUNNING", now)).toBe("10s");
  });

  it("treats COMPENSATING as live (uses now, not start)", () => {
    const now = new Date("2026-01-01T00:02:00.000Z").getTime();
    expect(formatDuration(start, null, "COMPENSATING", now)).toBe("2m 0s");
  });

  it("never returns a negative duration", () => {
    const now = new Date("2025-12-31T23:00:00.000Z").getTime(); // before start
    expect(formatDuration(start, null, "RUNNING", now)).toBe("0s");
  });
});
