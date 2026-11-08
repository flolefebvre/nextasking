import { describe, expect, test } from "vitest";

import { isResumingAfterSleep } from "./redis-step-store.js";

function savedSleep(wakeAt: number, attempt: number): string {
  return JSON.stringify({ kind: "sleep", wakeAt, attempt });
}

describe("resuming after a sleep", () => {
  test("is told apart from a retry by the attempt that saved the sleep", () => {
    const savedSteps = { "load-invoice": '["inv-1"]', wait: savedSleep(Date.now() - 1000, 2) };

    expect(isResumingAfterSleep(savedSteps, 2)).toBe(true);
    expect(isResumingAfterSleep(savedSteps, 3)).toBe(false);
    expect(isResumingAfterSleep({}, 1)).toBe(false);
  });
});
