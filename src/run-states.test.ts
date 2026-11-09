import { expect, test } from "vitest";

import { isRunState, RUN_STATES } from "./run-states.js";

test("a run state is one of the eight state names, and nothing else is", () => {
  expect(RUN_STATES.every(isRunState)).toBe(true);
  expect([undefined, "", "unknown", "Pending", 1].some(isRunState)).toBe(false);
});
