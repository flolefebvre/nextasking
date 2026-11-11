import { expect, test } from "vitest";

import { isOrderGroup, isRunState, ORDER_GROUPS, orderGroupOf, RUN_STATES } from "./run-states.js";

test("a run state is one of the eight state names, and nothing else is", () => {
  expect(RUN_STATES.every(isRunState)).toBe(true);
  expect([undefined, "", "unknown", "Pending", 1].some(isRunState)).toBe(false);
});

test("the eight states fall into four order groups, in the sequence a list of several groups follows", () => {
  expect(ORDER_GROUPS.map(({ name }) => name)).toEqual(["executing", "queued", "waitingForATime", "ended"]);
  expect(ORDER_GROUPS.flatMap(({ states }) => states).sort()).toEqual([...RUN_STATES].sort());
});

test("each state belongs to the order group that lists it", () => {
  expect(Object.fromEntries(RUN_STATES.map((state) => [state, orderGroupOf(state)]))).toEqual({
    delayed: "waitingForATime",
    pending: "queued",
    waitingForConcurrencySlot: "queued",
    executing: "executing",
    suspended: "waitingForATime",
    backingOff: "waitingForATime",
    completed: "ended",
    failed: "ended",
  });
});

test("an order group is one of the four group names, and nothing else is", () => {
  expect(["executing", "queued", "waitingForATime", "ended"].every(isOrderGroup)).toBe(true);
  expect([undefined, "", "pending", "Ended", 1].some(isOrderGroup)).toBe(false);
});
