import { expect, test } from "vitest";

import { unfinishedRunCountIn } from "./unfinished-run-count.js";

const NO_RUN = {
  delayed: 0,
  pending: 0,
  waitingForConcurrencySlot: 0,
  executing: 0,
  suspended: 0,
  backingOff: 0,
  completed: 0,
  failed: 0,
};

test("the unfinished run count sums every state but completed and failed, across tasks", () => {
  expect(
    unfinishedRunCountIn({
      "send-email": { ...NO_RUN, delayed: 1, pending: 2, completed: 900, failed: 40 },
      "build-report": { ...NO_RUN, waitingForConcurrencySlot: 3, executing: 4, suspended: 5, backingOff: 6 },
    }),
  ).toBe(21);
});

test("a queue holding only completed and failed runs has no unfinished run", () => {
  expect(unfinishedRunCountIn({ "send-email": { ...NO_RUN, completed: 1000, failed: 1000 } })).toBe(0);
});
