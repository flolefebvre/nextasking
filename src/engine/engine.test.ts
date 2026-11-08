import { expect, test } from "vitest";

import type { ImplementedSettleOutcome } from "./engine.js";

const ERROR_RECORD = { attempt: 1, name: "Error", message: "boom", stack: "", failedAt: "" };

test("the engine settles complete, yield and handBack, and refuses the other outcomes at compile time", () => {
  const implementedOutcomes: ImplementedSettleOutcome[] = [
    { kind: "complete" },
    { kind: "yield" },
    { kind: "handBack", error: ERROR_RECORD, failureNotice: undefined },
  ];
  // @ts-expect-error a retry is not settled by this engine yet
  const retry: ImplementedSettleOutcome = { kind: "retry", error: ERROR_RECORD, backoffInMilliseconds: 1000 };
  // @ts-expect-error a suspension is not settled by this engine yet
  const suspension: ImplementedSettleOutcome = { kind: "suspend", resumesAt: 0 };

  expect([...implementedOutcomes, retry, suspension]).toHaveLength(5);
});
