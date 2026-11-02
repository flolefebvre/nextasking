import { describe, expect, test } from "vitest";

import { StepMisuseError } from "./errors.js";
import { failureReasonFor } from "./failed-attempts.js";

class StepMisuseErrorFromAnotherCopy extends Error {
  override readonly name = "StepMisuseError";
}

describe("failureReasonFor", () => {
  test("fails a run at once on a step misuse, whatever retries are left", () => {
    expect(failureReasonFor(new StepMisuseError("repeated id"), 3)).toBe("non-retriable");
    expect(failureReasonFor(new StepMisuseErrorFromAnotherCopy("repeated id"), 3)).toBe("non-retriable");
  });

  test("leaves a run with retries left to retry after an ordinary error", () => {
    expect(failureReasonFor(new Error("upstream unavailable"), 3)).toBeUndefined();
  });
});
