import { describe, expect, test } from "vitest";

import { violationOfCapturedOutputLine } from "./captured-process.js";

const NEXT_START_WARNING_LINE = "(node:66076) Warning: warned inside next start";

const NEXT_START_UNHANDLED_REJECTION_LINE = " ⨯ unhandledRejection:  Error: rejected inside next start";

const FIXTURE_WORKER_ERROR_LINE = '{"level":"error","message":"worker error","err":{}}';

describe("the output lines of a captured Next server, as next start 16.0.10 writes them", () => {
  test("a process warning is a process warning violation named by its name and message", () => {
    const violation = violationOfCapturedOutputLine(NEXT_START_WARNING_LINE);

    expect(violation).toMatchObject({ invariant: "process warning", message: "Warning: warned inside next start" });
  });

  test("Next's unhandledRejection line is an unhandled rejection violation named by the reason's message", () => {
    const violation = violationOfCapturedOutputLine(NEXT_START_UNHANDLED_REJECTION_LINE);

    expect(violation).toMatchObject({ invariant: "unhandled rejection", message: "rejected inside next start" });
  });

  test("a worker error line of the fixture's logger is a worker error violation named by the whole line", () => {
    const violation = violationOfCapturedOutputLine(FIXTURE_WORKER_ERROR_LINE);

    expect(violation).toEqual({ invariant: "worker error", message: FIXTURE_WORKER_ERROR_LINE });
  });

  test("the other lines are no violation", () => {
    const otherLines = [
      "(Use `node --trace-warnings ...` to show where the warning was created)",
      "Error: rejected inside next start",
      "    at Timeout._onTimeout (../../nextasking-77/provoke-in-next.cjs:3:18)",
      '{"level":"error","message":"connection error","err":{}}',
      " ✓ Ready in 346ms",
    ];

    const violations = otherLines.map(violationOfCapturedOutputLine);

    expect(violations).toEqual(otherLines.map(() => undefined));
  });
});
