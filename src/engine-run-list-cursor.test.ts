import { describe, expect, test } from "vitest";

import { decodeEngineRunListCursor, encodeEngineRunListCursor } from "./engine-run-list-cursor.js";
import type { EngineRunListPosition } from "./engine-run-list-cursor.js";
import { cursorOfFields } from "./read-api.js";

const FAILED_RUN_POSITION: EngineRunListPosition = {
  state: "failed",
  task: null,
  indexPosition: { score: 1_731_000_000_000, runId: "run-42" },
};

describe("the listRuns cursor on the indexes", () => {
  test("decodes back to the index position it encodes", () => {
    const cursor = encodeEngineRunListCursor(FAILED_RUN_POSITION);

    expect(decodeEngineRunListCursor(cursor, { state: "failed", task: null })).toEqual(
      FAILED_RUN_POSITION.indexPosition,
    );
  });

  test("decodes back the position of one task", () => {
    const cursor = encodeEngineRunListCursor({ ...FAILED_RUN_POSITION, task: "send-email" });

    expect(decodeEngineRunListCursor(cursor, { state: "failed", task: "send-email" })).toEqual(
      FAILED_RUN_POSITION.indexPosition,
    );
  });

  test("is an opaque base64url string", () => {
    const cursor = encodeEngineRunListCursor(FAILED_RUN_POSITION);

    expect(cursor).toMatch(/^[A-Za-z0-9_-]+$/);
    expect(cursor).not.toContain("run-42");
  });

  test("is refused for another state than the one it pages through", () => {
    const cursor = encodeEngineRunListCursor(FAILED_RUN_POSITION);

    expect(() => decodeEngineRunListCursor(cursor, { state: "completed", task: null })).toThrow(
      'Invalid listRuns cursor: it pages through state "failed", not "completed"',
    );
  });

  test.each([
    [null, "send-email", 'it pages through every task, not task "send-email"'],
    ["send-email", null, 'it pages through task "send-email", not every task'],
    ["send-email", "build-report", 'it pages through task "send-email", not task "build-report"'],
  ])("is refused when it pages through %s and the call asks for %s", (cursorTask, calledTask, reason) => {
    const cursor = encodeEngineRunListCursor({ ...FAILED_RUN_POSITION, task: cursorTask });

    expect(() => decodeEngineRunListCursor(cursor, { state: "failed", task: calledTask })).toThrow(
      `Invalid listRuns cursor: ${reason}`,
    );
  });

  test.each([
    ["a string that is not a cursor", "not-a-cursor"],
    ["a number", 42],
    ["an empty string", ""],
    ["a 0.1.x cursor", cursorOfFields([1, "failed", 0, 50])],
    ["of another format version", cursorOfFields([1, "failed", null, 1, "run-42"])],
    ["missing its run id", cursorOfFields([2, "failed", null, 1])],
    ["of an unknown state", cursorOfFields([2, "sleeping", null, 1, "run-42"])],
    ["of a task that is not a string", cursorOfFields([2, "failed", 7, 1, "run-42"])],
    ["of a score that is a string", cursorOfFields([2, "failed", null, "1", "run-42"])],
    ["of a score that is not finite", cursorOfFields([2, "failed", null, null, "run-42"])],
    ["of an empty run id", cursorOfFields([2, "failed", null, 1, ""])],
    ["of a run id that is not a string", cursorOfFields([2, "failed", null, 1, 42])],
  ])("is refused when it is %s", (_description, cursor) => {
    expect(() => decodeEngineRunListCursor(cursor, { state: "failed", task: null })).toThrow(
      "Invalid listRuns cursor: pass back a nextCursor that listRuns returned",
    );
  });
});
