import { describe, expect, test } from "vitest";

import { decodeEngineRunListCursor, encodeEngineRunListCursor } from "./engine-run-list-cursor.js";
import type { EngineRunListPosition } from "./engine-run-list-cursor.js";
import { cursorOfFields } from "./read-api.js";

const FAILED_RUN_POSITION: EngineRunListPosition = {
  state: "failed",
  listedTask: null,
  lastListedRun: { score: 1_731_000_000_000, task: "send-email", runId: "run-42" },
};

describe("the listRuns cursor on the indexes", () => {
  test("decodes back to the last listed run it encodes", () => {
    const cursor = encodeEngineRunListCursor(FAILED_RUN_POSITION);

    expect(decodeEngineRunListCursor(cursor, { state: "failed", listedTask: null })).toEqual(
      FAILED_RUN_POSITION.lastListedRun,
    );
  });

  test("decodes back the position of one task", () => {
    const cursor = encodeEngineRunListCursor({ ...FAILED_RUN_POSITION, listedTask: "send-email" });

    expect(decodeEngineRunListCursor(cursor, { state: "failed", listedTask: "send-email" })).toEqual(
      FAILED_RUN_POSITION.lastListedRun,
    );
  });

  test("is an opaque base64url string", () => {
    const cursor = encodeEngineRunListCursor(FAILED_RUN_POSITION);

    expect(cursor).toMatch(/^[A-Za-z0-9_-]+$/);
    expect(cursor).not.toContain("run-42");
  });

  test("is refused for another state than the one it pages through", () => {
    const cursor = encodeEngineRunListCursor(FAILED_RUN_POSITION);

    expect(() => decodeEngineRunListCursor(cursor, { state: "completed", listedTask: null })).toThrow(
      'Invalid listRuns cursor: it pages through state "failed", not "completed"',
    );
  });

  test.each([
    [null, "send-email", 'it pages through every task, not task "send-email"'],
    ["send-email", null, 'it pages through task "send-email", not every task'],
    ["send-email", "build-report", 'it pages through task "send-email", not task "build-report"'],
  ])("is refused when it pages through %s and the call asks for %s", (cursorTask, calledTask, reason) => {
    const cursor = encodeEngineRunListCursor({ ...FAILED_RUN_POSITION, listedTask: cursorTask });

    expect(() => decodeEngineRunListCursor(cursor, { state: "failed", listedTask: calledTask })).toThrow(
      `Invalid listRuns cursor: ${reason}`,
    );
  });

  test.each([
    ["a string that is not a cursor", "not-a-cursor"],
    ["a number", 42],
    ["an empty string", ""],
    ["a 0.1.x cursor", cursorOfFields([1, "failed", 0, 50])],
    ["of another format version", cursorOfFields([1, "failed", null, 1, "send-email", "run-42"])],
    ["missing the task of its last run", cursorOfFields([2, "failed", null, 1, "run-42"])],
    ["of an unknown state", cursorOfFields([2, "sleeping", null, 1, "send-email", "run-42"])],
    ["of a listed task that is not a string", cursorOfFields([2, "failed", 7, 1, "send-email", "run-42"])],
    ["of a score that is a string", cursorOfFields([2, "failed", null, "1", "send-email", "run-42"])],
    ["of a score that is not finite", cursorOfFields([2, "failed", null, null, "send-email", "run-42"])],
    ["of an empty last run task", cursorOfFields([2, "failed", null, 1, "", "run-42"])],
    ["of a last run task that is not a string", cursorOfFields([2, "failed", null, 1, null, "run-42"])],
    ["of a last run task that cannot be encoded", cursorOfFields([2, "failed", null, 1, "\ud800", "run-42"])],
    ["of an empty run id", cursorOfFields([2, "failed", null, 1, "send-email", ""])],
    ["of a run id that is not a string", cursorOfFields([2, "failed", null, 1, "send-email", 42])],
  ])("is refused when it is %s", (_description, cursor) => {
    expect(() => decodeEngineRunListCursor(cursor, { state: "failed", listedTask: null })).toThrow(
      "Invalid listRuns cursor: pass back a nextCursor that listRuns returned",
    );
  });

  test("is refused when it lists one task and its last run is of another", () => {
    const cursor = cursorOfFields([2, "failed", "send-email", 1, "build-report", "run-42"]);

    expect(() => decodeEngineRunListCursor(cursor, { state: "failed", listedTask: "send-email" })).toThrow(
      "Invalid listRuns cursor: pass back a nextCursor that listRuns returned",
    );
  });
});
