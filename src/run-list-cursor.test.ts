import { describe, expect, test } from "vitest";

import { decodeRunListCursor, encodeRunListCursor } from "./run-list-cursor.js";
import type { RunListCursorContent } from "./run-list-cursor.js";
import { cursorOfFields } from "./run-list-cursor.js";

const FAILED_RUN_PLACE = { score: 1_731_000_000_000, task: "send-email", runId: "run-42" };

const FORWARD_FROM_A_FAILED_RUN: RunListCursorContent = {
  state: "failed",
  listedTask: null,
  direction: "forward",
  place: FAILED_RUN_PLACE,
};

const INVALID_CURSOR_MESSAGE =
  "Invalid listRuns cursor: pass back a nextCursor or previousCursor that listRuns returned";

describe("the listRuns cursor on the indexes", () => {
  test.each([
    ["forward from a place", "forward", FAILED_RUN_PLACE],
    ["backward from a place", "backward", FAILED_RUN_PLACE],
    ["backward from the end of the list", "backward", undefined],
  ] as const)("decodes back to the boundary it encodes, %s", (_description, direction, place) => {
    const cursor = encodeRunListCursor({ ...FORWARD_FROM_A_FAILED_RUN, direction, place });

    expect(decodeRunListCursor(cursor, { state: "failed", listedTask: null })).toEqual({ direction, place });
  });

  test("decodes back the boundary of one task", () => {
    const cursor = encodeRunListCursor({ ...FORWARD_FROM_A_FAILED_RUN, listedTask: "send-email" });

    expect(decodeRunListCursor(cursor, { state: "failed", listedTask: "send-email" })).toEqual({
      direction: "forward",
      place: FAILED_RUN_PLACE,
    });
  });

  test("is the format version 3, its direction and place, then its state and listed task", () => {
    const cursor = encodeRunListCursor({ ...FORWARD_FROM_A_FAILED_RUN, direction: "backward" });

    expect(cursor).toBe(cursorOfFields([3, "backward", [1_731_000_000_000, "send-email", "run-42"], "failed", null]));
  });

  test("reads a cursor of the format version 2 as a forward cursor from its place", () => {
    const cursor = cursorOfFields([2, "failed", "send-email", 1_731_000_000_000, "send-email", "run-42"]);

    expect(decodeRunListCursor(cursor, { state: "failed", listedTask: "send-email" })).toEqual({
      direction: "forward",
      place: FAILED_RUN_PLACE,
    });
  });

  test("is an opaque base64url string", () => {
    const cursor = encodeRunListCursor(FORWARD_FROM_A_FAILED_RUN);

    expect(cursor).toMatch(/^[A-Za-z0-9_-]+$/);
    expect(cursor).not.toContain("run-42");
  });

  test.each([
    ["of the format version 3", encodeRunListCursor({ ...FORWARD_FROM_A_FAILED_RUN, direction: "backward" })],
    ["of the format version 2", cursorOfFields([2, "failed", null, 1, "send-email", "run-42"])],
  ])("is refused for another state than the one it pages through, %s", (_description, cursor) => {
    expect(() => decodeRunListCursor(cursor, { state: "completed", listedTask: null })).toThrow(
      'Invalid listRuns cursor: it pages through state "failed", not "completed"',
    );
  });

  test.each([
    [null, "send-email", 'it pages through every task, not task "send-email"'],
    ["send-email", null, 'it pages through task "send-email", not every task'],
    ["send-email", "build-report", 'it pages through task "send-email", not task "build-report"'],
  ])("is refused when it pages through %s and the call asks for %s", (cursorTask, calledTask, reason) => {
    const cursor = encodeRunListCursor({ ...FORWARD_FROM_A_FAILED_RUN, listedTask: cursorTask });

    expect(() => decodeRunListCursor(cursor, { state: "failed", listedTask: calledTask })).toThrow(
      `Invalid listRuns cursor: ${reason}`,
    );
  });

  test.each([
    ["a string that is not a cursor", "not-a-cursor"],
    ["a number", 42],
    ["an empty string", ""],
    ["a 0.1.x cursor", cursorOfFields([1, "failed", 0, 50])],
    ["of another format version", cursorOfFields([1, "failed", null, 1, "send-email", "run-42"])],
    ["of a format version to come", cursorOfFields([4, "forward", [1, "send-email", "run-42"], "failed", null])],
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
    [
      "of version 3 with an unknown direction",
      cursorOfFields([3, "sideways", [1, "send-email", "run-42"], "failed", null]),
    ],
    ["of version 3 forward from no place", cursorOfFields([3, "forward", null, "failed", null])],
    ["of version 3 with a place that is not a list", cursorOfFields([3, "backward", "run-42", "failed", null])],
    ["of version 3 with a place missing its task", cursorOfFields([3, "backward", [1, "run-42"], "failed", null])],
    [
      "of version 3 with a score that is a string",
      cursorOfFields([3, "backward", ["1", "send-email", "run-42"], "failed", null]),
    ],
    ["of version 3 with an unknown state", cursorOfFields([3, "backward", null, "sleeping", null])],
    ["of version 3 missing its listed task", cursorOfFields([3, "backward", null, "failed"])],
  ])("is refused when it is %s", (_description, cursor) => {
    expect(() => decodeRunListCursor(cursor, { state: "failed", listedTask: null })).toThrow(INVALID_CURSOR_MESSAGE);
  });

  test.each([
    ["of the format version 2", cursorOfFields([2, "failed", "send-email", 1, "build-report", "run-42"])],
    ["of the format version 3", cursorOfFields([3, "forward", [1, "build-report", "run-42"], "failed", "send-email"])],
  ])("is refused when it lists one task and its place is a run of another, %s", (_description, cursor) => {
    expect(() => decodeRunListCursor(cursor, { state: "failed", listedTask: "send-email" })).toThrow(
      INVALID_CURSOR_MESSAGE,
    );
  });
});
