import { describe, expect, test } from "vitest";

import { decodeRunListCursor, encodeRunListCursor } from "./run-list-cursor.js";
import type { ListedIndexes, RunListCursorContent } from "./run-list-cursor.js";
import { cursorOfFields } from "./run-list-cursor.js";

const FAILED_RUN_PLACE = {
  orderGroup: "ended",
  score: 1_731_000_000_000,
  task: "send-email",
  runId: "run-42",
} as const;

const FAILED_OF_EVERY_TASK: ListedIndexes = { listedStates: ["failed"], listedTasks: null };

const FORWARD_FROM_A_FAILED_RUN: RunListCursorContent = {
  ...FAILED_OF_EVERY_TASK,
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

    expect(decodeRunListCursor(cursor, FAILED_OF_EVERY_TASK)).toEqual({ direction, place });
  });

  test("decodes back the boundary of one task", () => {
    const cursor = encodeRunListCursor({ ...FORWARD_FROM_A_FAILED_RUN, listedTasks: ["send-email"] });

    expect(decodeRunListCursor(cursor, { listedStates: ["failed"], listedTasks: ["send-email"] })).toEqual({
      direction: "forward",
      place: FAILED_RUN_PLACE,
    });
  });

  test("decodes back the boundary of every state, from a place in any order group", () => {
    const place = { orderGroup: "queued", score: 48_213, task: "send-email", runId: "run-42" } as const;
    const cursor = encodeRunListCursor({ listedStates: null, listedTasks: null, direction: "backward", place });

    expect(decodeRunListCursor(cursor, { listedStates: null, listedTasks: null })).toEqual({
      direction: "backward",
      place,
    });
  });

  test("is the format version 3, its direction and place in its order group, then its listed states and tasks", () => {
    const cursor = encodeRunListCursor({ ...FORWARD_FROM_A_FAILED_RUN, direction: "backward" });

    expect(cursor).toBe(
      cursorOfFields([3, "backward", ["ended", 1_731_000_000_000, "send-email", "run-42"], ["failed"], null]),
    );
  });

  test("reads a cursor of the format version 2 as a forward cursor from its place, in the order group of its state", () => {
    const cursor = cursorOfFields([2, "failed", "send-email", 1_731_000_000_000, "send-email", "run-42"]);

    expect(decodeRunListCursor(cursor, { listedStates: ["failed"], listedTasks: ["send-email"] })).toEqual({
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
    expect(() => decodeRunListCursor(cursor, { listedStates: ["completed"], listedTasks: null })).toThrow(
      'Invalid listRuns cursor: it pages through state "failed", not "completed"',
    );
  });

  test.each([
    [["completed", "failed"], ["failed"], 'states "completed" and "failed", not "failed"'],
    [["failed"], ["completed", "failed"], 'state "failed", not "completed" and "failed"'],
    [null, ["completed", "failed"], 'every state, not "completed" and "failed"'],
    [["failed"], null, 'state "failed", not every state'],
    [["pending", "suspended", "failed"], null, 'states "pending", "suspended" and "failed", not every state'],
  ] as const)(
    "is refused when it pages through the states %j and the call lists %j, naming both",
    (cursorStates, calledStates, reason) => {
      const cursor = encodeRunListCursor({ ...FORWARD_FROM_A_FAILED_RUN, listedStates: cursorStates });

      expect(() => decodeRunListCursor(cursor, { listedStates: calledStates, listedTasks: null })).toThrow(
        `Invalid listRuns cursor: it pages through ${reason}`,
      );
    },
  );

  test.each([
    [null, ["send-email"], 'it pages through every task, not task "send-email"'],
    [["send-email"], null, 'it pages through task "send-email", not every task'],
    [["send-email"], ["build-report"], 'it pages through task "send-email", not task "build-report"'],
    [
      ["build-report", "send-email"],
      ["send-email"],
      'it pages through tasks "build-report" and "send-email", not task "send-email"',
    ],
    [
      null,
      ["build-report", "send-email", "resize-image"],
      'it pages through every task, not tasks "build-report", "send-email" and "resize-image"',
    ],
    [["build-report", "send-email"], null, 'it pages through tasks "build-report" and "send-email", not every task'],
  ] as const)("is refused when it pages through %j and the call asks for %j", (cursorTasks, calledTasks, reason) => {
    const cursor = encodeRunListCursor({ ...FORWARD_FROM_A_FAILED_RUN, listedTasks: cursorTasks });

    expect(() => decodeRunListCursor(cursor, { listedStates: ["failed"], listedTasks: calledTasks })).toThrow(
      `Invalid listRuns cursor: ${reason}`,
    );
  });

  test.each([
    ["a string that is not a cursor", "not-a-cursor"],
    ["a number", 42],
    ["an empty string", ""],
    ["a 0.1.x cursor", cursorOfFields([1, "failed", 0, 50])],
    ["of another format version", cursorOfFields([1, "failed", null, 1, "send-email", "run-42"])],
    [
      "of a format version to come",
      cursorOfFields([4, "forward", ["ended", 1, "send-email", "run-42"], ["failed"], null]),
    ],
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
      cursorOfFields([3, "sideways", ["ended", 1, "send-email", "run-42"], ["failed"], null]),
    ],
    ["of version 3 forward from no place", cursorOfFields([3, "forward", null, ["failed"], null])],
    ["of version 3 with a place that is not a list", cursorOfFields([3, "backward", "run-42", ["failed"], null])],
    [
      "of version 3 with a place missing its order group",
      cursorOfFields([3, "backward", [1, "send-email", "run-42"], ["failed"], null]),
    ],
    [
      "of version 3 with a place in an unknown order group",
      cursorOfFields([3, "backward", ["finished", 1, "send-email", "run-42"], ["failed"], null]),
    ],
    [
      "of version 3 with a place in an order group none of its states is in",
      cursorOfFields([3, "backward", ["queued", 1, "send-email", "run-42"], ["failed"], null]),
    ],
    [
      "of version 3 with a score that is a string",
      cursorOfFields([3, "backward", ["ended", "1", "send-email", "run-42"], ["failed"], null]),
    ],
    ["of version 3 with an unknown state", cursorOfFields([3, "backward", null, ["sleeping"], null])],
    ["of version 3 with a state that is not listed", cursorOfFields([3, "backward", null, "failed", null])],
    ["of version 3 with an empty list of states", cursorOfFields([3, "backward", null, [], null])],
    ["of version 3 with an empty list of tasks", cursorOfFields([3, "backward", null, ["failed"], []])],
    ["of version 3 with a task that is not a string", cursorOfFields([3, "backward", null, ["failed"], [7]])],
    ["of version 3 missing its listed tasks", cursorOfFields([3, "backward", null, ["failed"]])],
  ])("is refused when it is %s", (_description, cursor) => {
    expect(() => decodeRunListCursor(cursor, FAILED_OF_EVERY_TASK)).toThrow(INVALID_CURSOR_MESSAGE);
  });

  test.each([
    ["of the format version 2", cursorOfFields([2, "failed", "send-email", 1, "build-report", "run-42"])],
    [
      "of the format version 3",
      cursorOfFields([3, "forward", ["ended", 1, "build-report", "run-42"], ["failed"], ["send-email"]]),
    ],
  ])("is refused when it lists one task and its place is a run of another, %s", (_description, cursor) => {
    expect(() => decodeRunListCursor(cursor, { listedStates: ["failed"], listedTasks: ["send-email"] })).toThrow(
      INVALID_CURSOR_MESSAGE,
    );
  });
});
