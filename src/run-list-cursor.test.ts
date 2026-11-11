import { describe, expect, test } from "vitest";

import { EVERY_TIME, decodeRunListCursor, encodeRunListCursor } from "./run-list-cursor.js";
import type { RunListCursorContent, RunListRequest, TimeRange } from "./run-list-cursor.js";
import { cursorOfFields } from "./run-list-cursor.js";
import type { RunState } from "./run-states.js";

const FAILED_RUN_PLACE = {
  orderGroup: "ended",
  score: 1_731_000_000_000,
  task: "send-email",
  runId: "run-42",
} as const;

function requestOf(
  listedStates: readonly RunState[] | null,
  listedTasks: readonly string[] | null,
  timeRange: TimeRange = EVERY_TIME,
): RunListRequest {
  return { listedStates, listedTasks, timeRange };
}

const FAILED_OF_EVERY_TASK = requestOf(["failed"], null);

const FORWARD_FROM_A_FAILED_RUN: RunListCursorContent = {
  ...FAILED_OF_EVERY_TASK,
  direction: "forward",
  place: FAILED_RUN_PLACE,
};

const TWO_HOURS_TO_THE_FAILED_RUN: TimeRange = { from: 1_730_992_800_000, until: 1_731_000_000_000 };

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

  test("decodes a cursor listing the same states and tasks in another order, as a release listing its tasks in another order hands", () => {
    const cursor = cursorOfFields([
      3,
      "forward",
      ["ended", 1_731_000_000_000, "send-email", "run-42"],
      ["failed", "completed"],
      ["send-email", "build-report"],
      null,
      null,
    ]);

    expect(decodeRunListCursor(cursor, requestOf(["completed", "failed"], ["build-report", "send-email"]))).toEqual({
      direction: "forward",
      place: FAILED_RUN_PLACE,
    });
  });

  test("decodes back the boundary of one task", () => {
    const cursor = encodeRunListCursor({ ...FORWARD_FROM_A_FAILED_RUN, listedTasks: ["send-email"] });

    expect(decodeRunListCursor(cursor, requestOf(["failed"], ["send-email"]))).toEqual({
      direction: "forward",
      place: FAILED_RUN_PLACE,
    });
  });

  test("decodes back the boundary of every state, from a place in any order group", () => {
    const place = { orderGroup: "queued", score: 48_213, task: "send-email", runId: "run-42" } as const;
    const cursor = encodeRunListCursor({ ...requestOf(null, null), direction: "backward", place });

    expect(decodeRunListCursor(cursor, requestOf(null, null))).toEqual({ direction: "backward", place });
  });

  test.each([
    ["both bounds", TWO_HOURS_TO_THE_FAILED_RUN],
    ["a start alone", { from: 1_731_000_000_000, until: undefined }],
    ["an end alone", { from: undefined, until: 1_731_000_000_000 }],
  ])("decodes back the boundary of a time range with %s, its place on a bound", (_description, timeRange) => {
    const cursor = encodeRunListCursor({ ...FORWARD_FROM_A_FAILED_RUN, timeRange });

    expect(decodeRunListCursor(cursor, requestOf(["failed"], null, timeRange))).toEqual({
      direction: "forward",
      place: FAILED_RUN_PLACE,
    });
  });

  test("is the format version 3, its direction and place in its order group, its listed states and tasks, then its time range", () => {
    const backwardCursor = encodeRunListCursor({ ...FORWARD_FROM_A_FAILED_RUN, direction: "backward" });
    const rangedCursor = encodeRunListCursor({ ...FORWARD_FROM_A_FAILED_RUN, timeRange: TWO_HOURS_TO_THE_FAILED_RUN });

    expect(backwardCursor).toBe(
      cursorOfFields([
        3,
        "backward",
        ["ended", 1_731_000_000_000, "send-email", "run-42"],
        ["failed"],
        null,
        null,
        null,
      ]),
    );
    expect(rangedCursor).toBe(
      cursorOfFields([
        3,
        "forward",
        ["ended", 1_731_000_000_000, "send-email", "run-42"],
        ["failed"],
        null,
        1_730_992_800_000,
        1_731_000_000_000,
      ]),
    );
  });

  test("reads a cursor of the format version 2 as a forward cursor from its place, in the order group of its state, at any time", () => {
    const cursor = cursorOfFields([2, "failed", "send-email", 1_731_000_000_000, "send-email", "run-42"]);

    expect(decodeRunListCursor(cursor, requestOf(["failed"], ["send-email"]))).toEqual({
      direction: "forward",
      place: FAILED_RUN_PLACE,
    });
    expect(() =>
      decodeRunListCursor(cursor, requestOf(["failed"], ["send-email"], TWO_HOURS_TO_THE_FAILED_RUN)),
    ).toThrow(
      "Invalid listRuns cursor: it pages through runs at any time, not runs from 2024-11-07T15:20:00.000Z until 2024-11-07T17:20:00.000Z",
    );
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
    expect(() => decodeRunListCursor(cursor, requestOf(["completed"], null))).toThrow(
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

      expect(() => decodeRunListCursor(cursor, requestOf(calledStates, null))).toThrow(
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

    expect(() => decodeRunListCursor(cursor, requestOf(["failed"], calledTasks))).toThrow(
      `Invalid listRuns cursor: ${reason}`,
    );
  });

  test.each([
    [
      TWO_HOURS_TO_THE_FAILED_RUN,
      { from: 1_730_996_400_000, until: 1_731_000_000_000 },
      "runs from 2024-11-07T15:20:00.000Z until 2024-11-07T17:20:00.000Z, not runs from 2024-11-07T16:20:00.000Z until 2024-11-07T17:20:00.000Z",
    ],
    [
      TWO_HOURS_TO_THE_FAILED_RUN,
      EVERY_TIME,
      "runs from 2024-11-07T15:20:00.000Z until 2024-11-07T17:20:00.000Z, not runs at any time",
    ],
    [
      { from: undefined, until: 1_731_000_000_000 },
      { from: 1_731_000_000_000, until: undefined },
      "runs until 2024-11-07T17:20:00.000Z, not runs from 2024-11-07T17:20:00.000Z",
    ],
  ] as const)(
    "is refused when it pages through another time range than the call's, naming both: %j against %j",
    (cursorRange, calledRange, reason) => {
      const cursor = encodeRunListCursor({ ...FORWARD_FROM_A_FAILED_RUN, timeRange: cursorRange });

      expect(() => decodeRunListCursor(cursor, requestOf(["failed"], null, calledRange))).toThrow(
        `Invalid listRuns cursor: it pages through ${reason}`,
      );
    },
  );

  test.each([
    ["a string that is not a cursor", "not-a-cursor"],
    ["a number", 42],
    ["an empty string", ""],
    ["a 0.1.x cursor", cursorOfFields([1, "failed", 0, 50])],
    ["of another format version", cursorOfFields([1, "failed", null, 1, "send-email", "run-42"])],
    [
      "of a format version to come",
      cursorOfFields([4, "forward", ["ended", 1, "send-email", "run-42"], ["failed"], null, null, null]),
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
      cursorOfFields([3, "sideways", ["ended", 1, "send-email", "run-42"], ["failed"], null, null, null]),
    ],
    ["of version 3 forward from no place", cursorOfFields([3, "forward", null, ["failed"], null, null, null])],
    [
      "of version 3 with a place that is not a list",
      cursorOfFields([3, "backward", "run-42", ["failed"], null, null, null]),
    ],
    [
      "of version 3 with a place missing its order group",
      cursorOfFields([3, "backward", [1, "send-email", "run-42"], ["failed"], null, null, null]),
    ],
    [
      "of version 3 with a place in an unknown order group",
      cursorOfFields([3, "backward", ["finished", 1, "send-email", "run-42"], ["failed"], null, null, null]),
    ],
    [
      "of version 3 with a place in an order group none of its states is in",
      cursorOfFields([3, "backward", ["queued", 1, "send-email", "run-42"], ["failed"], null, null, null]),
    ],
    [
      "of version 3 with a score that is a string",
      cursorOfFields([3, "backward", ["ended", "1", "send-email", "run-42"], ["failed"], null, null, null]),
    ],
    ["of version 3 with an unknown state", cursorOfFields([3, "backward", null, ["sleeping"], null, null, null])],
    ["of version 3 with a state that is not listed", cursorOfFields([3, "backward", null, "failed", null, null, null])],
    ["of version 3 with an empty list of states", cursorOfFields([3, "backward", null, [], null, null, null])],
    ["of version 3 with an empty list of tasks", cursorOfFields([3, "backward", null, ["failed"], [], null, null])],
    [
      "of version 3 with a task that is not a string",
      cursorOfFields([3, "backward", null, ["failed"], [7], null, null]),
    ],
    ["of version 3 missing its time range", cursorOfFields([3, "backward", null, ["failed"], null])],
    ["of version 3 with a start that is a string", cursorOfFields([3, "backward", null, ["failed"], null, "1", null])],
    [
      "of version 3 with an end that is not whole milliseconds",
      cursorOfFields([3, "backward", null, ["failed"], null, null, 1.5]),
    ],
    [
      "of version 3 with a start past the safe integers",
      cursorOfFields([3, "backward", null, ["failed"], null, 2 ** 60, null]),
    ],
    ["of version 3 with a start after its end", cursorOfFields([3, "backward", null, ["failed"], null, 2, 1])],
    [
      "of version 3 with a place before its start",
      cursorOfFields([3, "backward", ["ended", 1, "send-email", "run-42"], ["failed"], null, 2, 3]),
    ],
    [
      "of version 3 with a place after its end",
      cursorOfFields([3, "forward", ["ended", 4, "send-email", "run-42"], ["failed"], null, 2, 3]),
    ],
  ])("is refused when it is %s", (_description, cursor) => {
    expect(() => decodeRunListCursor(cursor, FAILED_OF_EVERY_TASK)).toThrow(INVALID_CURSOR_MESSAGE);
  });

  test.each([
    ["of the format version 2", cursorOfFields([2, "failed", "send-email", 1, "build-report", "run-42"])],
    [
      "of the format version 3",
      cursorOfFields([3, "forward", ["ended", 1, "build-report", "run-42"], ["failed"], ["send-email"], null, null]),
    ],
  ])("is refused when it lists one task and its place is a run of another, %s", (_description, cursor) => {
    expect(() => decodeRunListCursor(cursor, requestOf(["failed"], ["send-email"]))).toThrow(INVALID_CURSOR_MESSAGE);
  });
});
