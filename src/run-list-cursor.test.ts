import { describe, expect, test } from "vitest";

import { decodeRunListCursor, encodeRunListCursor } from "./run-list-cursor.js";

describe("the listRuns cursor", () => {
  test("decodes back to the position it encodes", () => {
    const position = { state: "failed", setIndex: 0, offset: 150 } as const;

    expect(decodeRunListCursor(encodeRunListCursor(position), "failed")).toEqual(position);
  });

  test("is an opaque string with no visible position", () => {
    const cursor = encodeRunListCursor({ state: "pending", setIndex: 2, offset: 42 });

    expect(cursor).toMatch(/^[A-Za-z0-9_-]+$/);
    expect(cursor).not.toContain("42");
  });

  test("is refused for another state than the one it pages through", () => {
    const cursor = encodeRunListCursor({ state: "failed", setIndex: 0, offset: 50 });

    expect(() => decodeRunListCursor(cursor, "completed")).toThrow(
      'Invalid listRuns cursor: it pages through state "failed", not "completed"',
    );
  });

  test.each([
    ["a string that is not a cursor", "not-a-cursor"],
    ["a number", 42],
    ["an empty string", ""],
    ["a negative offset", encodeRunListCursor({ state: "failed", setIndex: 0, offset: -1 })],
    ["a set past the state's sets", encodeRunListCursor({ state: "failed", setIndex: 1, offset: 0 })],
  ])("is refused when it is %s", (_description, cursor) => {
    expect(() => decodeRunListCursor(cursor, "failed")).toThrow(
      "Invalid listRuns cursor: pass back a nextCursor that listRuns returned",
    );
  });
});
