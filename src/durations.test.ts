import { describe, expect, expectTypeOf, test } from "vitest";

import { parseDuration, shownDurationOf } from "./durations.js";
import type { Duration } from "./durations.js";

describe("parseDuration", () => {
  test.each([
    ["30s", 30_000],
    ["5m", 300_000],
    ["2h", 7_200_000],
    ["3d", 259_200_000],
    ["0s", 0],
  ] as const)("reads %s as %i milliseconds", (duration, expectedMilliseconds) => {
    expect(parseDuration(duration)).toBe(expectedMilliseconds);
  });

  test.each(["5 s", "0.5h", "-5s", "5", "s", "5w", "5S", " 5s", "5s ", ""])(
    "throws on %j instead of reading it as zero",
    (duration) => {
      expect(() => parseDuration(duration as Duration)).toThrow(
        `${JSON.stringify(duration)} is not a duration: a whole number followed by s, m, h or d, such as "30s"`,
      );
    },
  );

  test.each([5, undefined, null, { seconds: 5 }])("throws on the non-string %j", (duration) => {
    expect(() => parseDuration(duration as unknown as Duration)).toThrow("is not a duration");
  });
});

describe("Duration", () => {
  test("is a whole number followed by s, m, h or d", () => {
    expectTypeOf<"30s" | "5m" | "2h" | "3d">().toExtend<Duration>();
  });

  test("rejects a space, a fraction and an unknown unit at compile time", () => {
    // @ts-expect-error a space doesn't compile
    const spaced: Duration = "5 s";
    // @ts-expect-error a fraction doesn't compile
    const fractional: Duration = "0.5h";
    // @ts-expect-error an unknown unit doesn't compile
    const inWeeks: Duration = "2w";

    expect([spaced, fractional, inWeeks]).toHaveLength(3);
  });
});

describe("shownDurationOf", () => {
  test.each([
    [1_800_000, "30m"],
    [10_000, "10s"],
    [90_000, "90s"],
    [7_200_000, "2h"],
    [172_800_000, "2d"],
    [2500, "2500ms"],
  ] as const)("shows %i milliseconds as %s", (milliseconds, expectedText) => {
    expect(shownDurationOf(milliseconds)).toBe(expectedText);
  });
});
