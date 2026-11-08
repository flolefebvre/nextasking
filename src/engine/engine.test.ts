import { expect, test } from "vitest";

import { CUT_ERROR_TEXT_MARKER, errorTextCutOf, MOST_ERROR_TEXT_CHARACTERS } from "./engine.js";

test("an error text of 10,000 characters is cut to its first 4,000 followed by the marker", () => {
  const longText = "abcdefghij".repeat(1000);

  const cutText = errorTextCutOf(longText);

  expect(MOST_ERROR_TEXT_CHARACTERS).toBe(4000);
  expect(cutText).toBe(`${longText.slice(0, 4000)}${CUT_ERROR_TEXT_MARKER}`);
});

test("the marker says the text was cut and at how many characters", () => {
  expect(CUT_ERROR_TEXT_MARKER).toBe("… [cut at 4000 characters]");
});

test("an error text of 4,000 characters or fewer is kept whole", () => {
  const textAtTheLimit = "a".repeat(4000);

  expect(errorTextCutOf(textAtTheLimit)).toBe(textAtTheLimit);
  expect(errorTextCutOf("")).toBe("");
});

test("an error text is cut on characters, never inside a character made of two code units", () => {
  const textOfEmoji = "😀".repeat(4001);

  const cutText = errorTextCutOf(textOfEmoji);

  expect(cutText).toBe(`${"😀".repeat(4000)}${CUT_ERROR_TEXT_MARKER}`);
});
