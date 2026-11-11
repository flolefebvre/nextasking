import { describe, expect, test } from "vitest";

import { indexPagesOf } from "./index-page-script.js";

const READABLE_INDEX_PAGE = [2, 1, 1, "send-email:run-1", "1700000000000"];

describe("the reply of the index page script", () => {
  test("reads one page per index, with its counts and its places", () => {
    expect(indexPagesOf([READABLE_INDEX_PAGE, [0, 0, 0]], 2)).toEqual([
      {
        entryCount: 2,
        entryCountBeforeBoundary: 1,
        firstEntryRank: 1,
        places: [{ storedRunId: "send-email:run-1", score: 1_700_000_000_000 }],
      },
      { entryCount: 0, entryCountBeforeBoundary: 0, firstEntryRank: 0, places: [] },
    ]);
  });

  test.each([
    ["not an array", "not an array", 1],
    ["one page short", [READABLE_INDEX_PAGE], 2],
    ["a page that is not an array", ["page"], 1],
    ["a page without its counts", [[2, 1]], 1],
    ["a page with half an entry", [[2, 1, 1, "send-email:run-1"]], 1],
    ["a count that is a string", [["2", 1, 1]], 1],
    ["a negative count", [[2, -1, 1]], 1],
    ["a score that is not a number", [[1, 0, 0, "send-email:run-1", "soon"]], 1],
    ["a member that is not a string", [[1, 0, 0, 7, "1"]], 1],
  ])("is refused when it is %s", (_description, reply, indexCount) => {
    expect(() => indexPagesOf(reply, indexCount)).toThrow("The index page script sent a reply the reader cannot read");
  });
});
