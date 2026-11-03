import { expect, test } from "vitest";

import * as root from "./index.js";
import * as testing from "./testing.js";

test("the testing export holds the test helpers and nothing else at runtime, and the root export holds none of them", () => {
  const exportedNames = Object.keys(testing).sort();

  expect(exportedNames).toEqual(["createNextaskingControl", "executeAttempt", "fakeNextasking"]);
  for (const exportedName of exportedNames) {
    expect(root).not.toHaveProperty(exportedName);
  }
});
