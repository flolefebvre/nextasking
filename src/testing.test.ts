import { expect, test } from "vitest";

import * as root from "./index.js";
import * as testing from "./testing.js";

test("the testing export holds fakeNextasking and nothing else at runtime, and the root export holds none of it", () => {
  const exportedNames = Object.keys(testing).sort();

  expect(exportedNames).toEqual(["fakeNextasking"]);
  expect(root).not.toHaveProperty("fakeNextasking");
});
