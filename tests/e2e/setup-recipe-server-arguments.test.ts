import { readFile } from "node:fs/promises";

import { expect, test } from "vitest";

import { SETUP_RECIPE_SERVER_ARGUMENTS } from "./setup-recipe-server-arguments.js";

const SETUP_PAGE_URL = new URL("../../docs/setup.md", import.meta.url);

function serverArgumentsOfTheRecipeIn(setupPage: string): string[] {
  const dockerRunLines = setupPage.split("\n").filter((line) => line.startsWith("docker run "));
  expect(dockerRunLines).toHaveLength(1);
  const words = dockerRunLines[0]!.split(" ").filter((word) => word !== "");
  const imageWordPosition = words.findIndex((word) => /^(valkey\/valkey|redis):/.test(word));
  expect(imageWordPosition).toBeGreaterThan(0);
  return words.slice(imageWordPosition + 1);
}

test("the e2e server takes exactly the arguments the setup page's docker run line passes after the image", async () => {
  const setupPage = await readFile(SETUP_PAGE_URL, "utf8");

  const recipeServerArguments = serverArgumentsOfTheRecipeIn(setupPage);

  expect(recipeServerArguments).toEqual(SETUP_RECIPE_SERVER_ARGUMENTS);
});
