import type {} from "vitest";
import type { TestProject } from "vitest/node";

import { provideRedisUrl } from "../valkey-container.js";
import { prepareFixtureApp } from "./fixture-app.js";
import { SETUP_RECIPE_SERVER_ARGUMENTS } from "./setup-recipe-server-arguments.js";

declare module "vitest" {
  interface ProvidedContext {
    fixtureAppDir: string;
    standaloneAppDir: string;
  }
}

export default async function setup(project: TestProject) {
  const { fixtureAppDir, standaloneAppDir, remove } = await prepareFixtureApp();
  project.provide("fixtureAppDir", fixtureAppDir);
  project.provide("standaloneAppDir", standaloneAppDir);
  try {
    const stopRedis = await provideRedisUrl(project, SETUP_RECIPE_SERVER_ARGUMENTS);
    return async () => {
      await stopRedis?.();
      await remove();
    };
  } catch (error) {
    await remove();
    throw error;
  }
}
