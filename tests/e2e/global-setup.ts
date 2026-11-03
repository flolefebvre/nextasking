import type {} from "vitest";
import type { TestProject } from "vitest/node";

import { provideRedisUrl } from "../valkey-container.js";
import { prepareFixtureApp } from "./fixture-app.js";

declare module "vitest" {
  interface ProvidedContext {
    fixtureAppDir: string;
  }
}

export default async function setup(project: TestProject) {
  const { fixtureAppDir, remove } = await prepareFixtureApp();
  project.provide("fixtureAppDir", fixtureAppDir);
  try {
    const stopRedis = await provideRedisUrl(project);
    return async () => {
      await stopRedis?.();
      await remove();
    };
  } catch (error) {
    await remove();
    throw error;
  }
}
