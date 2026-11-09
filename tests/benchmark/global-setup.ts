import type { TestProject } from "vitest/node";

import { startServerContainer } from "../own-server.js";
import type {} from "../valkey-container.js";

export default async function setup(project: TestProject) {
  const { url, container } = await startServerContainer();
  project.provide("redisUrl", url);
  return async () => {
    await container.stop();
  };
}
