import type { TestProject } from "vitest/node";

import { provideRedisUrl } from "./valkey-container.js";

export default async function setup(project: TestProject) {
  return provideRedisUrl(project);
}
