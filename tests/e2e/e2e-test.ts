import { randomUUID } from "node:crypto";
import { inject, test } from "vitest";

import type {} from "../valkey-container.js";
import type {} from "./global-setup.js";

export const e2eTest = test
  .extend("redisUrl", { scope: "worker" }, () => inject("redisUrl"))
  .extend("fixtureAppDir", { scope: "worker" }, () => inject("fixtureAppDir"))
  .extend("keyPrefix", () => `nextasking-e2e:${randomUUID()}:`)
  .extend("fixtureAppEnv", ({ redisUrl, keyPrefix }): Record<string, string> => ({
    REDIS_URL: redisUrl,
    NEXTASKING_PREFIX: keyPrefix,
    NEXTASKING_WORKER: "on",
  }));
