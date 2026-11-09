import { randomUUID } from "node:crypto";
import { inject, test } from "vitest";

import { checkInvariantsOfThisTest } from "../invariant-violations.js";
import type {} from "../valkey-container.js";
import type {} from "./global-setup.js";

export const e2eTest = test
  .extend("redisUrl", { scope: "worker" }, () => inject("redisUrl"))
  .extend("fixtureAppDir", { scope: "worker" }, () => inject("fixtureAppDir"))
  .extend("standaloneAppDir", { scope: "worker" }, () => inject("standaloneAppDir"))
  .extend("keyPrefix", () => `nextasking-e2e:${randomUUID()}:`)
  .extend("expectInvariantViolation", { auto: true }, ({ redisUrl, keyPrefix, onTestFinished }) =>
    checkInvariantsOfThisTest({ redisUrl, keyPrefix, onTestFinished }),
  )
  .extend("fixtureAppEnvWithWorkerOff", ({ redisUrl, keyPrefix }): Record<string, string> => ({
    REDIS_URL: redisUrl,
    NEXTASKING_PREFIX: keyPrefix,
    NEXTASKING_WORKER: "",
  }))
  .extend("fixtureAppEnv", ({ fixtureAppEnvWithWorkerOff }): Record<string, string> => ({
    ...fixtureAppEnvWithWorkerOff,
    NEXTASKING_WORKER: "on",
  }))
  .extend("fixtureAppEnvWithScheduleOnAndGracefulStop", ({ fixtureAppEnv }): Record<string, string> => ({
    ...fixtureAppEnv,
    NEXTASKING_E2E_SCHEDULE: "on",
    NEXT_MANUAL_SIG_HANDLE: "1",
  }));
