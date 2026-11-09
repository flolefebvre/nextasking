import { randomUUID } from "node:crypto";
import { inject, test } from "vitest";

import { checkInvariantsOfThisTest } from "./invariant-violations.js";
import type {} from "./valkey-container.js";

export const integrationTest = test
  .extend("redisUrl", { scope: "worker" }, () => inject("redisUrl"))
  .extend("keyPrefix", () => `nextasking-test:${randomUUID()}:`)
  .extend("expectInvariantViolation", { auto: true }, ({ redisUrl, keyPrefix, onTestFinished }) =>
    checkInvariantsOfThisTest({ redisUrl, keyPrefix, onTestFinished }),
  );
