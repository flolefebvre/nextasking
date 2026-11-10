import { randomUUID } from "node:crypto";
import { inject, test } from "vitest";

import { checkInvariantsOfThisTest } from "./invariant-violations.js";
import type {} from "./valkey-container.js";

export const TEST_TIMEOUT_PAST_ITS_WAITS_IN_MILLISECONDS = 5_000;

export const SEQUENTIAL_ROUND_TRIP_BOUND_IN_MILLISECONDS = 50;

export const integrationTest = test
  .extend("redisUrl", { scope: "worker" }, () => inject("redisUrl"))
  .extend("keyPrefix", () => `nextasking-test:${randomUUID()}:`)
  .extend("expectInvariantViolation", { auto: true }, ({ redisUrl, keyPrefix, onTestFinished }) =>
    checkInvariantsOfThisTest({ redisUrl, keyPrefix, onTestFinished }),
  );
